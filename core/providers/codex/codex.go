// Package codex exposes Codex as a first-class OpenAI-compatible provider.
package codex

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/maximhq/bifrost/core/providers/openai"
	providerUtils "github.com/maximhq/bifrost/core/providers/utils"
	schemas "github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

const (
	codexOAuthClientID = "app_EMoamEEZ73f0CkXaXp7hrann"
	codexTokenURL      = "https://auth.openai.com/oauth/token"
)

type CodexProvider struct {
	*openai.OpenAIProvider
	authMu sync.Mutex
	auths  map[string]codexAuthTokens
}

type codexAuthTokens struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in,omitempty"`
}

// NewCodexProvider creates a Codex provider backed by the OpenAI-compatible
// provider implementation. Codex OAuth keys can be stored as raw access tokens
// or as {"access_token":"...","refresh_token":"..."}; the latter is refreshed
// before OpenAI-compatible requests are delegated.
func NewCodexProvider(config *schemas.ProviderConfig, logger schemas.Logger) *CodexProvider {
	if config.CustomProviderConfig == nil {
		config.CustomProviderConfig = &schemas.CustomProviderConfig{}
	}
	config.CustomProviderConfig.CustomProviderKey = string(schemas.Codex)
	config.CustomProviderConfig.BaseProviderType = schemas.OpenAI
	return &CodexProvider{
		OpenAIProvider: openai.NewOpenAIProvider(config, logger),
		auths:          make(map[string]codexAuthTokens),
	}
}

func (provider *CodexProvider) ListModels(ctx *schemas.BifrostContext, keys []schemas.Key, request *schemas.BifrostListModelsRequest) (*schemas.BifrostListModelsResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ListModelsRequest, schemas.Codex)
}

func (provider *CodexProvider) ChatCompletion(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostChatRequest) (*schemas.BifrostChatResponse, *schemas.BifrostError) {
	return provider.OpenAIProvider.ChatCompletion(ctx, provider.resolveKey(key), request)
}

func (provider *CodexProvider) ChatCompletionStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostChatRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return provider.OpenAIProvider.ChatCompletionStream(ctx, postHookRunner, postHookSpanFinalizer, provider.resolveKey(key), request)
}

func (provider *CodexProvider) Responses(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostResponsesRequest) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
	return provider.OpenAIProvider.Responses(ctx, provider.resolveKey(key), request)
}

func (provider *CodexProvider) ResponsesStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostResponsesRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return provider.OpenAIProvider.ResponsesStream(ctx, postHookRunner, postHookSpanFinalizer, provider.resolveKey(key), request)
}

func (provider *CodexProvider) resolveKey(key schemas.Key) schemas.Key {
	raw := strings.TrimSpace(key.Value.GetValue())
	if raw == "" {
		return key
	}

	tokens, ok := parseCodexAuthTokens(raw)
	if !ok {
		return key
	}

	cacheKey := key.ID
	if cacheKey == "" {
		cacheKey = key.Name
	}
	if cacheKey == "" {
		cacheKey = raw
	}

	provider.authMu.Lock()
	if cached, ok := provider.auths[cacheKey]; ok && !codexAccessTokenExpiring(cached.AccessToken) {
		provider.authMu.Unlock()
		key.Value = *schemas.NewEnvVar(cached.AccessToken)
		return key
	}
	provider.authMu.Unlock()

	if tokens.RefreshToken != "" && codexAccessTokenExpiring(tokens.AccessToken) {
		if refreshed, err := refreshCodexToken(tokens.RefreshToken); err == nil && refreshed.AccessToken != "" {
			if refreshed.RefreshToken == "" {
				refreshed.RefreshToken = tokens.RefreshToken
			}
			tokens = refreshed
		}
	}

	provider.authMu.Lock()
	provider.auths[cacheKey] = tokens
	provider.authMu.Unlock()

	key.Value = *schemas.NewEnvVar(tokens.AccessToken)
	return key
}

func parseCodexAuthTokens(raw string) (codexAuthTokens, bool) {
	var direct codexAuthTokens
	if sonic.Unmarshal([]byte(raw), &direct) == nil && direct.AccessToken != "" {
		return direct, true
	}

	var wrapped struct {
		Tokens codexAuthTokens `json:"tokens"`
	}
	if sonic.Unmarshal([]byte(raw), &wrapped) == nil && wrapped.Tokens.AccessToken != "" {
		return wrapped.Tokens, true
	}
	return codexAuthTokens{}, false
}

func refreshCodexToken(refreshToken string) (codexAuthTokens, error) {
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("client_id", codexOAuthClientID)
	form.Set("refresh_token", refreshToken)

	client := &fasthttp.Client{ReadTimeout: 20 * time.Second, WriteTimeout: 20 * time.Second}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(fasthttp.MethodPost)
	req.SetRequestURI(codexTokenURL)
	req.Header.SetContentType("application/x-www-form-urlencoded")
	req.SetBodyString(form.Encode())

	if err := client.Do(req, resp); err != nil {
		return codexAuthTokens{}, err
	}
	if resp.StatusCode() < 200 || resp.StatusCode() >= 300 {
		return codexAuthTokens{}, fmt.Errorf("codex refresh failed with HTTP %d", resp.StatusCode())
	}

	var tokens codexAuthTokens
	if err := sonic.Unmarshal(resp.Body(), &tokens); err != nil {
		return codexAuthTokens{}, err
	}
	if tokens.AccessToken == "" {
		return codexAuthTokens{}, fmt.Errorf("codex refresh response did not include access_token")
	}
	return tokens, nil
}

func codexAccessTokenExpiring(token string) bool {
	exp, ok := jwtExpiry(token)
	if !ok {
		return false
	}
	return time.Until(exp) < time.Minute
}

func jwtExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, false
	}
	switch exp := claims["exp"].(type) {
	case float64:
		return time.Unix(int64(exp), 0), true
	case int64:
		return time.Unix(exp, 0), true
	default:
		return time.Time{}, false
	}
}
