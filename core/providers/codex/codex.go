// Package codex exposes Codex as a first-class OpenAI-compatible provider.
package codex

import (
	"bytes"
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
	codexResponsesURL  = "https://chatgpt.com/backend-api/codex/responses"
	codexCLIVersion    = "0.130.0"
)

type CodexProvider struct {
	*openai.OpenAIProvider
	authMu sync.Mutex
	auths  map[string]codexAuthTokens
}

type codexAuthTokens struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	AccountID    string `json:"account_id,omitempty"`
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
	keyStatuses := make([]schemas.KeyStatus, 0, len(keys))
	for _, key := range keys {
		keyStatuses = append(keyStatuses, schemas.KeyStatus{
			KeyID:    key.ID,
			Provider: schemas.Codex,
			Status:   schemas.KeyStatusSuccess,
		})
	}
	return &schemas.BifrostListModelsResponse{
		Data:        make([]schemas.Model, 0),
		KeyStatuses: keyStatuses,
	}, nil
}

func (provider *CodexProvider) ChatCompletion(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostChatRequest) (*schemas.BifrostChatResponse, *schemas.BifrostError) {
	resolvedKey, tokens, ok := provider.resolveCodexAuth(key)
	if !ok {
		return provider.OpenAIProvider.ChatCompletion(ctx, resolvedKey, request)
	}
	return provider.codexChatCompletion(ctx, resolvedKey, tokens, request)
}

func (provider *CodexProvider) ChatCompletionStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostChatRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	resolvedKey, tokens, ok := provider.resolveCodexAuth(key)
	if !ok {
		return provider.OpenAIProvider.ChatCompletionStream(ctx, postHookRunner, postHookSpanFinalizer, resolvedKey, request)
	}

	ch := make(chan *schemas.BifrostStreamChunk, 1)
	go func() {
		defer close(ch)
		resp, bifrostErr := provider.codexChatCompletion(ctx, resolvedKey, tokens, request)
		if bifrostErr != nil {
			ch <- &schemas.BifrostStreamChunk{BifrostError: bifrostErr}
			return
		}
		if len(resp.Choices) > 0 && resp.Choices[0].ChatNonStreamResponseChoice != nil {
			content := ""
			msg := resp.Choices[0].ChatNonStreamResponseChoice.Message
			if msg != nil && msg.Content != nil && msg.Content.ContentStr != nil {
				content = *msg.Content.ContentStr
			}
			role := string(schemas.ChatMessageRoleAssistant)
			resp.Choices[0].ChatNonStreamResponseChoice = nil
			resp.Choices[0].ChatStreamResponseChoice = &schemas.ChatStreamResponseChoice{
				Delta: &schemas.ChatStreamResponseChoiceDelta{Role: &role, Content: &content},
			}
			resp.Object = "chat.completion.chunk"
			resp.ExtraFields.RequestType = schemas.ChatCompletionStreamRequest
		}
		ch <- &schemas.BifrostStreamChunk{BifrostChatResponse: resp}
	}()
	return ch, nil
}

func (provider *CodexProvider) Responses(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostResponsesRequest) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
	resolvedKey, _, _ := provider.resolveCodexAuth(key)
	return provider.OpenAIProvider.Responses(ctx, resolvedKey, request)
}

func (provider *CodexProvider) ResponsesStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostResponsesRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	resolvedKey, _, _ := provider.resolveCodexAuth(key)
	return provider.OpenAIProvider.ResponsesStream(ctx, postHookRunner, postHookSpanFinalizer, resolvedKey, request)
}

func (provider *CodexProvider) resolveKey(key schemas.Key) schemas.Key {
	resolvedKey, _, _ := provider.resolveCodexAuth(key)
	return resolvedKey
}

func (provider *CodexProvider) resolveCodexAuth(key schemas.Key) (schemas.Key, codexAuthTokens, bool) {
	raw := strings.TrimSpace(key.Value.GetValue())
	if raw == "" {
		return key, codexAuthTokens{}, false
	}

	tokens, ok := parseCodexAuthTokens(raw)
	if !ok {
		return key, codexAuthTokens{}, false
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
		return key, cached, true
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
	if tokens.AccountID == "" {
		tokens.AccountID = codexAccountIDFromToken(tokens.AccessToken)
	}

	provider.authMu.Lock()
	provider.auths[cacheKey] = tokens
	provider.authMu.Unlock()

	key.Value = *schemas.NewEnvVar(tokens.AccessToken)
	return key, tokens, true
}

type codexResponsesRequest struct {
	Model          string                   `json:"model"`
	Instructions   string                   `json:"instructions"`
	Input          []codexResponsesMessage  `json:"input"`
	Stream         bool                     `json:"stream"`
	Store          bool                     `json:"store"`
	PromptCacheKey string                   `json:"prompt_cache_key"`
	Reasoning      *codexResponsesReasoning `json:"reasoning,omitempty"`
	ClientMetadata map[string]string        `json:"client_metadata,omitempty"`
}

type codexResponsesReasoning struct {
	Effort *string `json:"effort,omitempty"`
}

type codexResponsesMessage struct {
	Role    string                  `json:"role"`
	Content []codexResponsesContent `json:"content"`
}

type codexResponsesContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func (provider *CodexProvider) codexChatCompletion(ctx *schemas.BifrostContext, key schemas.Key, tokens codexAuthTokens, request *schemas.BifrostChatRequest) (*schemas.BifrostChatResponse, *schemas.BifrostError) {
	_ = ctx
	if tokens.AccountID == "" {
		return nil, providerUtils.NewBifrostOperationError("codex account_id is missing from access token", nil)
	}

	body, threadID, err := buildCodexResponsesRequest(request)
	if err != nil {
		return nil, providerUtils.NewBifrostOperationError(schemas.ErrRequestBodyConversion, err)
	}
	jsonBody, err := sonic.Marshal(body)
	if err != nil {
		return nil, providerUtils.NewBifrostOperationError(schemas.ErrProviderRequestMarshal, err)
	}

	client := &fasthttp.Client{ReadTimeout: 120 * time.Second, WriteTimeout: 20 * time.Second}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(fasthttp.MethodPost)
	req.SetRequestURI(codexResponsesURL)
	req.Header.SetContentType("application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer "+key.Value.GetValue())
	req.Header.Set("ChatGPT-Account-ID", tokens.AccountID)
	req.Header.Set("session_id", threadID)
	req.Header.Set("session-id", threadID)
	req.Header.Set("thread_id", threadID)
	req.Header.Set("thread-id", threadID)
	req.Header.Set("x-client-request-id", threadID)
	req.Header.Set("x-codex-installation-id", threadID)
	req.Header.Set("x-codex-window-id", threadID+":0")
	req.Header.Set("version", codexCLIVersion)
	req.SetBody(jsonBody)

	start := time.Now()
	if err := client.Do(req, resp); err != nil {
		return nil, providerUtils.NewBifrostOperationError(schemas.ErrProviderDoRequest, err)
	}
	latency := time.Since(start).Milliseconds()

	if resp.StatusCode() < 200 || resp.StatusCode() >= 300 {
		return nil, codexHTTPError(resp)
	}

	text, responseID, created, model, usage := parseCodexResponsesSSE(resp.Body(), request.Model)
	finishReason := string(schemas.BifrostFinishReasonStop)
	content := schemas.ChatMessageContent{ContentStr: &text}
	return &schemas.BifrostChatResponse{
		ID:      responseID,
		Object:  "chat.completion",
		Created: created,
		Model:   model,
		Choices: []schemas.BifrostResponseChoice{
			{
				Index:        0,
				FinishReason: &finishReason,
				ChatNonStreamResponseChoice: &schemas.ChatNonStreamResponseChoice{
					Message: &schemas.ChatMessage{
						Role:    schemas.ChatMessageRoleAssistant,
						Content: &content,
					},
				},
			},
		},
		Usage: usage,
		ExtraFields: schemas.BifrostResponseExtraFields{
			Provider:               schemas.Codex,
			RequestType:            schemas.ChatCompletionRequest,
			Latency:                latency,
			ResolvedModelUsed:      body.Model,
			OriginalModelRequested: request.Model,
		},
	}, nil
}

func buildCodexResponsesRequest(request *schemas.BifrostChatRequest) (*codexResponsesRequest, string, error) {
	if request == nil {
		return nil, "", fmt.Errorf("request is nil")
	}

	instructions := "You are ChatGPT Codex."
	input := make([]codexResponsesMessage, 0, len(request.Input))
	for _, msg := range request.Input {
		text := chatMessageText(msg)
		if text == "" {
			continue
		}
		switch msg.Role {
		case schemas.ChatMessageRoleSystem:
			if instructions == "You are ChatGPT Codex." {
				instructions = text
			} else {
				instructions += "\n" + text
			}
		case schemas.ChatMessageRoleAssistant:
			input = append(input, codexResponsesMessage{Role: "assistant", Content: []codexResponsesContent{{Type: "output_text", Text: text}}})
		default:
			input = append(input, codexResponsesMessage{Role: "user", Content: []codexResponsesContent{{Type: "input_text", Text: text}}})
		}
	}
	if len(input) == 0 {
		return nil, "", fmt.Errorf("codex request requires at least one text message")
	}

	threadID := fmt.Sprintf("bifrost-codex-%d", time.Now().UnixNano())
	model := normalizeCodexChatGPTModel(request.Model)
	body := &codexResponsesRequest{
		Model:          model,
		Instructions:   instructions,
		Input:          input,
		Stream:         true,
		Store:          false,
		PromptCacheKey: threadID,
		ClientMetadata: map[string]string{"x-codex-installation-id": threadID},
	}
	if request.Params != nil && request.Params.PromptCacheKey != nil && *request.Params.PromptCacheKey != "" {
		body.PromptCacheKey = *request.Params.PromptCacheKey
		threadID = *request.Params.PromptCacheKey
	}
	if request.Params != nil && request.Params.Reasoning != nil && request.Params.Reasoning.Effort != nil {
		body.Reasoning = &codexResponsesReasoning{Effort: request.Params.Reasoning.Effort}
	}
	return body, threadID, nil
}

func normalizeCodexChatGPTModel(model string) string {
	switch model {
	case "", "gpt-5-codex", "gpt-5", "gpt-5.4-codex":
		return "gpt-5.5"
	default:
		return model
	}
}

func chatMessageText(msg schemas.ChatMessage) string {
	if msg.Content == nil {
		return ""
	}
	if msg.Content.ContentStr != nil {
		return *msg.Content.ContentStr
	}
	parts := make([]string, 0, len(msg.Content.ContentBlocks))
	for _, block := range msg.Content.ContentBlocks {
		if block.Text != nil {
			parts = append(parts, *block.Text)
		}
	}
	return strings.Join(parts, "\n")
}

func parseCodexResponsesSSE(body []byte, fallbackModel string) (string, string, int, string, *schemas.BifrostLLMUsage) {
	model := fallbackModel
	created := int(time.Now().Unix())
	var responseID string
	var text strings.Builder
	var usage *schemas.BifrostLLMUsage

	for _, event := range bytes.Split(body, []byte("\n\n")) {
		for _, line := range bytes.Split(event, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if !bytes.HasPrefix(line, []byte("data:")) {
				continue
			}
			data := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
			if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
				continue
			}
			var payload struct {
				Type     string `json:"type"`
				Delta    string `json:"delta"`
				Text     string `json:"text"`
				Response *struct {
					ID        string                   `json:"id"`
					CreatedAt int                      `json:"created_at"`
					Model     string                   `json:"model"`
					Usage     *schemas.BifrostLLMUsage `json:"usage"`
				} `json:"response"`
			}
			if sonic.Unmarshal(data, &payload) != nil {
				continue
			}
			if payload.Response != nil {
				if payload.Response.ID != "" {
					responseID = payload.Response.ID
				}
				if payload.Response.CreatedAt != 0 {
					created = payload.Response.CreatedAt
				}
				if payload.Response.Model != "" {
					model = payload.Response.Model
				}
				if payload.Response.Usage != nil {
					usage = payload.Response.Usage
				}
			}
			switch payload.Type {
			case "response.output_text.delta":
				text.WriteString(payload.Delta)
			case "response.output_text.done":
				if text.Len() == 0 {
					text.WriteString(payload.Text)
				}
			}
		}
	}
	if responseID == "" {
		responseID = fmt.Sprintf("chatcmpl-codex-%d", time.Now().UnixNano())
	}
	if usage == nil {
		usage = &schemas.BifrostLLMUsage{}
	}
	return text.String(), responseID, created, model, usage
}

func codexHTTPError(resp *fasthttp.Response) *schemas.BifrostError {
	statusCode := resp.StatusCode()
	message := strings.TrimSpace(string(resp.Body()))
	var payload struct {
		Detail string `json:"detail"`
		Error  any    `json:"error"`
	}
	if sonic.Unmarshal(resp.Body(), &payload) == nil {
		if payload.Detail != "" {
			message = payload.Detail
		} else if payload.Error != nil {
			if b, err := sonic.Marshal(payload.Error); err == nil {
				message = string(b)
			}
		}
	}
	return &schemas.BifrostError{
		IsBifrostError: false,
		StatusCode:     &statusCode,
		Error:          &schemas.ErrorField{Message: message},
		ExtraFields: schemas.BifrostErrorExtraFields{
			Provider: schemas.Codex,
		},
	}
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

func codexAccountIDFromToken(token string) string {
	claims, ok := jwtClaims(token)
	if !ok {
		return ""
	}
	authClaims, ok := claims["https://api.openai.com/auth"].(map[string]any)
	if !ok {
		return ""
	}
	accountID, _ := authClaims["chatgpt_account_id"].(string)
	return accountID
}

func jwtExpiry(token string) (time.Time, bool) {
	claims, ok := jwtClaims(token)
	if !ok {
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

func jwtClaims(token string) (map[string]any, bool) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, false
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, false
	}
	return claims, true
}
