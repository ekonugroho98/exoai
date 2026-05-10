// Package moclaw implements the MoClaw (moclaw.ai) provider.
//
// MoClaw exposes Claude (Opus 4.6, Sonnet 4.6) and DeepSeek models through a
// custom WebSocket protocol at wss://realtime.moclaw.ai/ws. Authentication is
// handled by Auth0 with refresh-token rotation.
//
// This adapter is for personal/learning experimentation. MoClaw's free trial
// gives 1000 credits/month — enough to integrate from any OpenAI-compatible
// client (Cline, Continue, OpenAI SDK, etc.) for moderate use.
//
// Configuration:
//   - API Key (in Bifrost): either the Auth0 *refresh_token* or the raw
//     *access_token* extracted from a logged-in browser session at moclaw.ai.
//
//     Refresh token (preferred, lasts until unused for 30 days):
//       DevTools → Application → Local Storage → @@auth0spajs@@::...::offline_access → body.refresh_token
//
//     Access token (fallback, 24 h TTL):
//       DevTools → Application → Local Storage → @@auth0spajs@@::...::offline_access → body.access_token
//       (starts with "eyJ"; the adapter detects this and skips the Auth0 exchange)
//
// Operational notes:
//   - The refresh_token rotates every refresh; this adapter persists the
//     latest value to disk at $BIFROST_DATA_DIR/moclaw_refresh.txt (or
//     ./moclaw_refresh.txt as fallback) so it survives restarts.
//   - access_token has 24h TTL and is cached in memory.
//   - If the refresh_token is rejected by Auth0 (rotated by another session,
//     revoked, or unused for >30 days), the adapter returns an auth error;
//     the user must extract a fresh refresh_token from the browser.
package moclaw

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	ws "github.com/fasthttp/websocket"
	"github.com/google/uuid"
	"github.com/valyala/fasthttp"

	providerUtils "github.com/maximhq/bifrost/core/providers/utils"
	schemas "github.com/maximhq/bifrost/core/schemas"
)

// ----------------------------------------------------------------------------
// Constants
// ----------------------------------------------------------------------------

const (
	defaultAuth0Domain = "https://auth.moclaw.ai"
	defaultAuth0ClientID = "R7QyN3rYIv2DSEqkgQJjfSvvb6XFxMOu"
	defaultWSURL       = "wss://realtime.moclaw.ai/ws"
	defaultAPIURL      = "https://api.moclaw.ai"

	// Refresh access_token if expiry is within this window.
	refreshSafetyMargin = 60 * time.Second

	// Default file name for the rotating refresh token cache. The full path
	// is $BIFROST_DATA_DIR/<name> if set, else ./<name>.
	refreshTokenCacheFile = "moclaw_refresh.txt"

	objectChatCompletionChunk = "chat.completion.chunk"
	objectChatCompletion      = "chat.completion"
)

// ----------------------------------------------------------------------------
// Provider struct
// ----------------------------------------------------------------------------

// MoClawProvider implements the Provider interface for moclaw.ai.
type MoClawProvider struct {
	logger              schemas.Logger
	httpClient          *fasthttp.Client
	networkConfig       schemas.NetworkConfig
	sendBackRawRequest  bool
	sendBackRawResponse bool

	auth *authState
}

// NewMoClawProvider constructs a MoClawProvider.
//
// The provider configuration's API key (Key.Value) MUST be the Auth0
// refresh_token extracted from a logged-in moclaw.ai session.
func NewMoClawProvider(config *schemas.ProviderConfig, logger schemas.Logger) (*MoClawProvider, error) {
	config.CheckAndSetDefaults()

	requestTimeout := time.Second * time.Duration(config.NetworkConfig.DefaultRequestTimeoutInSeconds)
	httpClient := &fasthttp.Client{
		ReadTimeout:         requestTimeout,
		WriteTimeout:        requestTimeout,
		MaxConnsPerHost:     config.NetworkConfig.MaxConnsPerHost,
		MaxIdleConnDuration: 30 * time.Second,
		MaxConnWaitTimeout:  requestTimeout,
		MaxConnDuration:     time.Second * time.Duration(schemas.DefaultMaxConnDurationInSeconds),
		ConnPoolStrategy:    fasthttp.FIFO,
	}
	httpClient = providerUtils.ConfigureProxy(httpClient, config.ProxyConfig, logger)
	httpClient = providerUtils.ConfigureDialer(httpClient)
	httpClient = providerUtils.ConfigureTLS(httpClient, config.NetworkConfig, logger)

	if config.NetworkConfig.BaseURL == "" {
		config.NetworkConfig.BaseURL = defaultAPIURL
	}
	config.NetworkConfig.BaseURL = strings.TrimRight(config.NetworkConfig.BaseURL, "/")

	auth := &authState{
		clientID:   defaultAuth0ClientID,
		auth0URL:   defaultAuth0Domain + "/oauth/token",
		httpClient: httpClient,
		logger:     logger,
		cachePath:  resolveRefreshTokenCachePath(),
	}
	if v, ok := config.NetworkConfig.ExtraHeaders["X-MoClaw-Auth0-ClientId"]; ok && v != "" {
		auth.clientID = v
	}

	return &MoClawProvider{
		logger:              logger,
		httpClient:          httpClient,
		networkConfig:       config.NetworkConfig,
		sendBackRawRequest:  config.SendBackRawRequest,
		sendBackRawResponse: config.SendBackRawResponse,
		auth:                auth,
	}, nil
}

// resolveRefreshTokenCachePath returns the path where rotated refresh tokens
// are persisted. Honors $BIFROST_DATA_DIR if set; falls back to "./".
func resolveRefreshTokenCachePath() string {
	dir := os.Getenv("BIFROST_DATA_DIR")
	if dir == "" {
		if home, err := os.UserHomeDir(); err == nil {
			dir = filepath.Join(home, ".bifrost")
		}
	}
	if dir == "" {
		return refreshTokenCacheFile
	}
	_ = os.MkdirAll(dir, 0700)
	return filepath.Join(dir, refreshTokenCacheFile)
}

// GetProviderKey returns the provider identifier.
func (p *MoClawProvider) GetProviderKey() schemas.ModelProvider {
	return schemas.MoClaw
}

// ----------------------------------------------------------------------------
// Auth0 refresh-token state
// ----------------------------------------------------------------------------

// authState manages the (rotating) refresh_token + cached access_token.
//
// MoClaw's Auth0 setup rotates the refresh_token on every refresh, so we
// must always persist the latest value. We keep an in-memory cache and
// also write to a small file at startup/refresh so the rotated token
// survives Bifrost restarts.
type authState struct {
	mu sync.Mutex

	clientID   string
	auth0URL   string
	httpClient *fasthttp.Client
	logger     schemas.Logger

	refreshToken string    // rotated every refresh
	accessToken  string    // 24h TTL, in-memory only
	expiresAt    time.Time // when accessToken expires
	cachePath    string    // path to disk cache for refreshToken
}

// auth0TokenResp is Auth0's /oauth/token response shape.
type auth0TokenResp struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token,omitempty"`
	Scope        string `json:"scope,omitempty"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Error        string `json:"error,omitempty"`
	ErrorDesc    string `json:"error_description,omitempty"`
}

// loadInitialRefreshToken sets the seed refresh_token. Preference order:
//  1. Disk cache (most recent rotated value, survives restart).
//  2. Provided value (the API key from Bifrost config).
//
// The cache always wins over provided because Auth0 rotates the refresh_token
// on every use — the cache holds the latest valid token after the first
// successful exchange. The provided value acts as the seed for the very first
// call (before the cache exists) and as a fallback if the cache file is lost.
//
// Returns an error only if neither is available.
func (a *authState) loadInitialRefreshToken(provided string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.refreshToken != "" {
		return nil
	}
	if a.cachePath != "" {
		if b, err := os.ReadFile(a.cachePath); err == nil {
			t := strings.TrimSpace(string(b))
			if t != "" {
				a.refreshToken = t
				return nil
			}
		}
	}
	if provided == "" {
		return errors.New("moclaw: no refresh_token: provide as Bifrost Key value or via cache file")
	}
	a.refreshToken = provided
	return nil
}

// getAccessToken returns a valid access_token, refreshing if needed.
// Concurrent callers serialize through the mutex.
//
// If providedRefresh looks like a JWT (starts with "eyJ"), it is treated as a
// direct access_token — the Auth0 refresh flow is skipped entirely. This lets
// users paste their browser access_token as the API key when their refresh_token
// is unavailable or has been rotated away. The token is used until it expires,
// at which point the caller must update the API key with a fresh token.
func (a *authState) getAccessToken(ctx context.Context, providedRefresh string) (string, error) {
	// Fast path: caller supplied a raw JWT access_token.
	if isJWT(providedRefresh) {
		a.mu.Lock()
		defer a.mu.Unlock()
		// Cache it (and its expiry) so repeated calls don't re-parse.
		if a.accessToken == providedRefresh && time.Until(a.expiresAt) > refreshSafetyMargin {
			return a.accessToken, nil
		}
		exp, err := jwtExpiry(providedRefresh)
		if err != nil {
			return "", fmt.Errorf("moclaw: provided key looks like a JWT but exp parse failed: %w", err)
		}
		if time.Until(exp) <= refreshSafetyMargin {
			return "", errors.New("moclaw: provided access_token is expired; update the API key with a fresh token from the browser")
		}
		a.accessToken = providedRefresh
		a.expiresAt = exp
		return a.accessToken, nil
	}

	if err := a.loadInitialRefreshToken(providedRefresh); err != nil {
		return "", err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.accessToken != "" && time.Until(a.expiresAt) > refreshSafetyMargin {
		return a.accessToken, nil
	}

	return a.refreshLocked(ctx)
}

// isJWT reports whether s looks like a JWT (three base64url segments separated
// by dots, starting with the standard JWT header prefix "eyJ").
func isJWT(s string) bool {
	return strings.HasPrefix(s, "eyJ") && strings.Count(s, ".") == 2
}

// jwtExpiry extracts the "exp" claim from a JWT without verifying the signature.
func jwtExpiry(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, errors.New("not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("base64 decode: %w", err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := sonic.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, fmt.Errorf("json decode: %w", err)
	}
	if claims.Exp == 0 {
		return time.Time{}, errors.New("JWT has no exp claim")
	}
	return time.Unix(claims.Exp, 0), nil
}

// refreshLocked performs the Auth0 token-refresh exchange. Caller must hold a.mu.
func (a *authState) refreshLocked(ctx context.Context) (string, error) {
	body, _ := sonic.Marshal(map[string]string{
		"grant_type":    "refresh_token",
		"client_id":     a.clientID,
		"refresh_token": a.refreshToken,
	})

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(a.auth0URL)
	req.Header.SetMethod(http.MethodPost)
	req.Header.SetContentType("application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBody(body)

	if err := a.httpClient.Do(req, resp); err != nil {
		return "", fmt.Errorf("auth0 refresh transport error: %w", err)
	}

	var tok auth0TokenResp
	if err := sonic.Unmarshal(resp.Body(), &tok); err != nil {
		return "", fmt.Errorf("auth0 refresh decode error: %w (status=%d)", err, resp.StatusCode())
	}
	if resp.StatusCode() != fasthttp.StatusOK || tok.AccessToken == "" {
		return "", fmt.Errorf("auth0 refresh failed status=%d error=%q desc=%q", resp.StatusCode(), tok.Error, tok.ErrorDesc)
	}

	a.accessToken = tok.AccessToken
	a.refreshToken = tok.RefreshToken // rotated
	a.expiresAt = time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second)

	if a.cachePath != "" {
		// Best-effort persist; don't fail the request if disk write fails.
		if err := os.WriteFile(a.cachePath, []byte(tok.RefreshToken), 0600); err != nil {
			a.logger.Warn("moclaw: failed to persist rotated refresh_token: %v", err)
		}
	}

	return a.accessToken, nil
}

// invalidateAccessToken forces the next call to refresh. Used after a 401.
func (a *authState) invalidateAccessToken() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.accessToken = ""
	a.expiresAt = time.Time{}
}

// ----------------------------------------------------------------------------
// MoClaw wire types (WebSocket frames)
// ----------------------------------------------------------------------------

// moclawSendFrame is what we send to the server.
//
// Discovered empirically — server accepts:
//
//	{"type":"send","threadId":"thread_<uuid>","content":"<text>","traceparent":"00-<32hex>-<16hex>-01"}
type moclawSendFrame struct {
	Type        string `json:"type"`
	ThreadID    string `json:"threadId"`
	Content     string `json:"content"`
	Traceparent string `json:"traceparent"`
}

// moclawRX is the wrapper around all server-pushed events.
//
// Two top-level shapes observed:
//  1. Auth result: {"type":"auth_result","success":true,"userId":"..."}
//  2. Stream events: {"type":"stream","event":{"eventId","queryId","threadId","timestamp","event":{"type":<inner>,...}}}
type moclawRX struct {
	Type    string         `json:"type"`
	Success *bool          `json:"success,omitempty"`
	UserID  string         `json:"userId,omitempty"`
	Event   *moclawRXEvent `json:"event,omitempty"`
}

// moclawRXEvent is the middle layer of stream events.
type moclawRXEvent struct {
	EventID   string             `json:"eventId"`
	QueryID   string             `json:"queryId"`
	ThreadID  string             `json:"threadId"`
	Timestamp int64              `json:"timestamp"`
	Event     *moclawRXEventInner `json:"event"`
}

// moclawRXEventInner carries the actual event content.
//
// Observed inner types: query_start, thinking_start, thinking, thinking_end,
// text, query_done.
type moclawRXEventInner struct {
	Type     string                 `json:"type"`
	QueryID  string                 `json:"queryId,omitempty"`
	ThreadID string                 `json:"threadId,omitempty"`
	Text     string                 `json:"text,omitempty"`
	Extra    map[string]interface{} `json:"-"`
}

// UnmarshalJSON for moclawRXEventInner preserves extra fields (e.g., usage).
func (m *moclawRXEventInner) UnmarshalJSON(data []byte) error {
	type alias struct {
		Type     string `json:"type"`
		QueryID  string `json:"queryId,omitempty"`
		ThreadID string `json:"threadId,omitempty"`
		Text     string `json:"text,omitempty"`
	}
	var a alias
	if err := sonic.Unmarshal(data, &a); err != nil {
		return err
	}
	m.Type = a.Type
	m.QueryID = a.QueryID
	m.ThreadID = a.ThreadID
	m.Text = a.Text
	// Also try to keep the original fields for downstream introspection
	_ = json.Unmarshal(data, &m.Extra)
	return nil
}

// ----------------------------------------------------------------------------
// Helper utilities
// ----------------------------------------------------------------------------

// generateTraceparent returns a W3C Trace Context header value with random
// trace and span IDs. Server uses this for tracing only; not validated.
func generateTraceparent() string {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	_, _ = rand.Read(traceID)
	_, _ = rand.Read(spanID)
	return fmt.Sprintf("00-%s-%s-01", hex.EncodeToString(traceID), hex.EncodeToString(spanID))
}

// generateThreadID returns a fresh threadId in MoClaw's format.
func generateThreadID() string {
	return "thread_" + uuid.New().String()
}

// generateChatCompletionID returns an OpenAI-style chat completion ID.
func generateChatCompletionID() string {
	return "chatcmpl-" + uuid.New().String()
}

// extractLastUserMessage flattens the tail user message from a Bifrost
// chat request into a single string (MoClaw's send protocol carries one
// content per frame).
func extractLastUserMessage(input []schemas.ChatMessage) string {
	for i := len(input) - 1; i >= 0; i-- {
		if input[i].Role == schemas.ChatMessageRoleUser {
			return flattenContent(input[i].Content)
		}
	}
	if len(input) > 0 {
		return flattenContent(input[len(input)-1].Content)
	}
	return ""
}

// flattenContent reduces ChatMessageContent to a single string.
func flattenContent(c *schemas.ChatMessageContent) string {
	if c == nil {
		return ""
	}
	if c.ContentStr != nil {
		return *c.ContentStr
	}
	var b strings.Builder
	for _, blk := range c.ContentBlocks {
		if blk.Text != nil && *blk.Text != "" {
			if b.Len() > 0 {
				b.WriteString("\n")
			}
			b.WriteString(*blk.Text)
		}
	}
	return b.String()
}

// ----------------------------------------------------------------------------
// ChatCompletionStream — open WS, send, stream chunks back
// ----------------------------------------------------------------------------

// ChatCompletionStream opens a fresh WebSocket connection to MoClaw's realtime
// endpoint, sends the user's last message as a single "send" frame, and
// streams parsed chunks back via a Bifrost stream channel.
//
// Mapping from MoClaw events → OpenAI-style deltas:
//
//	thinking_start / thinking / thinking_end → choices[0].delta.reasoning
//	text                                     → choices[0].delta.content
//	query_done                               → finish_reason="stop", emit [DONE]
func (p *MoClawProvider) ChatCompletionStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostChatRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	// 1. Resolve access_token (refresh if needed).
	accessToken, err := p.auth.getAccessToken(ctx, key.Value.GetValue())
	if err != nil {
		return nil, providerUtils.NewBifrostOperationError("moclaw: auth refresh failed", err)
	}

	// 2. Dial WebSocket with token in URL query.
	wsURL := defaultWSURL
	if v, ok := p.networkConfig.ExtraHeaders["X-MoClaw-WS-URL"]; ok && v != "" {
		wsURL = v
	}
	wsURL += "?token=" + url.QueryEscape(accessToken)

	// Required headers for the handshake. moclaw.ai's realtime server:
	//   - checks Origin (CSRF defense) — must be https://moclaw.ai
	//   - filters non-browser User-Agents — must look like a real browser
	//   - may inspect Sec-Fetch-* (sent by browsers automatically)
	// Without these, the upgrade returns "bad handshake".
	wsHeaders := http.Header{}
	wsHeaders.Set("Origin", "https://moclaw.ai")
	wsHeaders.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36")
	wsHeaders.Set("Accept-Language", "en-US,en;q=0.9")
	wsHeaders.Set("Cache-Control", "no-cache")
	wsHeaders.Set("Pragma", "no-cache")
	wsHeaders.Set("Sec-Fetch-Site", "same-site")
	wsHeaders.Set("Sec-Fetch-Mode", "websocket")
	wsHeaders.Set("Sec-Fetch-Dest", "websocket")
	for k, v := range p.networkConfig.ExtraHeaders {
		// Allow callers to override defaults via NetworkConfig.ExtraHeaders.
		if strings.HasPrefix(k, "X-MoClaw-WS-") {
			continue // these are adapter directives, not HTTP headers
		}
		wsHeaders.Set(k, v)
	}

	dialer := ws.Dialer{
		HandshakeTimeout: 15 * time.Second,
	}
	conn, dialResp, dialErr := dialer.DialContext(ctx, wsURL, wsHeaders)
	if dialErr != nil {
		// Surface the upstream HTTP response (status + body) to make
		// handshake failures debuggable — websocket error itself is generic.
		body := ""
		status := 0
		if dialResp != nil {
			status = dialResp.StatusCode
			if dialResp.Body != nil {
				buf := make([]byte, 2048)
				n, _ := dialResp.Body.Read(buf)
				body = strings.TrimSpace(string(buf[:n]))
				_ = dialResp.Body.Close()
			}
		}
		// On 401, drop access_token so next call forces a refresh.
		if status == http.StatusUnauthorized {
			p.auth.invalidateAccessToken()
		}
		msg := fmt.Sprintf("moclaw: ws dial failed status=%d body=%q url_token_len=%d", status, body, len(accessToken))
		return nil, providerUtils.NewBifrostOperationError(msg, dialErr)
	}

	// 3. Build send frame.
	threadID := generateThreadID()
	if extras := request.GetExtraParams(); extras != nil {
		if v, ok := extras["thread_id"]; ok {
			if s, ok := v.(string); ok && s != "" {
				threadID = s
			}
		}
	}
	content := extractLastUserMessage(request.Input)
	frame := moclawSendFrame{
		Type:        "send",
		ThreadID:    threadID,
		Content:     content,
		Traceparent: generateTraceparent(),
	}

	if err := conn.WriteJSON(frame); err != nil {
		_ = conn.Close()
		return nil, providerUtils.NewBifrostOperationError("moclaw: ws write failed", err)
	}

	// 4. Stream goroutine.
	responseChan := make(chan *schemas.BifrostStreamChunk, schemas.DefaultStreamBufferSize)
	providerUtils.SetStreamIdleTimeoutIfEmpty(ctx, p.networkConfig.StreamIdleTimeoutInSeconds)

	respID := generateChatCompletionID()
	model := request.Model
	startTime := time.Now()

	go func() {
		defer providerUtils.EnsureStreamFinalizerCalled(ctx, postHookSpanFinalizer)
		defer func() {
			if ctx.Err() == context.Canceled {
				providerUtils.HandleStreamCancellation(ctx, postHookRunner, responseChan, p.logger, postHookSpanFinalizer, nil)
			} else if ctx.Err() == context.DeadlineExceeded {
				providerUtils.HandleStreamTimeout(ctx, postHookRunner, responseChan, p.logger, postHookSpanFinalizer, nil)
			}
			close(responseChan)
		}()
		defer conn.Close()

		chunkIndex := 0
		lastChunkTime := startTime

		emit := func(chatResp *schemas.BifrostChatResponse) {
			if chatResp == nil {
				return
			}
			chatResp.ID = respID
			chatResp.Model = model
			chatResp.Object = objectChatCompletionChunk
			chatResp.Created = int(time.Now().Unix())
			chatResp.ExtraFields.ChunkIndex = chunkIndex
			chatResp.ExtraFields.Latency = time.Since(lastChunkTime).Milliseconds()
			chunkIndex++
			lastChunkTime = time.Now()
			providerUtils.ProcessAndSendResponse(
				ctx, postHookRunner,
				providerUtils.GetBifrostResponseForStreamResponse(nil, chatResp, nil, nil, nil, nil),
				responseChan, postHookSpanFinalizer,
			)
		}

		for {
			if ctx.Err() != nil {
				return
			}
			_, msgBytes, err := conn.ReadMessage()
			if err != nil {
				if !ws.IsCloseError(err, ws.CloseNormalClosure) {
					p.logger.Warn("moclaw: ws read error: %v", err)
				}
				return
			}

			var rx moclawRX
			if err := sonic.Unmarshal(msgBytes, &rx); err != nil {
				p.logger.Warn("moclaw: bad ws frame: %v", err)
				continue
			}

			// Auth result frame is informational; first frame after dial.
			if rx.Type == "auth_result" {
				if rx.Success != nil && !*rx.Success {
					p.auth.invalidateAccessToken()
					ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, true)
					providerUtils.ProcessAndSendBifrostError(ctx, postHookRunner, &schemas.BifrostError{
						IsBifrostError: false,
						Error:          &schemas.ErrorField{Message: "moclaw: ws auth_result success=false"},
					}, responseChan, p.logger, postHookSpanFinalizer)
					return
				}
				continue
			}

			if rx.Type != "stream" || rx.Event == nil || rx.Event.Event == nil {
				continue
			}
			inner := rx.Event.Event

			switch inner.Type {
			case "query_start", "thinking_start", "thinking_end":
				// Phase markers — no content to emit.
				continue

			case "thinking":
				if inner.Text == "" {
					continue
				}
				reasoning := inner.Text
				emit(&schemas.BifrostChatResponse{
					Choices: []schemas.BifrostResponseChoice{
						{
							Index: 0,
							ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
								Delta: &schemas.ChatStreamResponseChoiceDelta{Reasoning: &reasoning},
							},
						},
					},
				})

			case "text":
				if inner.Text == "" {
					continue
				}
				text := inner.Text
				emit(&schemas.BifrostChatResponse{
					Choices: []schemas.BifrostResponseChoice{
						{
							Index: 0,
							ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
								Delta: &schemas.ChatStreamResponseChoiceDelta{Content: &text},
							},
						},
					},
				})

			case "query_done":
				stop := string(schemas.BifrostFinishReasonStop)
				ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, true)
				final := &schemas.BifrostChatResponse{
					Choices: []schemas.BifrostResponseChoice{
						{
							Index:        0,
							FinishReason: &stop,
							ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
								Delta: &schemas.ChatStreamResponseChoiceDelta{},
							},
						},
					},
				}
				final.ExtraFields.Latency = time.Since(startTime).Milliseconds()
				emit(final)
				return
			}
		}
	}()

	return responseChan, nil
}

// ----------------------------------------------------------------------------
// ChatCompletion — non-streaming variant: aggregate the stream
// ----------------------------------------------------------------------------

// ChatCompletion aggregates the WebSocket stream into a single response.
func (p *MoClawProvider) ChatCompletion(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostChatRequest) (*schemas.BifrostChatResponse, *schemas.BifrostError) {
	// Reuse the streaming impl by collecting chunks.
	chunkCh, bErr := p.ChatCompletionStream(ctx, nil, nil, key, request)
	if bErr != nil {
		return nil, bErr
	}

	var (
		content    strings.Builder
		reasoning  strings.Builder
		startedAt  = time.Now()
	)

	for chunk := range chunkCh {
		if chunk == nil || chunk.BifrostChatResponse == nil {
			continue
		}
		for _, c := range chunk.BifrostChatResponse.Choices {
			if c.ChatStreamResponseChoice == nil || c.ChatStreamResponseChoice.Delta == nil {
				continue
			}
			d := c.ChatStreamResponseChoice.Delta
			if d.Content != nil {
				content.WriteString(*d.Content)
			}
			if d.Reasoning != nil {
				reasoning.WriteString(*d.Reasoning)
			}
		}
	}

	finalContent := content.String()
	finishReason := string(schemas.BifrostFinishReasonStop)
	role := string(schemas.ChatMessageRoleAssistant)
	contentPtr := finalContent

	resp := &schemas.BifrostChatResponse{
		ID:      generateChatCompletionID(),
		Object:  objectChatCompletion,
		Created: int(time.Now().Unix()),
		Model:   request.Model,
		Choices: []schemas.BifrostResponseChoice{
			{
				Index:        0,
				FinishReason: &finishReason,
				ChatNonStreamResponseChoice: &schemas.ChatNonStreamResponseChoice{
					Message: &schemas.ChatMessage{
						Role:    schemas.ChatMessageRole(role),
						Content: &schemas.ChatMessageContent{ContentStr: &contentPtr},
					},
				},
			},
		},
	}
	if reasoning.Len() > 0 {
		if resp.ExtraParams == nil {
			resp.ExtraParams = map[string]interface{}{}
		}
		resp.ExtraParams["reasoning"] = reasoning.String()
	}
	resp.ExtraFields.Latency = time.Since(startedAt).Milliseconds()
	return resp, nil
}

// ----------------------------------------------------------------------------
// Unsupported operations
// ----------------------------------------------------------------------------

func (p *MoClawProvider) ListModels(ctx *schemas.BifrostContext, keys []schemas.Key, request *schemas.BifrostListModelsRequest) (*schemas.BifrostListModelsResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ListModelsRequest, schemas.MoClaw)
}
func (p *MoClawProvider) TextCompletion(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostTextCompletionRequest) (*schemas.BifrostTextCompletionResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError("text completion", "moclaw")
}
func (p *MoClawProvider) TextCompletionStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostTextCompletionRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError("text completion", "moclaw")
}
func (p *MoClawProvider) Responses(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostResponsesRequest) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
	chatResponse, err := p.ChatCompletion(ctx, key, request.ToChatRequest())
	if err != nil {
		return nil, err
	}
	return chatResponse.ToBifrostResponsesResponse(), nil
}
func (p *MoClawProvider) ResponsesStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostResponsesRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	ctx.SetValue(schemas.BifrostContextKeyIsResponsesToChatCompletionFallback, true)
	return p.ChatCompletionStream(ctx, postHookRunner, postHookSpanFinalizer, key, request.ToChatRequest())
}
func (p *MoClawProvider) Embedding(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.EmbeddingRequest, schemas.MoClaw)
}
func (p *MoClawProvider) Speech(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostSpeechRequest) (*schemas.BifrostSpeechResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.SpeechRequest, schemas.MoClaw)
}
func (p *MoClawProvider) SpeechStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostSpeechRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.SpeechStreamRequest, schemas.MoClaw)
}
func (p *MoClawProvider) Transcription(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostTranscriptionRequest) (*schemas.BifrostTranscriptionResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.TranscriptionRequest, schemas.MoClaw)
}
func (p *MoClawProvider) TranscriptionStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostTranscriptionRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.TranscriptionStreamRequest, schemas.MoClaw)
}
func (p *MoClawProvider) Rerank(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostRerankRequest) (*schemas.BifrostRerankResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.RerankRequest, schemas.MoClaw)
}
func (p *MoClawProvider) OCR(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostOCRRequest) (*schemas.BifrostOCRResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.OCRRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ImageGeneration(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostImageGenerationRequest) (*schemas.BifrostImageGenerationResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ImageGenerationRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ImageGenerationStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostImageGenerationRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ImageGenerationStreamRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ImageEdit(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostImageEditRequest) (*schemas.BifrostImageGenerationResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ImageEditRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ImageEditStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostImageEditRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ImageEditStreamRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ImageVariation(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostImageVariationRequest) (*schemas.BifrostImageGenerationResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ImageVariationRequest, schemas.MoClaw)
}
func (p *MoClawProvider) VideoGeneration(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostVideoGenerationRequest) (*schemas.BifrostVideoGenerationResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.VideoGenerationRequest, schemas.MoClaw)
}
func (p *MoClawProvider) VideoRetrieve(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostVideoRetrieveRequest) (*schemas.BifrostVideoGenerationResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.VideoRetrieveRequest, schemas.MoClaw)
}
func (p *MoClawProvider) VideoDownload(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostVideoDownloadRequest) (*schemas.BifrostVideoDownloadResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.VideoDownloadRequest, schemas.MoClaw)
}
func (p *MoClawProvider) VideoDelete(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostVideoDeleteRequest) (*schemas.BifrostVideoDeleteResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.VideoDeleteRequest, schemas.MoClaw)
}
func (p *MoClawProvider) VideoList(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostVideoListRequest) (*schemas.BifrostVideoListResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.VideoListRequest, schemas.MoClaw)
}
func (p *MoClawProvider) VideoRemix(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostVideoRemixRequest) (*schemas.BifrostVideoGenerationResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.VideoRemixRequest, schemas.MoClaw)
}
func (p *MoClawProvider) BatchCreate(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostBatchCreateRequest) (*schemas.BifrostBatchCreateResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.BatchCreateRequest, schemas.MoClaw)
}
func (p *MoClawProvider) BatchList(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostBatchListRequest) (*schemas.BifrostBatchListResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.BatchListRequest, schemas.MoClaw)
}
func (p *MoClawProvider) BatchRetrieve(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostBatchRetrieveRequest) (*schemas.BifrostBatchRetrieveResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.BatchRetrieveRequest, schemas.MoClaw)
}
func (p *MoClawProvider) BatchCancel(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostBatchCancelRequest) (*schemas.BifrostBatchCancelResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.BatchCancelRequest, schemas.MoClaw)
}
func (p *MoClawProvider) BatchDelete(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostBatchDeleteRequest) (*schemas.BifrostBatchDeleteResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.BatchDeleteRequest, schemas.MoClaw)
}
func (p *MoClawProvider) BatchResults(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostBatchResultsRequest) (*schemas.BifrostBatchResultsResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.BatchResultsRequest, schemas.MoClaw)
}
func (p *MoClawProvider) FileUpload(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostFileUploadRequest) (*schemas.BifrostFileUploadResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.FileUploadRequest, schemas.MoClaw)
}
func (p *MoClawProvider) FileList(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostFileListRequest) (*schemas.BifrostFileListResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.FileListRequest, schemas.MoClaw)
}
func (p *MoClawProvider) FileRetrieve(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostFileRetrieveRequest) (*schemas.BifrostFileRetrieveResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.FileRetrieveRequest, schemas.MoClaw)
}
func (p *MoClawProvider) FileDelete(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostFileDeleteRequest) (*schemas.BifrostFileDeleteResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.FileDeleteRequest, schemas.MoClaw)
}
func (p *MoClawProvider) FileContent(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostFileContentRequest) (*schemas.BifrostFileContentResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.FileContentRequest, schemas.MoClaw)
}
func (p *MoClawProvider) CountTokens(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostResponsesRequest) (*schemas.BifrostCountTokensResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.CountTokensRequest, schemas.MoClaw)
}
func (p *MoClawProvider) CachedContentCreate(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostCachedContentCreateRequest) (*schemas.BifrostCachedContentCreateResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.CachedContentCreateRequest, schemas.MoClaw)
}
func (p *MoClawProvider) CachedContentList(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostCachedContentListRequest) (*schemas.BifrostCachedContentListResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.CachedContentListRequest, schemas.MoClaw)
}
func (p *MoClawProvider) CachedContentRetrieve(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostCachedContentRetrieveRequest) (*schemas.BifrostCachedContentRetrieveResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.CachedContentRetrieveRequest, schemas.MoClaw)
}
func (p *MoClawProvider) CachedContentUpdate(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostCachedContentUpdateRequest) (*schemas.BifrostCachedContentUpdateResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.CachedContentUpdateRequest, schemas.MoClaw)
}
func (p *MoClawProvider) CachedContentDelete(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostCachedContentDeleteRequest) (*schemas.BifrostCachedContentDeleteResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.CachedContentDeleteRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ContainerCreate(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostContainerCreateRequest) (*schemas.BifrostContainerCreateResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerCreateRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ContainerList(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerListRequest) (*schemas.BifrostContainerListResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerListRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ContainerRetrieve(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerRetrieveRequest) (*schemas.BifrostContainerRetrieveResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerRetrieveRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ContainerDelete(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerDeleteRequest) (*schemas.BifrostContainerDeleteResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerDeleteRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ContainerFileCreate(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostContainerFileCreateRequest) (*schemas.BifrostContainerFileCreateResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerFileCreateRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ContainerFileList(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerFileListRequest) (*schemas.BifrostContainerFileListResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerFileListRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ContainerFileRetrieve(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerFileRetrieveRequest) (*schemas.BifrostContainerFileRetrieveResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerFileRetrieveRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ContainerFileContent(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerFileContentRequest) (*schemas.BifrostContainerFileContentResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerFileContentRequest, schemas.MoClaw)
}
func (p *MoClawProvider) ContainerFileDelete(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerFileDeleteRequest) (*schemas.BifrostContainerFileDeleteResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerFileDeleteRequest, schemas.MoClaw)
}
func (p *MoClawProvider) Passthrough(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostPassthroughRequest) (*schemas.BifrostPassthroughResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.PassthroughRequest, schemas.MoClaw)
}
func (p *MoClawProvider) PassthroughStream(_ *schemas.BifrostContext, _ schemas.PostHookRunner, _ func(context.Context), _ schemas.Key, _ *schemas.BifrostPassthroughRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.PassthroughStreamRequest, schemas.MoClaw)
}
