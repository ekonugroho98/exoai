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
//
//   - API Key (in Bifrost): either the Auth0 *refresh_token* or the raw
//     *access_token* extracted from a logged-in browser session at moclaw.ai.
//
//     Refresh token (preferred, lasts until unused for 30 days):
//     DevTools → Application → Local Storage → @@auth0spajs@@::...::offline_access → body.refresh_token
//
//     Access token (fallback, 24 h TTL):
//     DevTools → Application → Local Storage → @@auth0spajs@@::...::offline_access → body.access_token
//     (starts with "eyJ"; the adapter detects this and skips the Auth0 exchange)
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
	defaultAuth0Domain   = "https://auth.moclaw.ai"
	defaultAuth0ClientID = "R7QyN3rYIv2DSEqkgQJjfSvvb6XFxMOu"
	defaultWSURL         = "wss://realtime.moclaw.ai/ws"
	defaultAPIURL        = "https://api.moclaw.ai"

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
//
// Each configured Key (account) gets its own isolated authState so that
// multiple moclaw.ai accounts can be used concurrently without token
// cross-contamination or rotation races.
type MoClawProvider struct {
	logger              schemas.Logger
	httpClient          *fasthttp.Client
	networkConfig       schemas.NetworkConfig
	sendBackRawRequest  bool
	sendBackRawResponse bool

	// Per-key auth state. Keyed by the Key's unique identifier so every
	// configured account gets its own refresh/access-token lifecycle and
	// its own on-disk rotation cache.
	authsMu sync.Mutex
	auths   map[string]*authState

	// Shared Auth0 config (same for all keys on this provider instance).
	auth0ClientID string
	auth0URL      string
}

// NewMoClawProvider constructs a MoClawProvider.
//
// The provider configuration's API key (Key.Value) MUST be the Auth0
// refresh_token extracted from a logged-in moclaw.ai session.
func NewMoClawProvider(config *schemas.ProviderConfig, logger schemas.Logger) (*MoClawProvider, error) {
	config.CheckAndSetDefaults()

	// MoClaw supports multiple accounts (keys). Enable retries so the gateway
	// can rotate to another key when one account's refresh_token is stale.
	if config.NetworkConfig.MaxRetries < 5 {
		config.NetworkConfig.MaxRetries = 5
	}

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

	// Resolve Auth0 config (can be overridden via ExtraHeaders).
	auth0ClientID := defaultAuth0ClientID
	if v, ok := config.NetworkConfig.ExtraHeaders["X-MoClaw-Auth0-ClientId"]; ok && v != "" {
		auth0ClientID = v
	}

	return &MoClawProvider{
		logger:              logger,
		httpClient:          httpClient,
		networkConfig:       config.NetworkConfig,
		sendBackRawRequest:  config.SendBackRawRequest,
		sendBackRawResponse: config.SendBackRawResponse,
		auths:               make(map[string]*authState),
		auth0ClientID:       auth0ClientID,
		auth0URL:            defaultAuth0Domain + "/oauth/token",
	}, nil
}

// resolveRefreshTokenCachePathForKey returns the path where rotated refresh
// tokens are persisted for a specific key. Files are namespaced per keyID to
// prevent cross-contamination between multiple moclaw.ai accounts.
// Honors $BIFROST_DATA_DIR if set; falls back to ~/.bifrost/.
func resolveRefreshTokenCachePathForKey(keyID string) string {
	dir := os.Getenv("BIFROST_DATA_DIR")
	if dir == "" {
		if home, err := os.UserHomeDir(); err == nil {
			dir = filepath.Join(home, ".bifrost")
		}
	}
	if dir == "" {
		dir = "."
	}
	_ = os.MkdirAll(dir, 0700)
	// Sanitize keyID so it is safe to use as a filename component.
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, keyID)
	if safe == "" {
		safe = "default"
	}
	return filepath.Join(dir, "moclaw_refresh_"+safe+".txt")
}

// GetProviderKey returns the provider identifier.
func (p *MoClawProvider) GetProviderKey() schemas.ModelProvider {
	return schemas.MoClaw
}

// getOrCreateAuth returns the authState for the given key, creating it lazily
// on first use. Each key gets its own isolated authState with its own on-disk
// rotation cache, so multiple moclaw.ai accounts can run concurrently without
// token cross-contamination or rotation races.
func (p *MoClawProvider) getOrCreateAuth(key schemas.Key) *authState {
	keyID := key.ID
	if keyID == "" {
		keyID = "default"
	}
	p.authsMu.Lock()
	defer p.authsMu.Unlock()
	if a, ok := p.auths[keyID]; ok {
		return a
	}
	a := &authState{
		clientID:      p.auth0ClientID,
		auth0URL:      p.auth0URL,
		httpClient:    p.httpClient,
		logger:        p.logger,
		cachePath:     resolveRefreshTokenCachePathForKey(keyID),
		inFlight:      make(chan struct{}, 1), // capacity 1 = "at most one WS request in flight"
		creditBalance: -1,                     // -1 = unknown until first fetch
	}
	p.auths[keyID] = a
	return a
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

	// Per-account session state (v2 API). MoClaw requires a valid
	// session+thread before the WS will accept "send" frames.
	appSessionID    string // e.g. "sess_XSNeqpNFs2UspqsK"
	sessionID       string // e.g. "session_fc72c110-..."
	threadID        string // e.g. "thread_d19d216d-..."
	threadBootstrap bool   // true once bootstrap greeting has been consumed

	// Persistent WS connection. MoClaw's agent always greets on the
	// first message of a new WS connection. By keeping the connection
	// open and reusing it, subsequent messages get real responses.
	wsConn *ws.Conn   // nil = not connected
	wsMu   sync.Mutex // serializes WS send/recv (one request at a time)

	// inFlight is a 1-slot semaphore that serializes WS requests for
	// THIS account. MoClaw's WS is auth-scoped (one user = one event
	// bus), so opening multiple parallel WS connections under the same
	// token causes server-side event cross-routing (we observed this
	// empirically — see provider docstring). Forcing one-in-flight per
	// key sidesteps that without throwing requests away: parallel callers
	// queue here, or rotate to other keys via Bifrost retry if this one
	// is busy.
	inFlight chan struct{}

	// Credit accounting cache. Refreshed on a TTL and on every observed
	// "insufficient credits" error from the server.
	creditMu        sync.Mutex
	creditBalance   int       // last-known balance; -1 = unknown
	creditFetchedAt time.Time // when creditBalance was set
	creditExhausted bool      // true after server returned an out-of-credit error
	creditResetAt   time.Time // server-side reset time (period_end of trial wallet)
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

// invalidateSession forces the next call to re-fetch the session.
func (a *authState) invalidateSession() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.appSessionID = ""
	a.sessionID = ""
	a.threadID = ""
	a.threadBootstrap = false
}

// getThreadID creates a fresh thread for each request via the v2 REST API.
// MoClaw threads accumulate conversation context — reusing a thread causes
// the model to answer from cached context instead of processing the new prompt.
// Each request gets its own thread to ensure isolation.
func (a *authState) getThreadID(accessToken string, httpClient *fasthttp.Client, apiURL string) (string, error) {
	// Resolve app_session_id (cached — same for all threads in this account)
	a.mu.Lock()
	appSession := a.appSessionID
	a.mu.Unlock()

	if appSession == "" {
		// Fetch active session first
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseResponse(resp)

		req.SetRequestURI(apiURL + "/api/v2/sessions/active?channel=web")
		req.Header.SetMethod(http.MethodGet)
		req.Header.Set("Authorization", "Bearer "+accessToken)
		req.Header.Set("Origin", "https://moclaw.ai")
		req.Header.Set("Accept", "application/json")

		if err := httpClient.Do(req, resp); err != nil {
			return "", fmt.Errorf("moclaw session fetch failed: %w", err)
		}
		if resp.StatusCode() != fasthttp.StatusOK {
			return "", fmt.Errorf("moclaw session fetch status=%d body=%s", resp.StatusCode(), string(resp.Body()[:min(200, len(resp.Body()))]))
		}

		var sessionResp struct {
			AppSessionID string `json:"app_session_id"`
		}
		if err := sonic.Unmarshal(resp.Body(), &sessionResp); err != nil {
			return "", fmt.Errorf("moclaw session decode failed: %w", err)
		}
		if sessionResp.AppSessionID == "" {
			return "", fmt.Errorf("moclaw session response has no app_session_id")
		}
		a.mu.Lock()
		a.appSessionID = sessionResp.AppSessionID
		a.mu.Unlock()
		appSession = sessionResp.AppSessionID
	}

	// Create a NEW thread for this request
	req2 := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req2)
	resp2 := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp2)

	req2.SetRequestURI(apiURL + "/api/v2/sessions/" + appSession + "/threads")
	req2.Header.SetMethod(http.MethodPost)
	req2.Header.Set("Authorization", "Bearer "+accessToken)
	req2.Header.Set("Origin", "https://moclaw.ai")
	req2.Header.SetContentType("application/json")
	req2.SetBody([]byte("{}"))

	if err := httpClient.Do(req2, resp2); err != nil {
		return "", fmt.Errorf("moclaw thread create failed: %w", err)
	}
	if resp2.StatusCode() != fasthttp.StatusOK && resp2.StatusCode() != fasthttp.StatusCreated {
		return "", fmt.Errorf("moclaw thread create status=%d", resp2.StatusCode())
	}

	var threadResp struct {
		Session struct {
			ThreadID string `json:"threadId"`
		} `json:"session"`
	}
	if err := sonic.Unmarshal(resp2.Body(), &threadResp); err != nil {
		return "", fmt.Errorf("moclaw thread decode failed: %w", err)
	}
	if threadResp.Session.ThreadID == "" {
		return "", fmt.Errorf("moclaw thread response has no threadId")
	}

	if a.logger != nil {
		a.logger.Debug("moclaw: created fresh thread %s for request", threadResp.Session.ThreadID)
	}
	return threadResp.Session.ThreadID, nil
}

// ----------------------------------------------------------------------------
// Per-key concurrency limit
// ----------------------------------------------------------------------------

// acquireSlot waits for an open in-flight slot on this account, or returns
// an error if ctx expires first.
//
// MoClaw's WS routes events to whichever socket happens to be reading for
// the account, not to the socket that initiated each request. Two parallel
// WS connections under the same token end up cross-contaminating responses
// (we observed this empirically — see ChatCompletionStream comments). The
// safest workaround is to ensure only one WS request is in flight per
// account at any time; concurrent callers queue here. Multi-account
// deployments naturally parallelize because Bifrost rotates across keys.
func (a *authState) acquireSlot(ctx context.Context) error {
	select {
	case a.inFlight <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseSlot frees the in-flight slot. Safe to call multiple times (the
// non-blocking select drains at most once).
func (a *authState) releaseSlot() {
	select {
	case <-a.inFlight:
	default:
	}
}

// ----------------------------------------------------------------------------
// Credit accounting
// ----------------------------------------------------------------------------

const (
	// creditBalanceTTL is how long we trust a cached balance.
	creditBalanceTTL = 60 * time.Second

	// creditSafetyThreshold — below this many credits we skip the key.
	// Picked so a single Opus turn (~3-5 credits typical) still fits.
	creditSafetyThreshold = 5
)

// moclawCreditBalanceResp is the /api/credits/balance response shape.
type moclawCreditBalanceResp struct {
	TotalBalance int `json:"total_balance"`
	Wallets      []struct {
		Balance   int    `json:"balance"`
		ExpiresAt string `json:"expires_at"`
	} `json:"wallets"`
}

// fetchBalance pulls the current credit balance from MoClaw. Uses the
// supplied access token; does NOT take a.mu (caller must not hold it
// either, since this does I/O).
func (a *authState) fetchBalance(accessToken string, apiURL string) (int, time.Time, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(apiURL + "/api/credits/balance")
	req.Header.SetMethod(http.MethodGet)
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	if err := a.httpClient.Do(req, resp); err != nil {
		return 0, time.Time{}, fmt.Errorf("balance fetch: %w", err)
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		return 0, time.Time{}, fmt.Errorf("balance fetch status=%d", resp.StatusCode())
	}
	var b moclawCreditBalanceResp
	if err := sonic.Unmarshal(resp.Body(), &b); err != nil {
		return 0, time.Time{}, fmt.Errorf("balance decode: %w", err)
	}

	var resetAt time.Time
	if len(b.Wallets) > 0 {
		if t, perr := time.Parse(time.RFC3339, b.Wallets[0].ExpiresAt); perr == nil {
			resetAt = t
		}
	}
	return b.TotalBalance, resetAt, nil
}

// ensureCreditsAvailable returns nil if the key is good to use, or a
// descriptive error if the account is too low or known-exhausted.
//
// Caches the balance for creditBalanceTTL to avoid hammering the API.
// If the cache holds an "exhausted" flag, returns an exhaustion error
// until creditResetAt passes.
func (a *authState) ensureCreditsAvailable(accessToken, apiURL string) error {
	a.creditMu.Lock()
	if a.creditExhausted && time.Now().Before(a.creditResetAt) {
		resetAt := a.creditResetAt
		a.creditMu.Unlock()
		return fmt.Errorf("moclaw: account credits exhausted until %s", resetAt.Format(time.RFC3339))
	}
	cachedFresh := time.Since(a.creditFetchedAt) < creditBalanceTTL && a.creditBalance >= 0
	cachedBalance := a.creditBalance
	a.creditMu.Unlock()

	if cachedFresh {
		if cachedBalance < creditSafetyThreshold {
			return fmt.Errorf("moclaw: account credits low (%d, threshold %d)", cachedBalance, creditSafetyThreshold)
		}
		return nil
	}

	// Refresh from server.
	balance, resetAt, err := a.fetchBalance(accessToken, apiURL)
	if err != nil {
		// Don't fail the request if balance check itself fails — best effort.
		if a.logger != nil {
			a.logger.Warn("moclaw: balance check failed (treating as ok): %v", err)
		}
		return nil
	}

	a.creditMu.Lock()
	a.creditBalance = balance
	a.creditFetchedAt = time.Now()
	if !resetAt.IsZero() {
		a.creditResetAt = resetAt
	}
	a.creditMu.Unlock()

	if balance < creditSafetyThreshold {
		return fmt.Errorf("moclaw: account credits low (%d, threshold %d)", balance, creditSafetyThreshold)
	}
	return nil
}

// markCreditsExhausted records that the account has run out, so future
// requests fast-fail (and Bifrost rotates to another key) until reset.
// Pass the period_end if known so we can clear the flag automatically.
func (a *authState) markCreditsExhausted(resetAt time.Time) {
	a.creditMu.Lock()
	a.creditExhausted = true
	a.creditBalance = 0
	a.creditFetchedAt = time.Now()
	if !resetAt.IsZero() {
		a.creditResetAt = resetAt
	}
	a.creditMu.Unlock()
	if a.logger != nil {
		a.logger.Warn("moclaw: credits exhausted on this key; will skip until %s", resetAt.Format(time.RFC3339))
	}
}

// isBootstrapped reports whether this thread has already consumed the
// MoClaw onboarding greeting. Thread-safe.
func (a *authState) isBootstrapped() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.threadBootstrap
}

// markBootstrapped marks the current thread as bootstrapped.
func (a *authState) markBootstrapped() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.threadBootstrap = true
}

// bootstrapThread sends a silent "hi" to a fresh MoClaw thread and drains
// the greeting response. MoClaw's agent system always replies with a
// greeting on the first message of every new thread (BOOTSTRAP.md +
// SOUL.md onboarding). This must be consumed before real prompts work.
//
// Returns nil on success; the WS connection is ready for the real prompt.
func (a *authState) bootstrapThread(conn *ws.Conn, threadID string, logger schemas.Logger) error {
	frame := moclawSendFrame{
		Type:        "send",
		ThreadID:    threadID,
		Content:     "hi",
		Traceparent: generateTraceparent(),
	}
	if err := conn.WriteJSON(frame); err != nil {
		return fmt.Errorf("bootstrap send failed: %w", err)
	}
	logger.Debug("moclaw: bootstrapping thread %s (consuming greeting)", threadID)

	// Drain until query_done or timeout (max 15s for greeting).
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(deadline)
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("bootstrap read failed: %w", err)
		}
		var rx moclawRX
		if err := sonic.Unmarshal(msgBytes, &rx); err != nil {
			continue
		}
		// auth_result comes first on fresh connections
		if rx.Type == "auth_result" {
			if rx.Success != nil && !*rx.Success {
				return fmt.Errorf("bootstrap auth failed")
			}
			continue
		}
		if rx.Type == "stream" && rx.Event != nil && rx.Event.Event != nil {
			if rx.Event.Event.Type == "query_done" {
				break
			}
		}
	}
	// Reset read deadline for the real conversation
	_ = conn.SetReadDeadline(time.Time{})

	a.markBootstrapped()
	logger.Debug("moclaw: thread %s bootstrapped successfully", threadID)
	return nil
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
	EventID   string              `json:"eventId"`
	QueryID   string              `json:"queryId"`
	ThreadID  string              `json:"threadId"`
	Timestamp int64               `json:"timestamp"`
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
//
// Kept for callers that intentionally want only the last turn (e.g.
// single-shot probes). Multi-turn callers should use flattenConversation
// to preserve OpenAI semantics — see ChatCompletionStream.
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

// flattenConversation collapses a full OpenAI-style messages array into a
// single prompt string for MoClaw's single-content-per-frame protocol.
//
// Why this exists:
//   MoClaw's WS `send` frame carries one `content` field — there is no
//   schema for prior turns. The server does track its own per-thread
//   conversation history, but OpenAI clients (Cline, Continue, openai-sdk,
//   etc.) re-send the FULL conversation on every request, expecting the
//   model to use that history (not the server's). If we only forward the
//   tail user message, the model will respond based on stale server-side
//   memory rather than the client's intended context.
//
// Format:
//   Single-message requests (just one user turn) pass through unchanged —
//   no role markers, no preamble — to keep simple probes clean.
//
//   Multi-turn requests get rendered as:
//     [system]
//     <system content>
//
//     [user]
//     <turn 1 user>
//
//     [assistant]
//     <turn 1 assistant>
//
//     [user]
//     <turn 2 user>
//
//   The trailing user turn is what the model is asked to respond to.
//   Tool calls and other non-text content are flattened to text only;
//   MoClaw doesn't expose tool calling via this transport.
func flattenConversation(input []schemas.ChatMessage) string {
	if len(input) == 0 {
		return ""
	}
	// Fast path: single message → no role markup (cleanest output).
	if len(input) == 1 {
		return flattenContent(input[0].Content)
	}
	// Fast path: only the last message has content and it's user → same
	// as single-turn.
	nonEmpty := 0
	var lastUserOnly bool = true
	for i, m := range input {
		text := flattenContent(m.Content)
		if text != "" {
			nonEmpty++
			if i != len(input)-1 || m.Role != schemas.ChatMessageRoleUser {
				lastUserOnly = false
			}
		}
	}
	if nonEmpty == 1 && lastUserOnly {
		return flattenContent(input[len(input)-1].Content)
	}

	var b strings.Builder
	for _, m := range input {
		text := flattenContent(m.Content)
		if text == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString("[")
		b.WriteString(string(m.Role))
		b.WriteString("]\n")
		b.WriteString(text)
	}
	return b.String()
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
// Persistent WS connection management
// ----------------------------------------------------------------------------

// dialWS creates a new WebSocket connection to MoClaw's realtime endpoint.
func (p *MoClawProvider) dialWS(ctx context.Context, accessToken string) (*ws.Conn, error) {
	wsURL := defaultWSURL
	if v, ok := p.networkConfig.ExtraHeaders["X-MoClaw-WS-URL"]; ok && v != "" {
		wsURL = v
	}
	wsURL += "?token=" + url.QueryEscape(accessToken)

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
		if strings.HasPrefix(k, "X-MoClaw-WS-") {
			continue
		}
		wsHeaders.Set(k, v)
	}

	dialer := ws.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, dialResp, err := dialer.DialContext(ctx, wsURL, wsHeaders)
	if err != nil {
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
		return nil, fmt.Errorf("ws dial failed status=%d body=%q: %w", status, body, err)
	}
	return conn, nil
}

// getOrDialWS returns the persistent WS connection for this auth state,
// dialing a new one if needed. Also performs bootstrap (greeting drain)
// on fresh connections. Caller must hold auth.wsMu.
func (a *authState) getOrDialWS(ctx context.Context, p *MoClawProvider, accessToken, threadID string) (*ws.Conn, error) {
	// Check if existing connection is alive
	if a.wsConn != nil {
		// Quick ping check
		if err := a.wsConn.WriteControl(ws.PingMessage, nil, time.Now().Add(2*time.Second)); err == nil {
			return a.wsConn, nil
		}
		// Dead connection — close and reconnect
		_ = a.wsConn.Close()
		a.wsConn = nil
		a.threadBootstrap = false
	}

	// Dial new connection
	conn, err := p.dialWS(ctx, accessToken)
	if err != nil {
		return nil, err
	}

	// Wait for auth_result
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, authMsg, err := conn.ReadMessage()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ws auth read failed: %w", err)
	}
	var authRX moclawRX
	if err := sonic.Unmarshal(authMsg, &authRX); err == nil {
		if authRX.Type == "auth_result" && authRX.Success != nil && !*authRX.Success {
			_ = conn.Close()
			return nil, fmt.Errorf("ws auth_result success=false")
		}
	}
	_ = conn.SetReadDeadline(time.Time{})

	// NOTE: bootstrap removed (2026-05).
	//
	// Original assumption was: "MoClaw always greets on the first message of
	// a new thread, so we must drain that greeting before real prompts work."
	//
	// That's only true for *fresh* threads. But getThreadID() returns the
	// user's session-bound thread from /api/v2/sessions/active — a
	// long-lived thread already populated with history. Sending "hi" to it
	// just appends to that history (wastes a credit, pollutes context), and
	// the response is a real reply, not a greeting.
	//
	// Net effect of removing bootstrap:
	//   - Saves one credit per fresh WS connection.
	//   - Eliminates leak of bootstrap-response events into the first real
	//     request's stream.
	//   - For genuinely fresh threads (callers overriding via
	//     ExtraParams.thread_id), any greeting events arrive as normal
	//     stream events; the read loop ignores non-text/non-thinking types
	//     via `continue`, so they're dropped harmlessly.
	a.threadBootstrap = true // keep flag set so legacy code paths see "bootstrapped"

	a.wsConn = conn
	return conn, nil
}

// closeWS closes the persistent WS connection.
func (a *authState) closeWS() {
	if a.wsConn != nil {
		_ = a.wsConn.Close()
		a.wsConn = nil
		a.threadBootstrap = false
	}
}

// ----------------------------------------------------------------------------
// ChatCompletionStream — open WS, send, stream chunks back
// ----------------------------------------------------------------------------

// ChatCompletionStream uses a persistent WebSocket connection to MoClaw's
// realtime endpoint. The first request bootstraps (consumes the greeting),
// subsequent requests reuse the same connection for real responses.
//
// Mapping from MoClaw events → OpenAI-style deltas:
//
//	thinking_start / thinking / thinking_end → choices[0].delta.reasoning
//	text                                     → choices[0].delta.content
//	query_done                               → finish_reason="stop", emit [DONE]
func (p *MoClawProvider) ChatCompletionStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostChatRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	// 1. Resolve access_token (refresh if needed).
	auth := p.getOrCreateAuth(key)
	accessToken, err := auth.getAccessToken(ctx, key.Value.GetValue())
	if err != nil {
		statusCode := 401
		errType := "auth_error"
		return nil, &schemas.BifrostError{
			IsBifrostError: false,
			StatusCode:     &statusCode,
			Error: &schemas.ErrorField{
				Message: "moclaw: auth refresh failed",
				Type:    &errType,
				Error:   err,
			},
		}
	}

	// 1b. Credit pre-flight (cached 60s).
	//
	// Fast-fail with a retryable status if the key is too low or
	// known-exhausted; Bifrost's retry logic rotates to another key.
	apiURLForCredits := p.networkConfig.BaseURL
	if cerr := auth.ensureCreditsAvailable(accessToken, apiURLForCredits); cerr != nil {
		statusCode := 402
		errType := "insufficient_credits"
		return nil, &schemas.BifrostError{
			IsBifrostError: false,
			StatusCode:     &statusCode,
			Error: &schemas.ErrorField{
				Message: cerr.Error(),
				Type:    &errType,
				Error:   cerr,
			},
		}
	}

	// 1c. Acquire the per-account WS slot — at most ONE WS request
	// in flight per key. See acquireSlot() comment for the why.
	if serr := auth.acquireSlot(ctx); serr != nil {
		statusCode := 503
		errType := "key_busy"
		return nil, &schemas.BifrostError{
			IsBifrostError: false,
			StatusCode:     &statusCode,
			Error: &schemas.ErrorField{
				Message: "moclaw: account busy with another in-flight request",
				Type:    &errType,
				Error:   serr,
			},
		}
	}
	// Slot is released in the stream goroutine's defer (or in early-return
	// paths below).

	// 2. Resolve session thread ID.
	apiURL := p.networkConfig.BaseURL
	threadID, err := auth.getThreadID(accessToken, p.httpClient, apiURL)
	if err != nil {
		// NOTE: was silently falling back to generateThreadID() here.
		//
		// That's a footgun: MoClaw treats unknown threadIds as fresh
		// conversations, and fresh conversations always reply with the
		// onboarding greeting ("Hey! What's up?") regardless of the prompt
		// content. The fallback turned every "session resolve failed" into
		// a silent "all replies are greetings" mystery.
		//
		// Now: return the error. If session resolve is broken, the caller
		// gets a 502 they can debug, not a misleading greeting.
		p.logger.Error("moclaw: session resolve failed: %v", err)
		auth.releaseSlot()
		statusCode := 502
		errType := "session_resolve_error"
		return nil, &schemas.BifrostError{
			IsBifrostError: false,
			StatusCode:     &statusCode,
			Error: &schemas.ErrorField{
				Message: fmt.Sprintf("moclaw: cannot resolve session thread: %v", err),
				Type:    &errType,
				Error:   err,
			},
		}
	}
	if extras := request.GetExtraParams(); extras != nil {
		if v, ok := extras["thread_id"]; ok {
			if s, ok := v.(string); ok && s != "" {
				threadID = s
			}
		}
	}

	// 3. Dial fresh WS connection.
	conn, dialErr := p.dialWS(ctx, accessToken)
	if dialErr != nil {
		if strings.Contains(dialErr.Error(), "401") {
			auth.invalidateAccessToken()
		}
		auth.releaseSlot()
		return nil, providerUtils.NewBifrostOperationError("moclaw: ws connect failed", dialErr)
	}

	// 3b. Wait for auth_result before sending.
	//
	// MoClaw's server pushes an `auth_result` frame right after the WS
	// upgrade. If we WriteJSON the "send" frame before that handshake
	// completes server-side, the server silently drops the frame (no
	// error, no response), and the read loop hangs forever waiting for
	// stream events that never arrive.
	//
	// Browser UI works because it implicitly waits (its onmessage handler
	// processes auth_result before any user-initiated send). We need to
	// emulate that explicitly.
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, authMsg, authErr := conn.ReadMessage()
	if authErr != nil {
		_ = conn.Close()
		auth.releaseSlot()
		if strings.Contains(authErr.Error(), "401") || strings.Contains(authErr.Error(), "unauthorized") {
			auth.invalidateAccessToken()
		}
		return nil, providerUtils.NewBifrostOperationError("moclaw: ws auth handshake read failed", authErr)
	}
	{
		var authRX moclawRX
		if uerr := sonic.Unmarshal(authMsg, &authRX); uerr == nil {
			if authRX.Type == "auth_result" && authRX.Success != nil && !*authRX.Success {
				_ = conn.Close()
				auth.releaseSlot()
				auth.invalidateAccessToken()
				return nil, providerUtils.NewBifrostOperationError("moclaw: ws auth_result success=false", fmt.Errorf("auth rejected by server"))
			}
		}
	}
	_ = conn.SetReadDeadline(time.Time{})

	// 4. Send the real prompt.
	//
	// MoClaw's WS protocol carries one content per frame, so for multi-turn
	// requests we flatten the full OpenAI messages array into a single
	// prompt with [role] headers. Single-turn requests pass through clean
	// (no headers) — see flattenConversation for details.
	content := flattenConversation(request.Input)
	frame := moclawSendFrame{
		Type:        "send",
		ThreadID:    threadID,
		Content:     content,
		Traceparent: generateTraceparent(),
	}

	if err := conn.WriteJSON(frame); err != nil {
		_ = conn.Close()
		auth.releaseSlot()
		return nil, providerUtils.NewBifrostOperationError("moclaw: ws write failed", err)
	}

	// 5. Stream goroutine.
	responseChan := make(chan *schemas.BifrostStreamChunk, schemas.DefaultStreamBufferSize)
	providerUtils.SetStreamIdleTimeoutIfEmpty(ctx, p.networkConfig.StreamIdleTimeoutInSeconds)

	respID := generateChatCompletionID()
	model := request.Model
	startTime := time.Now()

	go func() {
		defer conn.Close()
		// Release the per-key WS slot so the next request on this account
		// can proceed. Runs after stream finishes (query_done or error).
		defer auth.releaseSlot()
		defer providerUtils.EnsureStreamFinalizerCalled(ctx, postHookSpanFinalizer)
		defer func() {
			if ctx.Err() == context.Canceled {
				providerUtils.HandleStreamCancellation(ctx, postHookRunner, responseChan, p.logger, postHookSpanFinalizer, nil)
			} else if ctx.Err() == context.DeadlineExceeded {
				providerUtils.HandleStreamTimeout(ctx, postHookRunner, responseChan, p.logger, postHookSpanFinalizer, nil)
			}
			close(responseChan)
		}()

		chunkIndex := 0
		lastChunkTime := startTime

		// ourThreadID is the thread we created for THIS request (see step 2 above).
		//
		// MoClaw's WS is auth-scoped (per user token), not thread-scoped:
		// once authenticated, the server pushes events for ANY of the user's
		// threads to this connection. With parallel requests (each in its own
		// fresh thread), every WS sees the union of all in-flight responses.
		//
		// To isolate this request's response we drop any event whose
		// threadId doesn't match the thread we just created. Since
		// getThreadID() POSTs a fresh /threads each call, ourThreadID is
		// guaranteed unique and the filter is foolproof — no race-prone
		// first-event guessing.
		ourThreadID := threadID

		// First-queryId capture as a *secondary* filter for the edge case
		// where threadId is missing from an event envelope. Not used unless
		// threadId is empty.
		var ourQueryID string

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
			resp := providerUtils.GetBifrostResponseForStreamResponse(nil, chatResp, nil, nil, nil, nil)
			if postHookRunner != nil {
				providerUtils.ProcessAndSendResponse(ctx, postHookRunner, resp, responseChan, postHookSpanFinalizer)
			} else {
				// Non-streaming caller (ChatCompletion) passes nil postHookRunner.
				// Send directly to channel without hook processing.
				responseChan <- &schemas.BifrostStreamChunk{BifrostChatResponse: chatResp}
			}
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
				if strings.Contains(err.Error(), "Init failed") {
					auth.invalidateSession()
				}
				// Credit-exhaustion detection: server may close the WS with
				// a status code or message hinting at insufficient credits.
				// Best-effort substring match — flag the account exhausted
				// so subsequent requests fast-fail and Bifrost rotates keys.
				low := strings.ToLower(err.Error())
				if strings.Contains(low, "credit") || strings.Contains(low, "insufficient") || strings.Contains(low, "quota") || strings.Contains(low, "402") {
					// Try to fetch period_end so we know when the key recovers.
					if balance, resetAt, ferr := auth.fetchBalance(accessToken, p.networkConfig.BaseURL); ferr == nil && balance <= 0 {
						auth.markCreditsExhausted(resetAt)
					}
				}
				return
			}

			var rx moclawRX
			if err := sonic.Unmarshal(msgBytes, &rx); err != nil {
				p.logger.Warn("moclaw: bad ws frame: %v", err)
				continue
			}

			// Credit-exhaustion also surfaces as a structured error frame
			// rather than a connection close. Inspect type/payload defensively.
			if rx.Type == "error" || strings.Contains(strings.ToLower(string(msgBytes)), "insufficient_credit") {
				if balance, resetAt, ferr := auth.fetchBalance(accessToken, p.networkConfig.BaseURL); ferr == nil && balance <= 0 {
					auth.markCreditsExhausted(resetAt)
				}
				return
			}

			// Auth result already handled in ChatCompletionStream pre-send
			// handshake; skip if received again (server should not re-send,
			// but be defensive).
			if rx.Type == "auth_result" {
				continue
			}

			if rx.Type != "stream" || rx.Event == nil || rx.Event.Event == nil {
				continue
			}
			inner := rx.Event.Event

			// PRIMARY filter: threadId.
			//
			// Drop any event whose threadId doesn't match the thread we
			// created for this request. The auth-scoped WS may carry
			// concurrent users' responses on the same socket, but each
			// request has its own freshly-POSTed thread so this filter is
			// deterministic.
			frameThreadID := rx.Event.ThreadID
			if frameThreadID == "" {
				frameThreadID = inner.ThreadID
			}
			if frameThreadID != "" && frameThreadID != ourThreadID {
				continue
			}

			// SECONDARY filter: queryId.
			//
			// Defensive fallback if the server ever omits threadId on an
			// event (none observed in practice, but cheap insurance). The
			// FIRST event matching our thread locks the queryId; later
			// events with a different queryId are dropped.
			frameQID := rx.Event.QueryID
			if frameQID == "" {
				frameQID = inner.QueryID
			}
			if ourQueryID == "" {
				ourQueryID = frameQID
			} else if frameQID != "" && frameQID != ourQueryID {
				continue
			}

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
		content   strings.Builder
		reasoning strings.Builder
		startedAt = time.Now()
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

// ListModels returns the hardcoded list of MoClaw-served models.
//
// MoClaw doesn't expose a public /v1/models endpoint — the available models
// are advertised on moclaw.ai's landing page and bound per-session via the
// web UI's model picker. We return them statically so Bifrost's model
// catalog has entries to route against; otherwise wildcard ("*") keys leave
// the catalog empty and chat/completions requests hang.
//
// Keep in sync with moclaw.ai's marketing page and the placeholder string
// at ui/lib/constants/config.ts (moclaw line).
func (p *MoClawProvider) ListModels(ctx *schemas.BifrostContext, keys []schemas.Key, request *schemas.BifrostListModelsRequest) (*schemas.BifrostListModelsResponse, *schemas.BifrostError) {
	prefix := string(schemas.MoClaw) + "/"
	names := []string{
		"claude-opus-4-6",
		"claude-sonnet-4-6",
		"deepseek-v4-pro",
	}
	data := make([]schemas.Model, 0, len(names))
	for _, n := range names {
		data = append(data, schemas.Model{ID: prefix + n})
	}
	return &schemas.BifrostListModelsResponse{Data: data}, nil
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
