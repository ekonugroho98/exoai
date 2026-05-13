package handlers

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/maximhq/bifrost/transports/bifrost-http/lib"
	"github.com/valyala/fasthttp"
)

const (
	codexOAuthClientID    = "app_EMoamEEZ73f0CkXaXp7hrann"
	codexOAuthRedirectURI = "http://localhost:1455/auth/callback"
	codexOAuthAuthorize   = "https://auth.openai.com/oauth/authorize"
	codexOAuthTokenURL    = "https://auth.openai.com/oauth/token"
)

type codexOAuthSession struct {
	sess         *browserLoginSession
	codeVerifier string
}

var (
	codexOAuthMu       sync.Mutex
	codexOAuthSessions = map[string]*codexOAuthSession{}
	codexCallbackMu    sync.Mutex
	codexCallbackReady bool
	codexCallbackErr   error
)

type codexBrowserLoginStartResponse struct {
	SessionID string `json:"session_id"`
	AuthURL   string `json:"auth_url"`
}

type codexTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	AccountID    string `json:"account_id"`
}

func (h *ProviderHandler) codexBrowserLoginStart(ctx *fasthttp.RequestCtx) {
	if err := ensureCodexCallbackServer(); err != nil {
		SendError(ctx, fasthttp.StatusBadRequest, err.Error())
		return
	}

	sessionID, sess := newBLSession()
	state := randomURLToken(32)
	verifier := randomURLToken(64)
	challenge := codexCodeChallenge(verifier)

	codexOAuthMu.Lock()
	codexOAuthSessions[state] = &codexOAuthSession{sess: sess, codeVerifier: verifier}
	codexOAuthMu.Unlock()

	values := url.Values{}
	values.Set("client_id", codexOAuthClientID)
	values.Set("code_challenge", challenge)
	values.Set("code_challenge_method", "S256")
	values.Set("codex_cli_simplified_flow", "true")
	values.Set("id_token_add_organizations", "true")
	values.Set("originator", "codex_cli_rs")
	values.Set("redirect_uri", codexOAuthRedirectURI)
	values.Set("response_type", "code")
	values.Set("scope", "openid profile email offline_access")
	values.Set("state", state)

	authURL := codexOAuthAuthorize + "?" + values.Encode()
	sess.publish(browserLoginEvent{Status: "running", Message: "Waiting for login..."})
	SendJSON(ctx, codexBrowserLoginStartResponse{SessionID: sessionID, AuthURL: authURL})
}

func (h *ProviderHandler) codexBrowserLoginStatus(ctx *fasthttp.RequestCtx) {
	sessionID, _ := ctx.UserValue("session_id").(string)
	sess, ok := getBLSession(sessionID)
	if !ok {
		SendError(ctx, fasthttp.StatusNotFound, "browser login session not found")
		return
	}

	ctx.SetContentType("text/event-stream")
	ctx.Response.Header.Set("Cache-Control", "no-cache")
	ctx.Response.Header.Set("Connection", "keep-alive")
	ctx.Response.Header.Set("X-Accel-Buffering", "no")

	reader := lib.NewSSEStreamReader()
	ctx.Response.SetBodyStream(reader, -1)

	ch := sess.subscribe()
	go func() {
		defer reader.Done()
		for evt := range ch {
			data, _ := sonic.Marshal(evt)
			if !reader.SendEvent("status", data) {
				return
			}
			if evt.Status == "done" || evt.Status == "error" {
				return
			}
		}
	}()
}

func ensureCodexCallbackServer() error {
	codexCallbackMu.Lock()
	defer codexCallbackMu.Unlock()
	if codexCallbackReady {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/auth/callback", codexOAuthCallback)
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	ln, err := net.Listen("tcp", "127.0.0.1:1455")
	if err != nil {
		codexCallbackErr = fmt.Errorf("port 1455 is already in use; close the other Codex login callback service first, then try again")
		return codexCallbackErr
	}
	codexCallbackReady = true
	codexCallbackErr = nil
	go func() {
		_ = server.Serve(ln)
	}()
	return codexCallbackErr
}

func codexOAuthCallback(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	code := r.URL.Query().Get("code")
	oauthErr := r.URL.Query().Get("error")

	codexOAuthMu.Lock()
	pending := codexOAuthSessions[state]
	delete(codexOAuthSessions, state)
	codexOAuthMu.Unlock()

	if pending == nil {
		http.Error(w, "Unknown or expired Codex login session.", http.StatusBadRequest)
		return
	}
	if oauthErr != "" {
		pending.sess.publish(browserLoginEvent{Status: "error", Error: oauthErr})
		http.Error(w, "Codex login failed: "+oauthErr, http.StatusBadRequest)
		return
	}
	if code == "" {
		pending.sess.publish(browserLoginEvent{Status: "error", Error: "OAuth callback missing code"})
		http.Error(w, "OAuth callback missing code.", http.StatusBadRequest)
		return
	}

	pending.sess.publish(browserLoginEvent{Status: "running", Message: "Exchanging authorization code..."})
	tokens, err := exchangeCodexCode(code, pending.codeVerifier)
	if err != nil {
		pending.sess.publish(browserLoginEvent{Status: "error", Error: err.Error()})
		http.Error(w, "Token exchange failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	accountName := codexAccountName(tokens)
	saveCodexSession(accountName, tokens)
	pending.sess.publish(browserLoginEvent{
		Status:       "done",
		Message:      "Login successful",
		AccessToken:  tokens.AccessToken,
		RefreshToken: tokens.RefreshToken,
		ExpiresIn:    tokens.ExpiresIn,
	})

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte("<!doctype html><title>Codex login complete</title><body style=\"font-family: system-ui; padding: 32px; background: #0d0d0f; color: #f4f4f5\"><h2>Codex login complete</h2><p>You can close this tab and return to Bifrost.</p></body>"))
}

func exchangeCodexCode(code, verifier string) (*codexTokenResponse, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("client_id", codexOAuthClientID)
	form.Set("code", code)
	form.Set("code_verifier", verifier)
	form.Set("redirect_uri", codexOAuthRedirectURI)

	client := &fasthttp.Client{ReadTimeout: 20 * time.Second, WriteTimeout: 20 * time.Second}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(fasthttp.MethodPost)
	req.SetRequestURI(codexOAuthTokenURL)
	req.Header.SetContentType("application/x-www-form-urlencoded")
	req.SetBodyString(form.Encode())

	if err := client.Do(req, resp); err != nil {
		return nil, err
	}
	if resp.StatusCode() < 200 || resp.StatusCode() >= 300 {
		return nil, fmt.Errorf("OpenAI auth token endpoint returned HTTP %d: %s", resp.StatusCode(), string(resp.Body()))
	}

	var tokens codexTokenResponse
	if err := sonic.Unmarshal(resp.Body(), &tokens); err != nil {
		return nil, err
	}
	if tokens.AccessToken == "" {
		return nil, fmt.Errorf("OpenAI auth response did not include access_token")
	}
	return &tokens, nil
}

func codexAccountName(tokens *codexTokenResponse) string {
	for _, field := range []string{"email", "https://api.openai.com/profile.email", "sub"} {
		if value := jwtStringClaim(tokens.IDToken, field); value != "" {
			return value
		}
	}
	if tokens.AccountID != "" {
		return tokens.AccountID
	}
	return "codex-account"
}

func jwtStringClaim(token, claim string) string {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	if value, ok := claims[claim].(string); ok {
		return value
	}
	return ""
}

func saveCodexSession(accountName string, tokens *codexTokenResponse) {
	dir := os.Getenv("BIFROST_DATA_DIR")
	if dir == "" {
		if home, err := os.UserHomeDir(); err == nil {
			dir = filepath.Join(home, ".bifrost")
		}
	}
	if dir == "" {
		return
	}
	dir = filepath.Join(dir, "codex_sessions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return
	}
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			return r
		}
		return '_'
	}, accountName)
	if safe == "" {
		safe = "codex-account"
	}
	payload, err := sonic.MarshalIndent(map[string]any{
		"account": accountName,
		"tokens":  tokens,
	}, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(dir, safe+".json"), payload, 0600)
}

func codexCodeChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func randomURLToken(bytesLen int) string {
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}
