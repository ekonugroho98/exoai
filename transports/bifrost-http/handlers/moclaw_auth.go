package handlers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/valyala/fasthttp"
)

// moclawCachedRefreshToken returns the most-recently-rotated refresh token for
// the given keyID from the on-disk cache written by the MoClaw provider, or ""
// if the file doesn't exist / can't be read.
func moclawCachedRefreshToken(keyID string) string {
	if keyID == "" {
		return ""
	}
	dir := os.Getenv("BIFROST_DATA_DIR")
	if dir == "" {
		if home, err := os.UserHomeDir(); err == nil {
			dir = filepath.Join(home, ".bifrost")
		}
	}
	if dir == "" {
		return ""
	}
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, keyID)
	if safe == "" {
		return ""
	}
	b, err := os.ReadFile(filepath.Join(dir, "moclaw_refresh_"+safe+".txt"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// moclawSaveRefreshToken persists a rotated refresh_token to disk so
// subsequent exchanges use the latest token (the old one is dead after rotation).
func moclawSaveRefreshToken(keyID, refreshToken string) {
	if keyID == "" || refreshToken == "" {
		return
	}
	dir := os.Getenv("BIFROST_DATA_DIR")
	if dir == "" {
		if home, err := os.UserHomeDir(); err == nil {
			dir = filepath.Join(home, ".bifrost")
		}
	}
	if dir == "" {
		return
	}
	_ = os.MkdirAll(dir, 0700)
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, keyID)
	if safe == "" {
		return
	}
	_ = os.WriteFile(filepath.Join(dir, "moclaw_refresh_"+safe+".txt"), []byte(refreshToken), 0600)
}

// MoClawLoginRequest is the body for the auto-login endpoint.
type MoClawLoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// MoClawLoginResponse is returned after a successful Auth0 ROPC exchange.
type MoClawLoginResponse struct {
	Email        string `json:"email"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

const (
	moclawAuth0Domain   = "https://auth.moclaw.ai"
	moclawAuth0ClientID = "R7QyN3rYIv2DSEqkgQJjfSvvb6XFxMOu"
)

// moclawAuth0TokenResp mirrors the Auth0 /oauth/token response.
type moclawAuth0TokenResp struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Error        string `json:"error,omitempty"`
	ErrorDesc    string `json:"error_description,omitempty"`
}

// moclawAccessCachePath returns the path for the access-token cache file.
func moclawAccessCachePath(keyID string) string {
	dir := os.Getenv("BIFROST_DATA_DIR")
	if dir == "" {
		if home, err := os.UserHomeDir(); err == nil {
			dir = filepath.Join(home, ".bifrost")
		}
	}
	if dir == "" {
		return ""
	}
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, keyID)
	if safe == "" {
		return ""
	}
	return filepath.Join(dir, "moclaw_access_"+safe+".txt")
}

// jwtExp decodes the `exp` claim from a JWT without verifying the signature.
func jwtExp(token string) int64 {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) < 2 {
		return 0
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return 0
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return 0
	}
	return claims.Exp
}

// moclawCachedAccessToken returns a cached access_token that is still valid
// (with a 60-second safety margin), or "" if none found.
func moclawCachedAccessToken(keyID string) string {
	path := moclawAccessCachePath(keyID)
	if path == "" {
		return ""
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	lines := strings.SplitN(strings.TrimSpace(string(b)), "\n", 2)
	if len(lines) < 2 {
		return ""
	}
	token := strings.TrimSpace(lines[0])
	expStr := strings.TrimSpace(lines[1])
	expUnix, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil {
		return ""
	}
	// Use a 60-second safety margin before expiry.
	if time.Now().Unix() >= expUnix-60 {
		return ""
	}
	return token
}

// moclawSaveAccessToken persists an access_token + its JWT expiry to disk.
func moclawSaveAccessToken(keyID, accessToken string) {
	path := moclawAccessCachePath(keyID)
	if path == "" {
		return
	}
	expUnix := jwtExp(accessToken)
	if expUnix == 0 {
		// Unknown expiry — store with a 23h TTL as fallback.
		expUnix = time.Now().Add(23 * time.Hour).Unix()
	}
	content := accessToken + "\n" + strconv.FormatInt(expUnix, 10)
	_ = os.WriteFile(path, []byte(content), 0600)
}

// moclawExchangeRefreshToken exchanges a refresh_token for an access_token via
// Auth0. Returns (access_token, new_refresh_token, error).
// IMPORTANT: Auth0 rotates the refresh_token on every exchange — the caller
// MUST persist the returned refresh_token; the input one is now dead.
func moclawExchangeRefreshToken(refreshToken string) (string, string, error) {
	body, _ := sonic.Marshal(map[string]string{
		"grant_type":    "refresh_token",
		"client_id":     moclawAuth0ClientID,
		"refresh_token": refreshToken,
	})
	ac := &fasthttp.Client{ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second}
	rq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(rq)
	rp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(rp)
	rq.SetRequestURI(moclawAuth0Domain + "/oauth/token")
	rq.Header.SetMethod(http.MethodPost)
	rq.Header.SetContentType("application/json")
	rq.Header.Set("Accept", "application/json")
	rq.SetBody(body)
	if err := ac.Do(rq, rp); err != nil {
		return "", "", fmt.Errorf("auth0 request failed: %w", err)
	}
	var tok moclawAuth0TokenResp
	if err := sonic.Unmarshal(rp.Body(), &tok); err != nil || tok.AccessToken == "" {
		return "", "", fmt.Errorf("token exchange failed")
	}
	return tok.AccessToken, tok.RefreshToken, nil
}

// moclawLogin handles POST /api/providers/moclaw/login.
// It calls Auth0's Resource Owner Password Credentials grant with the supplied
// email/password and returns the resulting access_token and refresh_token.
func (h *ProviderHandler) moclawLogin(ctx *fasthttp.RequestCtx) {
	var req MoClawLoginRequest
	if err := sonic.Unmarshal(ctx.PostBody(), &req); err != nil {
		SendError(ctx, fasthttp.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}
	if req.Email == "" || req.Password == "" {
		SendError(ctx, fasthttp.StatusBadRequest, "email and password are required")
		return
	}

	body, _ := sonic.Marshal(map[string]string{
		"grant_type": "password",
		"client_id":  moclawAuth0ClientID,
		"username":   req.Email,
		"password":   req.Password,
		"scope":      "openid profile email offline_access",
		"audience":   "https://realtime.moclaw.ai",
	})

	httpClient := &fasthttp.Client{
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	freq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(freq)
	fresp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(fresp)

	freq.SetRequestURI(moclawAuth0Domain + "/oauth/token")
	freq.Header.SetMethod(http.MethodPost)
	freq.Header.SetContentType("application/json")
	freq.Header.Set("Accept", "application/json")
	freq.SetBody(body)

	if err := httpClient.Do(freq, fresp); err != nil {
		SendError(ctx, fasthttp.StatusBadGateway, fmt.Sprintf("auth0 request failed: %v", err))
		return
	}

	var tok moclawAuth0TokenResp
	if err := sonic.Unmarshal(fresp.Body(), &tok); err != nil {
		SendError(ctx, fasthttp.StatusBadGateway, fmt.Sprintf("auth0 response decode failed: %v", err))
		return
	}

	if fresp.StatusCode() != fasthttp.StatusOK || tok.AccessToken == "" {
		msg := tok.ErrorDesc
		if msg == "" {
			msg = tok.Error
		}
		if msg == "" {
			msg = fmt.Sprintf("auth0 returned status %d", fresp.StatusCode())
		}
		SendError(ctx, fasthttp.StatusUnauthorized, fmt.Sprintf("login failed: %s", msg))
		return
	}

	SendJSON(ctx, MoClawLoginResponse{
		Email:        req.Email,
		AccessToken:  tok.AccessToken,
		RefreshToken: tok.RefreshToken,
		ExpiresIn:    tok.ExpiresIn,
	})
}

// moclawCheckBalance handles POST /api/providers/moclaw/check-balance.
// Accepts a refresh_token (v1.*) or JWT access_token and returns the account's
// credit balance from the moclaw API.
func (h *ProviderHandler) moclawCheckBalance(ctx *fasthttp.RequestCtx) {
	var req struct {
		Token string `json:"token"`
		KeyID string `json:"key_id"`
	}
	if err := sonic.Unmarshal(ctx.PostBody(), &req); err != nil || req.Token == "" {
		SendError(ctx, fasthttp.StatusBadRequest, "token required")
		return
	}

	accessToken := req.Token

	// 0. If a key_id was provided, always resolve the real token from the
	//    store — the keys API returns redacted/masked values to the UI, so
	//    req.Token may be garbage (e.g. "eyJh****09NQ").
	if req.KeyID != "" {
		if rawKey, err := h.inMemoryStore.GetProviderKeyRaw("moclaw", req.KeyID); err == nil && rawKey != nil {
			accessToken = rawKey.Value.Val
		}
	}

	// 1. If a key_id was provided, try the cached access_token first — this
	//    avoids exchanging the refresh_token on every call (which would rotate
	//    it and quickly make the stored value stale).
	if req.KeyID != "" {
		if at := moclawCachedAccessToken(req.KeyID); at != "" {
			accessToken = at
		}
	}

	// 2. If we still have a refresh_token (not a JWT), exchange it for an
	//    access_token. Prefer the latest rotated token from the provider's
	//    on-disk cache over the (possibly stale) value stored in the config DB.
	if !strings.HasPrefix(accessToken, "eyJ") {
		refreshToken := accessToken
		if req.KeyID != "" {
			if cached := moclawCachedRefreshToken(req.KeyID); cached != "" {
				refreshToken = cached
			}
		}
		at, newRT, err := moclawExchangeRefreshToken(refreshToken)
		if err != nil {
			SendError(ctx, fasthttp.StatusBadRequest, "failed to exchange refresh token (likely stale or rotated; re-extract from moclaw.ai DevTools)")
			return
		}
		accessToken = at
		if req.KeyID != "" {
			// Persist the fresh access_token so subsequent calls skip the refresh.
			moclawSaveAccessToken(req.KeyID, accessToken)
			// Persist the rotated refresh_token — the old one is now dead.
			if newRT != "" {
				moclawSaveRefreshToken(req.KeyID, newRT)
			}
		}
	}

	// Call moclaw credits API
	ac := &fasthttp.Client{ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	rq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(rq)
	rp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(rp)
	rq.SetRequestURI("https://api.moclaw.ai/api/credits/balance")
	rq.Header.SetMethod(http.MethodGet)
	rq.Header.Set("Authorization", "Bearer "+accessToken)
	rq.Header.Set("Content-Type", "application/json")
	rq.Header.Set("Origin", "https://moclaw.ai")
	if err := ac.Do(rq, rp); err != nil {
		SendError(ctx, fasthttp.StatusBadGateway, "balance check failed: "+err.Error())
		return
	}
	if rp.StatusCode() != fasthttp.StatusOK {
		// Map upstream 401/403 to 400 so the UI baseApi doesn't treat it as
		// a session-expired event. Other status codes pass through.
		outStatus := rp.StatusCode()
		if outStatus == fasthttp.StatusUnauthorized || outStatus == fasthttp.StatusForbidden {
			outStatus = fasthttp.StatusBadRequest
		}
		SendError(ctx, outStatus, fmt.Sprintf("moclaw API error %d", rp.StatusCode()))
		return
	}
	ctx.SetContentType("application/json")
	ctx.Response.SetBody(append([]byte{}, rp.Body()...))
}
