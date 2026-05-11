package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/bytedance/sonic"
	"github.com/valyala/fasthttp"
)

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
