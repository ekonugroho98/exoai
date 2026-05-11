package handlers

// Browser-assisted MoClaw login via Python + Camoufox / Playwright.
//
// Flow:
//  1. POST /api/providers/moclaw/browser-login  → runs moclaw_login.py with the
//     supplied email/password and returns {"session_id": "<id>"}.
//  2. GET  /api/providers/moclaw/browser-login/{session_id}  → SSE stream that
//     pushes status updates and, on success, the extracted tokens.
//
// moclaw_login.py tries camoufox first (anti-detect Firefox), then falls back to
// playwright/chromium. It prints PROGRESS / DONE / ERROR lines to stdout which
// this handler translates into SSE events.

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/bytedance/sonic"
	"github.com/maximhq/bifrost/transports/bifrost-http/lib"
	"github.com/valyala/fasthttp"
)

//go:embed moclaw_login.py
var moclawLoginPy string

// ----------------------------------------------------------------------------
// In-memory session store
// ----------------------------------------------------------------------------

type browserLoginEvent struct {
	Status       string `json:"status"`
	Message      string `json:"message,omitempty"`
	AccessToken  string `json:"access_token,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	ExpiresIn    int    `json:"expires_in,omitempty"`
	Error        string `json:"error,omitempty"`
}

type browserLoginSession struct {
	mu      sync.Mutex
	done    bool
	lastEvt *browserLoginEvent
	subs    []chan browserLoginEvent
}

var (
	blSessions   = map[string]*browserLoginSession{}
	blSessionsMu sync.Mutex
)

func newBLSession() (string, *browserLoginSession) {
	id := fmt.Sprintf("%016x", rand.Int63())
	sess := &browserLoginSession{}
	blSessionsMu.Lock()
	blSessions[id] = sess
	blSessionsMu.Unlock()
	return id, sess
}

func getBLSession(id string) (*browserLoginSession, bool) {
	blSessionsMu.Lock()
	defer blSessionsMu.Unlock()
	s, ok := blSessions[id]
	return s, ok
}

func (s *browserLoginSession) publish(evt browserLoginEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastEvt = &evt
	if evt.Status == "done" || evt.Status == "error" {
		s.done = true
	}
	for _, ch := range s.subs {
		select {
		case ch <- evt:
		default:
		}
	}
}

func (s *browserLoginSession) subscribe() chan browserLoginEvent {
	ch := make(chan browserLoginEvent, 32)
	s.mu.Lock()
	s.subs = append(s.subs, ch)
	if s.done && s.lastEvt != nil {
		ch <- *s.lastEvt // replay terminal event to late subscribers
	}
	s.mu.Unlock()
	return ch
}

// ----------------------------------------------------------------------------
// HTTP handlers
// ----------------------------------------------------------------------------

type browserLoginStartRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Headless bool   `json:"headless"`
}

// moclawBrowserLoginStart handles POST /api/providers/moclaw/browser-login.
func (h *ProviderHandler) moclawBrowserLoginStart(ctx *fasthttp.RequestCtx) {
	var req browserLoginStartRequest
	if err := sonic.Unmarshal(ctx.PostBody(), &req); err != nil {
		SendError(ctx, fasthttp.StatusBadRequest, "invalid JSON")
		return
	}
	if req.Email == "" || req.Password == "" {
		SendError(ctx, fasthttp.StatusBadRequest, "email and password are required")
		return
	}

	sessionID, sess := newBLSession()
	go runBrowserLogin(sess, req.Email, req.Password, req.Headless)
	SendJSON(ctx, map[string]string{"session_id": sessionID})
}

// moclawBrowserLoginStatus handles GET /api/providers/moclaw/browser-login/{session_id}.
// It streams SSE events until login completes or fails.
func (h *ProviderHandler) moclawBrowserLoginStatus(ctx *fasthttp.RequestCtx) {
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

// ----------------------------------------------------------------------------
// Python runner
// ----------------------------------------------------------------------------

func findPython() (string, error) {
	for _, name := range []string{"python3", "python"} {
		path, err := exec.LookPath(name)
		if err != nil {
			continue
		}
		out, _ := exec.Command(path, "--version").Output()
		if strings.HasPrefix(string(out), "Python 3") {
			return path, nil
		}
	}
	return "", fmt.Errorf("Python 3 not found — install it with: brew install python3")
}

func runBrowserLogin(sess *browserLoginSession, email, password string, headless bool) {
	pub := func(status, msg string) {
		sess.publish(browserLoginEvent{Status: status, Message: msg})
	}
	fail := func(msg string) {
		sess.publish(browserLoginEvent{Status: "error", Error: msg})
	}

	// Locate Python 3
	pythonBin, err := findPython()
	if err != nil {
		fail(err.Error())
		return
	}

	// Write the embedded Python script to a temp file
	f, err := os.CreateTemp("", "moclaw-login-*.py")
	if err != nil {
		fail("cannot create temp script: " + err.Error())
		return
	}
	scriptPath := f.Name()
	defer os.Remove(scriptPath)
	if _, err := f.WriteString(moclawLoginPy); err != nil {
		f.Close()
		fail("cannot write script: " + err.Error())
		return
	}
	f.Close()

	headlessStr := "false"
	if headless {
		headlessStr = "true"
	}

	pub("launching", "Starting browser automation...")

	cmd := exec.Command(pythonBin, scriptPath, email, password, headlessStr)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fail("pipe error: " + err.Error())
		return
	}
	if err := cmd.Start(); err != nil {
		fail("cannot start Python: " + err.Error())
		return
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "PROGRESS:"):
			pub("running", strings.TrimPrefix(line, "PROGRESS:"))

		case strings.HasPrefix(line, "DONE:"):
			jsonStr := strings.TrimPrefix(line, "DONE:")
			var tokens struct {
				AccessToken  string `json:"access_token"`
				RefreshToken string `json:"refresh_token"`
				ExpiresIn    int    `json:"expires_in"`
			}
			if err := json.Unmarshal([]byte(jsonStr), &tokens); err != nil || tokens.AccessToken == "" {
				fail("failed to parse tokens from script output")
				return
			}
			sess.publish(browserLoginEvent{
				Status:       "done",
				Message:      "Login successful",
				AccessToken:  tokens.AccessToken,
				RefreshToken: tokens.RefreshToken,
				ExpiresIn:    tokens.ExpiresIn,
			})
			return

		case strings.HasPrefix(line, "ERROR:"):
			fail(strings.TrimPrefix(line, "ERROR:"))
			return
		}
	}

	fail("Login script exited without completing")
}
