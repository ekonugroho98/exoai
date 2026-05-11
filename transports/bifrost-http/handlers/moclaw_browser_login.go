package handlers

// Browser-assisted MoClaw login via Chrome DevTools Protocol (CDP).
//
// Flow:
//  1. POST /api/providers/moclaw/browser-login  → launches Chrome (visible) and
//     returns {"session_id": "<id>"}.
//  2. GET  /api/providers/moclaw/browser-login/{session_id}  → SSE stream that
//     pushes status updates and, on success, the extracted tokens.
//
// The user sees a real Chrome window and completes the Google OAuth flow manually.
// Once the Auth0 SPA SDK token appears in localStorage, the backend captures it
// and pushes a "done" event over SSE.
//
// No new Go dependencies — CDP is plain HTTP + WebSocket (fasthttp/websocket is
// already present in this module).

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	ws "github.com/fasthttp/websocket"
	"github.com/maximhq/bifrost/transports/bifrost-http/lib"
	"github.com/valyala/fasthttp"
)

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

// moclawBrowserLoginStart handles POST /api/providers/moclaw/browser-login.
func (h *ProviderHandler) moclawBrowserLoginStart(ctx *fasthttp.RequestCtx) {
	sessionID, sess := newBLSession()
	go runBrowserLogin(sess)
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
// Chrome path resolution
// ----------------------------------------------------------------------------

func chromePath() (string, error) {
	switch runtime.GOOS {
	case "darwin":
		for _, p := range []string{
			"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
			"/Applications/Chromium.app/Contents/MacOS/Chromium",
		} {
			if _, err := os.Stat(p); err == nil {
				return p, nil
			}
		}
	case "linux":
		for _, name := range []string{"google-chrome", "chromium-browser", "chromium", "google-chrome-stable"} {
			if path, err := exec.LookPath(name); err == nil {
				return path, nil
			}
		}
	case "windows":
		for _, p := range []string{
			filepath.Join(os.Getenv("PROGRAMFILES"), "Google", "Chrome", "Application", "chrome.exe"),
			filepath.Join(os.Getenv("PROGRAMFILES(X86)"), "Google", "Chrome", "Application", "chrome.exe"),
		} {
			if _, err := os.Stat(p); err == nil {
				return p, nil
			}
		}
	}
	return "", fmt.Errorf("Chrome/Chromium not found — please install Google Chrome")
}

// ----------------------------------------------------------------------------
// Minimal CDP client (no external deps)
// ----------------------------------------------------------------------------

type cdpMsg struct {
	ID     int             `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

type cdpClient struct {
	conn     *ws.Conn
	mu       sync.Mutex
	seq      int
	inflight map[int]chan cdpMsg
}

func dialCDP(wsURL string) (*cdpClient, error) {
	conn, _, err := ws.DefaultDialer.Dial(wsURL, http.Header{})
	if err != nil {
		return nil, err
	}
	c := &cdpClient{conn: conn, inflight: map[int]chan cdpMsg{}}
	go c.readLoop()
	return c, nil
}

func (c *cdpClient) readLoop() {
	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var resp cdpMsg
		_ = json.Unmarshal(msg, &resp)
		if resp.ID == 0 {
			continue
		}
		c.mu.Lock()
		ch, ok := c.inflight[resp.ID]
		if ok {
			delete(c.inflight, resp.ID)
		}
		c.mu.Unlock()
		if ok {
			ch <- resp
		}
	}
}

func (c *cdpClient) call(method string, params any) (json.RawMessage, error) {
	c.mu.Lock()
	c.seq++
	id := c.seq
	ch := make(chan cdpMsg, 1)
	c.inflight[id] = ch
	c.mu.Unlock()

	var paramsRaw json.RawMessage
	if params != nil {
		b, _ := json.Marshal(params)
		paramsRaw = b
	}
	raw, _ := json.Marshal(cdpMsg{ID: id, Method: method, Params: paramsRaw})
	if err := c.conn.WriteMessage(ws.TextMessage, raw); err != nil {
		return nil, err
	}
	select {
	case resp := <-ch:
		if resp.Error != nil {
			return nil, fmt.Errorf("CDP %s: %s", method, resp.Error.Message)
		}
		return resp.Result, nil
	case <-time.After(15 * time.Second):
		return nil, fmt.Errorf("CDP %s: timeout", method)
	}
}

func (c *cdpClient) evalJS(expr string) (json.RawMessage, error) {
	return c.call("Runtime.evaluate", map[string]any{
		"expression":    expr,
		"returnByValue": true,
		"awaitPromise":  true,
		"userGesture":   true,
	})
}

// ----------------------------------------------------------------------------
// Browser login goroutine
// ----------------------------------------------------------------------------

func runBrowserLogin(sess *browserLoginSession) {
	pub := func(status, msg string) {
		sess.publish(browserLoginEvent{Status: status, Message: msg})
	}
	fail := func(msg string) {
		sess.publish(browserLoginEvent{Status: "error", Error: msg})
	}

	// 1. Find Chrome
	bin, err := chromePath()
	if err != nil {
		fail(err.Error())
		return
	}

	// 2. Isolated profile dir (prevents mixing with user's existing Chrome session)
	profileDir, err := os.MkdirTemp("", "moclaw-chrome-*")
	if err != nil {
		fail("cannot create temp profile: " + err.Error())
		return
	}
	defer os.RemoveAll(profileDir)

	// 3. Random debugging port (9300-9399)
	debugPort := 9300 + rand.Intn(100)

	// 4. Launch Chrome (non-headless, visible to user)
	cmd := exec.Command(bin,
		fmt.Sprintf("--remote-debugging-port=%d", debugPort),
		fmt.Sprintf("--user-data-dir=%s", profileDir),
		"--no-first-run",
		"--no-default-browser-check",
		"--disable-popup-blocking",
		"--disable-extensions",
		"--disable-sync",
		"--disable-background-networking",
		"--app=https://moclaw.ai/",
		"--window-size=1024,768",
	)
	if err := cmd.Start(); err != nil {
		fail("failed to launch Chrome: " + err.Error())
		return
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}()

	pub("launching", "Opening Chrome window...")

	// 5. Wait for CDP to be ready (poll /json/version for up to 20s)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var wsDebuggerURL string
	for {
		select {
		case <-ctx.Done():
			fail("Chrome did not respond in time — make sure Google Chrome is installed")
			return
		case <-time.After(500 * time.Millisecond):
		}
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/json/version", debugPort))
		if err != nil {
			continue
		}
		var info struct {
			WebSocketDebuggerURL string `json:"webSocketDebuggerUrl"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&info)
		_ = resp.Body.Close()
		if info.WebSocketDebuggerURL != "" {
			wsDebuggerURL = info.WebSocketDebuggerURL
			break
		}
	}

	pub("connecting", "Connected to browser")

	// 6. Connect to CDP
	cdp, err := dialCDP(wsDebuggerURL)
	if err != nil {
		fail("CDP connect failed: " + err.Error())
		return
	}
	defer cdp.conn.Close()

	if _, err := cdp.call("Runtime.enable", nil); err != nil {
		fail("CDP Runtime.enable: " + err.Error())
		return
	}

	// 7. Wait for page then click "Get Started"
	time.Sleep(2500 * time.Millisecond)
	pub("navigating", "Clicking Get Started...")
	_, _ = cdp.evalJS(`
		(function() {
			var el = Array.from(document.querySelectorAll('a,button'))
				.find(e => /get\s*started/i.test(e.innerText || e.textContent));
			if (el) el.click();
		})();
	`)

	time.Sleep(1000 * time.Millisecond)
	pub("waiting", "Please log in with Google in the browser window (you have 5 minutes)...")

	// 8. Poll localStorage for Auth0 SPA token — up to 5 minutes
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)

		res, err := cdp.evalJS(`
			(function() {
				try {
					for (var i = 0; i < localStorage.length; i++) {
						var k = localStorage.key(i);
						if (k && k.indexOf('@@auth0spajs@@') !== -1) {
							var v = JSON.parse(localStorage.getItem(k));
							var b = v && v.body;
							if (b && b.access_token) {
								return JSON.stringify({
									access_token: b.access_token,
									refresh_token: b.refresh_token || '',
									expires_in: b.expires_in || 86400
								});
							}
						}
					}
				} catch(e) {}
				return null;
			})();
		`)
		if err != nil {
			continue
		}

		// CDP returns {"result":{"type":"string","value":"...json..."}}
		// or {"result":{"type":"null"}}
		var wrapper struct {
			Result struct {
				Value *string `json:"value"`
			} `json:"result"`
		}
		if err := json.Unmarshal(res, &wrapper); err != nil || wrapper.Result.Value == nil {
			continue
		}
		inner := *wrapper.Result.Value
		if inner == "" || inner == "null" {
			continue
		}

		var tokens struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			ExpiresIn    int    `json:"expires_in"`
		}
		if err := json.Unmarshal([]byte(inner), &tokens); err != nil || tokens.AccessToken == "" {
			continue
		}

		sess.publish(browserLoginEvent{
			Status:       "done",
			Message:      "Login successful",
			AccessToken:  tokens.AccessToken,
			RefreshToken: tokens.RefreshToken,
			ExpiresIn:    tokens.ExpiresIn,
		})
		return
	}

	fail("Login timed out after 5 minutes. Please try again.")
}
