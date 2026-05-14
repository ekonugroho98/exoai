#!/usr/bin/env python3
"""
MoClaw browser-pool worker — drives the real moclaw.ai chat UI.

Same architecture as zai_browser_worker.py: Camoufox keeps a logged-in
session, each /chat request types into the textarea, presses Enter, and
streams the response back via SSE.

Endpoints
---------
  POST /chat   — send prompt, stream SSE response back
  GET  /status — JSON: {ready, logged_in, busy, url, profile}
  POST /reset  — close & relaunch browser

Usage
-----
  python3 moclaw_browser_worker.py \
      --port 9002 \
      --profile ~/.bifrost/moclaw-profiles/default \
      --session ~/.bifrost/moclaw_sessions/qmwy1@gesoel.com.json \
      [--headless] [--no-geoip]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

try:
    from aiohttp import web
except ImportError:
    sys.stderr.write("FATAL: aiohttp not installed. Install: pip install aiohttp\n")
    sys.exit(1)

try:
    from camoufox.async_api import AsyncCamoufox
except ImportError:
    sys.stderr.write(
        "FATAL: camoufox not installed.\n"
        "Install: pip install 'camoufox[geoip]' && python -m camoufox fetch\n"
    )
    sys.exit(1)

log = logging.getLogger("moclaw-worker")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

# ─── selectors ────────────────────────────────────────────────────────────────
SELECTOR_INPUT = 'textarea[placeholder*="Tell MoClaw"], textarea[placeholder*="Send a message"], textarea[placeholder*="message"], #chat-input, [contenteditable="true"]'
SELECTOR_NEW_CHAT = 'button:has-text("New Chat"), a:has-text("New Chat"), [aria-label="New Chat"]'
SELECTOR_SEND_BTN = 'button[type="submit"], button[aria-label="Send"], button:has(svg):near(textarea)'

# Response detection: watch for assistant message elements
SELECTOR_ASSISTANT_MSG = '[data-role="assistant"], [class*="assistant"], [class*="response"]'

# ─── login detection ──────────────────────────────────────────────────────────
LOGIN_CHECK_JS = r"""
() => {
    // Check for chat input (most reliable)
    if (document.querySelector('textarea[placeholder*="message"]')) return true;
    if (document.querySelector('#chat-input')) return true;
    if (document.querySelector('[contenteditable="true"]')) return true;
    // Check Auth0 tokens in localStorage
    try {
        for (var i = 0; i < localStorage.length; i++) {
            var k = localStorage.key(i);
            if (k && k.indexOf('auth0') !== -1) {
                var v = JSON.parse(localStorage.getItem(k));
                if (v && v.body && v.body.access_token) return true;
            }
        }
    } catch (e) {}
    return false;
}
"""

# ─── WS interceptor JS ───────────────────────────────────────────────────────
# Monkey-patches WebSocket to capture incoming messages from realtime.moclaw.ai
WS_INTERCEPTOR_JS = r"""
() => {
    if (window.__moclawWsHooked) return;
    window.__moclawWsHooked = true;
    window.__moclawResponses = [];
    window.__moclawQueryDone = false;

    const origWS = window.WebSocket;
    window.WebSocket = function(...args) {
        const ws = new origWS(...args);
        const url = args[0] || '';

        if (url.includes('realtime.moclaw.ai')) {
            const origOnMsg = null;
            ws.addEventListener('message', function(evt) {
                try {
                    const data = JSON.parse(evt.data);
                    if (data.type === 'stream' && data.event && data.event.event) {
                        const inner = data.event.event;
                        window.__moclawResponses.push(inner);
                        if (inner.type === 'query_done') {
                            window.__moclawQueryDone = true;
                        }
                    }
                } catch(e) {}
            });
        }
        return ws;
    };
    window.WebSocket.prototype = origWS.prototype;
    Object.assign(window.WebSocket, origWS);
}
"""

RESET_INTERCEPTOR_JS = r"""
() => {
    window.__moclawResponses = [];
    window.__moclawQueryDone = false;
}
"""

POLL_RESPONSES_JS = r"""
() => {
    const events = window.__moclawResponses || [];
    const done = window.__moclawQueryDone || false;
    // Drain: return and clear
    window.__moclawResponses = [];
    return {events: events, done: done};
}
"""


# ─── message compaction ──────────────────────────────────────────────────────
def compact_messages(messages: list[dict]) -> str:
    if not messages:
        return ""
    if len(messages) == 1:
        c = messages[0].get("content", "")
        return c if isinstance(c, str) else str(c)

    last = messages[-1]
    history = messages[:-1]
    parts: list[str] = []

    system = next((m for m in history if m.get("role") == "system"), None)
    if system:
        sys_text = system.get("content", "").strip()
        if sys_text:
            parts.append(f"[System instructions]\n{sys_text}\n")
        history = [m for m in history if m is not system]

    if history:
        parts.append("[Conversation so far]")
        for m in history:
            role = (m.get("role") or "user").capitalize()
            c = m.get("content", "")
            parts.append(f"{role}: {c}")
        parts.append("")

    c = last.get("content", "")
    parts.append(c if isinstance(c, str) else str(c))
    return "\n".join(parts)


# ─── BrowserManager ──────────────────────────────────────────────────────────
class BrowserManager:
    def __init__(self, profile: Path, headless, geoip: bool, session_file: str | None):
        self.profile = profile
        self.headless = headless
        self.geoip = geoip
        self.session_file = session_file

        self._cam = None
        self._browser = None
        self._page = None
        self._ready = False
        self._logged_in = False
        self._ws_hooked = False

        self.lock = asyncio.Lock()

    async def start(self):
        self.profile.mkdir(parents=True, exist_ok=True)
        log.info("Launching Camoufox (profile=%s, headless=%s)", self.profile, self.headless)

        self._cam = AsyncCamoufox(
            persistent_context=True,
            user_data_dir=str(self.profile),
            headless=self.headless,
            geoip=self.geoip,
        )
        self._browser = await self._cam.__aenter__()

        # Import session (cookies + localStorage) if provided
        if self.session_file:
            await self._import_session(self.session_file)

        pages = getattr(self._browser, "pages", []) or []
        self._page = pages[0] if pages else await self._browser.new_page()

        # Install WS interceptor before navigating
        await self._page.add_init_script(WS_INTERCEPTOR_JS)

        log.info("Navigating to moclaw.ai ...")
        try:
            await self._page.goto("https://moclaw.ai/chat", timeout=30000, wait_until="domcontentloaded")
        except Exception as e:
            log.warning("initial goto failed: %s", e)

        await self._page.wait_for_timeout(2000)

        # If session file was loaded, inject localStorage on the correct origin
        if self.session_file:
            await self._inject_localstorage(self.session_file)
            # Reload to let Auth0 SDK pick up tokens
            try:
                await self._page.goto("https://moclaw.ai/chat", timeout=30000, wait_until="domcontentloaded")
            except Exception:
                pass
            await self._page.wait_for_timeout(3000)

        # Ensure WS interceptor is active
        await self._page.evaluate(WS_INTERCEPTOR_JS)
        self._ws_hooked = True

        await self._refresh_login_state()

        if not self._logged_in:
            log.warning(
                "Not logged in to moclaw.ai. Please log in manually in the "
                "browser window, or provide a --session file."
            )
        else:
            log.info("Logged-in session detected — ready for chat requests")

        self._ready = True

    async def stop(self):
        self._ready = False
        if self._cam is not None:
            try:
                await self._cam.__aexit__(None, None, None)
            except Exception as e:
                log.warning("camoufox shutdown: %s", e)
        self._cam = self._browser = self._page = None

    async def _import_session(self, session_file: str):
        try:
            with open(session_file) as f:
                session = json.load(f)

            cookies = session.get("cookies", [])
            pw_cookies = []
            for c in cookies:
                cookie = {
                    "name": c["name"], "value": c["value"],
                    "domain": c.get("domain", ".moclaw.ai"),
                    "path": c.get("path", "/"),
                }
                if c.get("expires", -1) > 0:
                    cookie["expires"] = int(c["expires"])
                if c.get("httpOnly"):
                    cookie["httpOnly"] = True
                if c.get("secure"):
                    cookie["secure"] = True
                sam = c.get("sameSite", "").lower()
                if sam in ("strict", "lax", "none"):
                    cookie["sameSite"] = {"strict": "Strict", "lax": "Lax", "none": "None"}[sam]
                pw_cookies.append(cookie)

            if pw_cookies:
                await self._browser.add_cookies(pw_cookies)
            log.info("Imported %d cookies from %s", len(pw_cookies), session_file)
        except Exception as e:
            log.warning("Session import failed: %s", e)

    async def _inject_localstorage(self, session_file: str):
        try:
            with open(session_file) as f:
                session = json.load(f)
            ls = session.get("localStorage", {})
            for key, value in ls.items():
                await self._page.evaluate("([k,v]) => localStorage.setItem(k,v)", [key, value])
            log.info("Injected %d localStorage items", len(ls))
        except Exception as e:
            log.warning("localStorage inject failed: %s", e)

    async def _refresh_login_state(self):
        if self._page is None:
            self._logged_in = False
            return
        try:
            self._logged_in = bool(await self._page.evaluate(LOGIN_CHECK_JS))
        except Exception:
            self._logged_in = False

    def status(self) -> dict:
        return {
            "ready": self._ready,
            "logged_in": self._logged_in,
            "busy": self.lock.locked(),
            "profile": str(self.profile),
            "headless": self.headless,
            "url": self._page.url if self._page else None,
        }

    async def _find_input(self):
        """Find the chat input element."""
        for sel in SELECTOR_INPUT.split(", "):
            try:
                loc = self._page.locator(sel).first
                if await loc.count() > 0 and await loc.is_visible(timeout=2000):
                    return sel
            except Exception:
                continue
        return None

    async def _start_new_chat(self):
        """Navigate to fresh chat."""
        try:
            for sel in SELECTOR_NEW_CHAT.split(", "):
                try:
                    btn = self._page.locator(sel).first
                    if await btn.count() > 0 and await btn.is_visible(timeout=2000):
                        await btn.click(timeout=3000)
                        await self._page.wait_for_timeout(2000)
                        return
                except Exception:
                    continue
        except Exception:
            pass

        # Fallback: navigate to root
        try:
            await self._page.goto("https://moclaw.ai/chat", timeout=15000, wait_until="domcontentloaded")
            await self._page.wait_for_timeout(2000)
        except Exception as e:
            log.warning("new chat fallback failed: %s", e)

    async def stream_chat(self, payload: dict):
        """Async generator yielding (kind, value) tuples."""
        async with self.lock:
            await self._refresh_login_state()
            if not self._logged_in:
                yield ("done", {"status": 401, "phase": "not-logged-in"})
                return

            messages = payload.get("messages", [])
            prompt = compact_messages(messages)
            if not prompt.strip():
                yield ("done", {"status": 400, "phase": "end", "error": "empty prompt"})
                return

            try:
                # Ensure WS interceptor is active
                await self._page.evaluate(WS_INTERCEPTOR_JS)
                # Reset response buffer
                await self._page.evaluate(RESET_INTERCEPTOR_JS)

                # Start new chat
                await self._start_new_chat()

                # Find input
                input_sel = await self._find_input()
                if not input_sel:
                    yield ("done", {"status": 500, "phase": "end",
                                    "error": "chat input not found"})
                    return

                log.info("→ submitting prompt (%d chars, %d messages)", len(prompt), len(messages))

                # Type prompt
                await self._page.fill(input_sel, prompt)
                await self._page.wait_for_timeout(200)
                await self._page.press(input_sel, "Enter")

                yield ("done", {"status": 200, "phase": "start"})

                # Poll for response via DOM scraping (most reliable).
                # MoClaw renders assistant messages in the chat UI. We poll
                # the last assistant message element until it stops growing.
                deadline = time.time() + 120
                last_text = ""
                stable_count = 0
                first_chunk_sent = False

                while time.time() < deadline:
                    await self._page.wait_for_timeout(1000)

                    # Extract the last assistant message text from DOM
                    current_text = await self._page.evaluate(r"""
                        () => {
                            // Find all assistant message containers
                            const msgs = document.querySelectorAll(
                                '[data-role="assistant"], .prose, [class*="markdown"]'
                            );
                            if (msgs.length === 0) return '';
                            const last = msgs[msgs.length - 1];
                            return last.innerText || last.textContent || '';
                        }
                    """) or ""

                    if current_text and current_text != last_text:
                        # New content arrived — send delta
                        delta = current_text[len(last_text):]
                        if delta:
                            chunk = json.dumps({
                                "type": "chat:completion",
                                "data": {"delta_content": delta, "phase": "answer"}
                            })
                            yield ("chunk", f"data: {chunk}\n\n".encode())
                            first_chunk_sent = True
                        last_text = current_text
                        stable_count = 0
                    elif current_text and current_text == last_text:
                        stable_count += 1
                        # If text hasn't changed for 5 seconds, consider done
                        if stable_count >= 5 and first_chunk_sent:
                            break
                    elif not current_text and not first_chunk_sent:
                        # Still waiting for first content
                        pass

                # Also try WS interceptor as supplementary
                try:
                    result = await self._page.evaluate(POLL_RESPONSES_JS)
                    ws_events = result.get("events", [])
                    if ws_events and not first_chunk_sent:
                        # WS interceptor caught something DOM didn't
                        for evt in ws_events:
                            if evt.get("type") == "text" and evt.get("text"):
                                chunk = json.dumps({
                                    "type": "chat:completion",
                                    "data": {"delta_content": evt["text"], "phase": "answer"}
                                })
                                yield ("chunk", f"data: {chunk}\n\n".encode())
                                first_chunk_sent = True
                except Exception:
                    pass

                # End marker
                done_chunk = json.dumps({
                    "type": "chat:completion",
                    "data": {"phase": "done", "done": True}
                })
                yield ("chunk", f"data: {done_chunk}\n\n".encode())

                if not first_chunk_sent:
                    log.warning("no response content captured")

                yield ("done", {"status": 200, "phase": "end"})

            except Exception as e:
                log.exception("stream_chat error: %s", e)
                yield ("done", {"status": 500, "phase": "end",
                                "error": f"worker exception: {e}"})


# ─── HTTP routes ──────────────────────────────────────────────────────────────
async def chat_route(request: web.Request) -> web.StreamResponse:
    bm: BrowserManager = request.app["bm"]
    try:
        payload = await request.json()
    except Exception as e:
        return web.json_response({"error": f"invalid JSON: {e}"}, status=400)

    log.info("→ /chat model=%s msgs=%d stream=%s",
             payload.get("model"), len(payload.get("messages", [])), payload.get("stream"))

    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
    await resp.prepare(request)

    async for kind, val in bm.stream_chat(payload):
        if kind == "done":
            meta = val if isinstance(val, dict) else json.loads(val)
            phase = meta.get("phase")
            stat = meta.get("status", 200)

            if phase == "start":
                await resp.write(f": moclaw-status {stat}\n\n".encode())
                continue

            if phase == "end":
                if meta.get("error"):
                    err = str(meta["error"]).replace("\n", " ")
                    await resp.write(f": moclaw-error {err}\n\n".encode())
                break

            if phase == "not-logged-in":
                await resp.write(
                    b': moclaw-error not-logged-in\n\n'
                    b'data: {"error":{"message":"browser session not logged in",'
                    b'"type":"auth_error"}}\n\n'
                )
                break
        else:
            data = val.encode() if isinstance(val, str) else val
            await resp.write(data)

    await resp.write_eof()
    return resp


async def status_route(request: web.Request) -> web.Response:
    return web.json_response(request.app["bm"].status())


async def reset_route(request: web.Request) -> web.Response:
    bm: BrowserManager = request.app["bm"]
    await bm.stop()
    await bm.start()
    return web.json_response({"reset": True, "status": bm.status()})


# ─── app bootstrap ────────────────────────────────────────────────────────────
async def _build_app(args) -> web.Application:
    bm = BrowserManager(
        profile=Path(args.profile).expanduser(),
        headless=args.headless,
        geoip=args.geoip,
        session_file=args.session,
    )
    await bm.start()

    app = web.Application(client_max_size=4 * 1024 * 1024)
    app["bm"] = bm
    app.router.add_post("/chat", chat_route)
    app.router.add_get("/status", status_route)
    app.router.add_post("/reset", reset_route)

    async def _shutdown(_):
        await bm.stop()
    app.on_shutdown.append(_shutdown)

    return app


def main():
    p = argparse.ArgumentParser(description="MoClaw browser-pool worker")
    p.add_argument("--port", type=int, default=9002)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--profile", default="~/.bifrost/moclaw-profiles/default")
    p.add_argument("--session", default=None,
                   help="Path to session JSON (from auto-login export: cookies + localStorage)")
    p.add_argument("--headless", nargs="?", const=True, default=False)
    p.add_argument("--no-geoip", dest="geoip", action="store_false", default=True)
    args = p.parse_args()

    if isinstance(args.headless, str) and args.headless.lower() in ("virtual", "v", "xvfb"):
        args.headless = "virtual"

    log.info("moclaw-worker booting on http://%s:%d", args.host, args.port)
    web.run_app(_build_app(args), host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
