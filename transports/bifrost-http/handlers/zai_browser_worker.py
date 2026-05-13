#!/usr/bin/env python3
"""
z.ai browser-pool worker — Phase 1 (single worker, single profile).

Strategy: UI automation, not API replay
---------------------------------------
chat.z.ai's web API requires:
  • a per-request x-signature (HMAC computed inside the obfuscated JS bundle),
  • a one-time captcha_verify_param produced by an Alibaba "invisible"
    captcha widget that scores the browser's runtime behavior,
  • signature_timestamp / requestId / message-id UUIDs aligned with the
    signature,
  • a handful of body fields (chat_id, current_user_message_id, variables,
    features, signature_prompt) that the frontend assembles right before
    POST.

Reproducing those by hand from Python is brittle — every z.ai deploy can
shift the algorithm. So this worker DOES NOT try. It just drives the real
chat UI:

  1. Camoufox launches with a persistent profile (cookies + localStorage).
  2. Browser stays on chat.z.ai, logged in.
  3. For each /chat request the worker:
        a. clicks "New Chat" (or navigates to /) to start clean state,
        b. waits for #chat-input,
        c. types the compacted prompt,
        d. presses Enter,
        e. intercepts the resulting POST /api/v2/chat/completions response
           via Playwright `page.on('response')`,
        f. streams the SSE body back to the caller.

Z.ai's own bundle handles signing, captcha solving, and field assembly —
the worker is just an observer that copies the response out.

Endpoints
---------
  POST /chat   — drive UI for one prompt; stream SSE back
  GET  /status — JSON snapshot: {ready, logged_in, busy, url, profile}
  POST /reset  — close & relaunch the browser (profile is preserved)

Usage
-----
  python3 zai_browser_worker.py \\
      --port 9001 \\
      --profile ~/.bifrost/zai-profiles/default \\
      [--headless]

First-run flow
--------------
  1. Launches a *visible* Camoufox window with the profile dir.
  2. Navigates to chat.z.ai.
  3. If not logged in: leaves the window open; user logs in manually.
  4. Future runs reuse the profile → instant ready.

Phase 1 limitations
-------------------
  • Multi-turn conversation history is compacted into a single prompt
    string (z.ai's UI keeps its own per-conversation history, which we
    can't faithfully replay; assistant turns become quoted context).
  • One concurrent request at a time (serialized by asyncio.Lock).
  • Streaming granularity = full response body split on SSE event
    boundaries. True token-level streaming arrives in a later phase via
    CDP / route-handler-based incremental reads.

Dependencies
------------
  pip install aiohttp 'camoufox[geoip]'
  python -m camoufox fetch
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

try:
    from aiohttp import web
except ImportError:
    sys.stderr.write("FATAL: aiohttp not installed. Install: pip install aiohttp\n")
    sys.exit(1)

try:
    from camoufox.async_api import AsyncCamoufox  # type: ignore
except ImportError:
    sys.stderr.write(
        "FATAL: camoufox not installed.\n"
        "Install: pip install 'camoufox[geoip]' && python -m camoufox fetch\n"
    )
    sys.exit(1)


# ─── logging ────────────────────────────────────────────────────────────────
log = logging.getLogger("zai-worker")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


# ─── selectors (captured from a real logged-in chat page) ───────────────────
# chat textarea — stable ID, present on every chat page
SELECTOR_INPUT     = "#chat-input"
# explicit "New Chat" button in the sidebar; we match by visible text so
# this survives Tailwind class churn
SELECTOR_NEW_CHAT  = 'button:has-text("New Chat")'
# the small send button next to the textarea (SVG-only, no text). Used as a
# fallback if Enter key doesn't dispatch a submit.
SELECTOR_SEND_FALLBACK = '#chat-input ~ button[type="submit"]'

# URL fragment used to detect that a POST request is the chat completion call
COMPLETIONS_URL_FRAGMENT = "/api/v2/chat/completions"


# ─── login detection ────────────────────────────────────────────────────────
# Best-effort login heuristic. chat.z.ai mutates class names across releases,
# so we combine a stable selector (#chat-input) with cookie/localStorage
# fallbacks.
LOGIN_CHECK_JS = r"""
() => {
    if (document.querySelector('#chat-input'))            return true;
    if (document.querySelector('[class*="chat-input"]'))  return true;
    if (/(?:^|; )token=/.test(document.cookie))           return true;
    try {
        if (localStorage.getItem('token'))                return true;
    } catch (e) {}
    return false;
}
"""


# ─── message compaction ─────────────────────────────────────────────────────
def _msg_content(msg: dict) -> str:
    """Extract a plain string from a Bifrost/OpenAI message content field.
    Supports both `content: "..."` and `content: [{type:'text', text:'...'}]`.
    """
    c = msg.get("content")
    if isinstance(c, str):
        return c
    if isinstance(c, list):
        return "".join(
            (p.get("text") or "")
            for p in c
            if isinstance(p, dict)
        )
    return str(c) if c is not None else ""


def compact_messages(messages: list[dict]) -> str:
    """Render a Bifrost/OpenAI `messages` array as a single textarea-ready
    string. Single-message inputs pass through unchanged; multi-turn inputs
    get a `[Conversation so far]` preamble so z.ai's model can see prior
    context, even though it can't be replayed as proper turns through the UI.
    """
    if not messages:
        return ""
    if len(messages) == 1:
        return _msg_content(messages[0])

    last     = messages[-1]
    history  = messages[:-1]
    parts: list[str] = []

    system = next((m for m in history if m.get("role") == "system"), None)
    if system:
        sys_text = _msg_content(system).strip()
        if sys_text:
            parts.append(f"[System instructions]\n{sys_text}\n")
        history = [m for m in history if m is not system]

    if history:
        parts.append("[Conversation so far]")
        for m in history:
            role = (m.get("role") or "user").capitalize()
            parts.append(f"{role}: {_msg_content(m)}")
        parts.append("")  # blank separator

    parts.append(_msg_content(last))
    return "\n".join(parts)


# ─── BrowserManager ─────────────────────────────────────────────────────────
class BrowserManager:
    """Owns the Camoufox instance and the active chat.z.ai page.

    HTTP routes stay thin — all browser interaction lives here. A single
    asyncio.Lock serializes chat requests so Phase 1 can stay simple;
    multi-browser concurrency arrives in Phase 4.
    """

    def __init__(self, profile: Path, headless, geoip: bool = True):
        """`headless` is one of:
              False       — visible window (default; use for first login)
              True        — true headless (fastest, but more captcha hits)
              "virtual"   — xvfb-backed headless (recommended on Linux VPS;
                            requires xvfb installed: `apt install xvfb`)

        `geoip` controls whether Camoufox spoofs locale/timezone from the
        outgoing IP via MaxMind GeoLite2. Disable on platforms where the
        geoip lookup blocks launch (e.g. some macOS setups).
        """
        self.profile = profile
        self.headless = headless
        self.geoip = geoip

        self._cam = None
        self._browser = None
        self._page = None
        self._ready = False
        self._logged_in = False

        self.lock = asyncio.Lock()

    # ─── lifecycle ─────────────────────────────────────────────────
    async def start(self, cookies_file: str | None = None):
        self.profile.mkdir(parents=True, exist_ok=True)
        log.info("Launching Camoufox (profile=%s, headless=%s)",
                 self.profile, self.headless)

        self._cam = AsyncCamoufox(
            persistent_context=True,
            user_data_dir=str(self.profile),
            headless=self.headless,
            geoip=self.geoip,
        )
        # With persistent_context=True the context manager yields a
        # BrowserContext (not a Browser). Either object exposes `pages`
        # and `new_page`, so the same code path works.
        self._browser = await self._cam.__aenter__()

        # Import cookies from JSON file (Chrome extension export format).
        # This enables headless login without manual interaction.
        if cookies_file:
            await self._import_cookies(cookies_file)

        pages = getattr(self._browser, "pages", []) or []
        self._page = pages[0] if pages else await self._browser.new_page()

        log.info("Navigating to chat.z.ai ...")
        try:
            await self._page.goto(
                "https://chat.z.ai/",
                wait_until="domcontentloaded",
                timeout=30000,
            )
        except Exception as e:
            log.warning("initial goto failed (will retry on first /chat): %s", e)

        # Give the captcha widget + auth bootstrap a moment to settle.
        await self._page.wait_for_timeout(2500)
        await self._refresh_login_state()

        if not self._logged_in:
            log.warning(
                "Not logged in to chat.z.ai. The browser window will stay "
                "open — please log in manually. Once logged in, /chat will "
                "start working without restarting the worker."
            )
        else:
            log.info("Logged-in session detected — ready for chat requests")

        self._ready = True

    async def _import_cookies(self, cookies_file: str):
        """Load cookies from a Chrome-extension-style JSON export into the
        browser context. Converts the Chrome format to Playwright format."""
        try:
            with open(cookies_file) as f:
                raw_cookies = json.load(f)
            log.info("Importing %d cookies from %s", len(raw_cookies), cookies_file)

            pw_cookies = []
            for c in raw_cookies:
                cookie = {
                    "name": c["name"],
                    "value": c["value"],
                    "domain": c.get("domain", ".z.ai"),
                    "path": c.get("path", "/"),
                }
                if c.get("expirationDate"):
                    cookie["expires"] = int(c["expirationDate"])
                if c.get("httpOnly"):
                    cookie["httpOnly"] = True
                if c.get("secure"):
                    cookie["secure"] = True
                sam = c.get("sameSite", "").lower()
                if sam in ("strict", "lax", "none"):
                    cookie["sameSite"] = sam.capitalize()
                    if sam == "none":
                        cookie["sameSite"] = "None"
                pw_cookies.append(cookie)

            await self._browser.add_cookies(pw_cookies)
            log.info("Cookies imported successfully")
        except Exception as e:
            log.warning("Failed to import cookies from %s: %s", cookies_file, e)

    async def stop(self):
        self._ready = False
        if self._cam is not None:
            try:
                await self._cam.__aexit__(None, None, None)
            except Exception as e:
                log.warning("camoufox shutdown error: %s", e)
        self._cam = self._browser = self._page = None

    # ─── login detection ───────────────────────────────────────────
    async def _refresh_login_state(self):
        if self._page is None:
            self._logged_in = False
            return
        try:
            v = await self._page.evaluate(LOGIN_CHECK_JS)
            self._logged_in = bool(v)
        except Exception as e:
            log.warning("login check failed: %s", e)
            self._logged_in = False

    def status(self) -> dict:
        return {
            "ready":     self._ready,
            "logged_in": self._logged_in,
            "busy":      self.lock.locked(),
            "profile":   str(self.profile),
            "headless":  self.headless,
            "url":       self._page.url if self._page else None,
        }

    # ─── helpers ───────────────────────────────────────────────────
    async def _start_new_chat(self):
        """Start a fresh chat conversation so we don't accumulate UI history.

        Tries the visible "New Chat" button first (preserves SPA state, faster
        than full nav). Falls back to a hard navigation to root if the button
        is missing or click fails.
        """
        try:
            btn = self._page.locator(SELECTOR_NEW_CHAT).first
            if await btn.count() > 0:
                try:
                    visible = await btn.is_visible()
                except Exception:
                    visible = False
                if visible:
                    await btn.click(timeout=3000)
                    # Wait for the chat textarea to mount/clear under the new
                    # session.
                    await self._page.wait_for_selector(
                        SELECTOR_INPUT, state="visible", timeout=8000
                    )
                    # textarea may carry leftover text from previous session
                    try:
                        await self._page.fill(SELECTOR_INPUT, "")
                    except Exception:
                        pass
                    return
        except Exception as e:
            log.debug("new-chat button path failed (%s); falling back to goto", e)

        try:
            await self._page.goto(
                "https://chat.z.ai/",
                wait_until="domcontentloaded",
                timeout=15000,
            )
            await self._page.wait_for_selector(
                SELECTOR_INPUT, state="visible", timeout=10000
            )
        except Exception as e:
            log.warning("new-chat goto fallback failed: %s", e)

    # ─── streaming chat ────────────────────────────────────────────
    async def stream_chat(self, payload: dict):
        """Async generator yielding (kind, value) tuples:
          ('chunk', bytes) — raw SSE bytes from chat.z.ai
          ('done',  dict)  — {status, phase, [error]}

        Two 'done' events are produced per request: one with phase='start'
        carrying the upstream HTTP status, and one with phase='end' at the
        end of stream (or on error).
        """
        async with self.lock:
            await self._refresh_login_state()
            if not self._logged_in:
                yield ("done", {"status": 401, "phase": "not-logged-in"})
                return

            messages = payload.get("messages", [])
            prompt   = compact_messages(messages)
            if not prompt.strip():
                yield ("done", {"status": 400, "phase": "end",
                                "error": "empty prompt"})
                return

            loop = asyncio.get_event_loop()
            response_arrived: asyncio.Future = loop.create_future()

            def _on_response(response):
                # `response` is a sync Playwright object — accessing url /
                # .request is safe here. Only mark the future once.
                try:
                    if response_arrived.done():
                        return
                    if COMPLETIONS_URL_FRAGMENT not in response.url:
                        return
                    if response.request.method != "POST":
                        return
                    response_arrived.set_result(response)
                except Exception as e:
                    log.warning("response listener error: %s", e)

            self._page.on("response", _on_response)

            try:
                # Fresh chat keeps the UI deterministic and avoids leaking
                # prior turns into the model's context window.
                await self._start_new_chat()

                # Make sure the textarea is mounted (start_new_chat already
                # waits, but a defensive wait helps when New-Chat path was
                # skipped).
                await self._page.wait_for_selector(
                    SELECTOR_INPUT, state="visible", timeout=15000
                )

                log.info("→ submitting prompt (%d chars, %d messages)",
                         len(prompt), len(messages))
                await self._page.fill(SELECTOR_INPUT, prompt)
                # tiny settle so any input-debounced enable/disable logic
                # on the send button has time to flip
                await self._page.wait_for_timeout(150)
                await self._page.press(SELECTOR_INPUT, "Enter")

                # If Enter doesn't trigger the request within ~2s, try the
                # send button as fallback (some forms bind submit to a click
                # handler, not key press).
                try:
                    response = await asyncio.wait_for(
                        asyncio.shield(response_arrived), timeout=2.0
                    )
                except asyncio.TimeoutError:
                    log.info("  Enter didn't fire request — trying send button")
                    try:
                        btn = self._page.locator(SELECTOR_SEND_FALLBACK).first
                        if await btn.count() > 0:
                            await btn.click(timeout=2000)
                    except Exception as e:
                        log.debug("send-button fallback failed: %s", e)
                    response = await asyncio.wait_for(response_arrived,
                                                      timeout=20.0)

                yield ("done", {"status": response.status, "phase": "start"})

                # Pull the full body. Playwright doesn't expose chunked SSE
                # reads on Firefox — Phase 1.5 will add CDP-based streaming.
                try:
                    body = await response.body()
                except Exception as e:
                    yield ("done", {"status": response.status, "phase": "end",
                                    "error": f"body read failed: {e}"})
                    return

                if body:
                    # Split on SSE event boundaries so the consumer sees
                    # one event per write.
                    parts = body.split(b"\n\n")
                    for i, p in enumerate(parts):
                        if not p:
                            continue
                        # Re-append the separator we split on (except for a
                        # trailing empty part, which we already filtered).
                        chunk = p + b"\n\n"
                        yield ("chunk", chunk)

                yield ("done", {"status": response.status, "phase": "end"})

            except asyncio.TimeoutError:
                yield ("done", {"status": 504, "phase": "end",
                                "error": "no response from z.ai within 20s "
                                         "(UI selector may have changed, "
                                         "or page is unresponsive)"})
            except Exception as e:
                log.exception("stream_chat crashed: %s", e)
                yield ("done", {"status": 500, "phase": "end",
                                "error": f"worker exception: {e}"})
            finally:
                try:
                    self._page.remove_listener("response", _on_response)
                except Exception:
                    pass


# ─── HTTP routes ───────────────────────────────────────────────────────────
async def chat_route(request: web.Request) -> web.StreamResponse:
    bm: BrowserManager = request.app["bm"]
    try:
        payload = await request.json()
    except Exception as e:
        return web.json_response({"error": f"invalid JSON: {e}"}, status=400)
    log.info("→ /chat model=%s msgs=%d stream=%s",
             payload.get("model"),
             len(payload.get("messages", [])),
             payload.get("stream"))

    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type":      "text/event-stream",
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )
    await resp.prepare(request)

    upstream_status = None
    async for kind, val in bm.stream_chat(payload):
        if kind == "done":
            meta  = val if isinstance(val, dict) else json.loads(val)
            phase = meta.get("phase")
            stat  = meta.get("status", 200)

            if phase == "start" and upstream_status is None:
                upstream_status = stat
                # Embed upstream status in an SSE comment so the Go adapter
                # can map it without us re-encoding HTTP status codes.
                await resp.write(f": zai-status {stat}\n\n".encode())
                continue

            if phase == "end":
                if meta.get("error"):
                    err = str(meta["error"]).replace("\n", " ")
                    await resp.write(f': zai-error {err}\n\n'.encode())
                break

            if phase == "not-logged-in":
                await resp.write(
                    b': zai-error not-logged-in\n\n'
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


# ─── app bootstrap ─────────────────────────────────────────────────────────
async def _build_app(args) -> web.Application:
    bm = BrowserManager(
        profile=Path(args.profile).expanduser(),
        headless=args.headless,
        geoip=args.geoip,
    )
    await bm.start(cookies_file=args.cookies)

    app = web.Application(client_max_size=4 * 1024 * 1024)
    app["bm"] = bm
    app.router.add_post("/chat",   chat_route)
    app.router.add_get ("/status", status_route)
    app.router.add_post("/reset",  reset_route)

    async def _shutdown(_):
        await bm.stop()
    app.on_shutdown.append(_shutdown)

    return app


def main():
    p = argparse.ArgumentParser(
        description="z.ai browser-pool worker (Phase 1, single instance)"
    )
    p.add_argument("--port",     type=int, default=9001)
    p.add_argument("--host",     default="127.0.0.1")
    p.add_argument("--profile",  default="~/.bifrost/zai-profiles/default")
    p.add_argument(
        "--headless",
        nargs="?", const=True, default=False,
        help="Headless mode. Pass --headless (true headless) or "
             "--headless=virtual (xvfb-backed; recommended for VPS, needs xvfb "
             "installed). Default: visible window (required for first login).",
    )
    p.add_argument(
        "--no-geoip", dest="geoip", action="store_false", default=True,
        help="Disable MaxMind GeoIP lookup. Use this if Camoufox exits "
             "immediately on launch (some macOS setups). Default: geoip on.",
    )
    p.add_argument(
        "--cookies", default=None,
        help="Path to a cookies JSON file (Chrome extension export format). "
             "Cookies are imported into the browser on startup, enabling "
             "headless login without manual interaction.",
    )
    args = p.parse_args()

    # Normalize: argparse gives string "virtual" or True/False.
    if isinstance(args.headless, str) and args.headless.lower() in ("virtual", "v", "xvfb"):
        args.headless = "virtual"

    log.info("zai-worker booting on http://%s:%d", args.host, args.port)
    web.run_app(_build_app(args), host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
