#!/usr/bin/env python3
"""
Automated Google OAuth login for moclaw.ai.

Usage: python3 moclaw_login.py <email> <password> <headless: true|false>

Stdout protocol:
  PROGRESS:<message>          — status update
  DONE:<json>                 — success; json has access_token, refresh_token, expires_in
  ERROR:<message>             — fatal error
"""
import asyncio
import json
import os
import sys
import urllib.request

EMAIL    = sys.argv[1]
PASSWORD = sys.argv[2]
HEADLESS = sys.argv[3].lower() == "true"


# Result tracking — main() wraps do_login() in a retry loop and inspects
# these flags after each attempt to decide whether to retry, finalize, or
# bail out. do_login() itself still uses prog/err/done as before; we just
# layer in the flag-setting wrappers.
_RESULT_STATE = {"done": False, "last_error": ""}


def prog(msg: str):
    print(f"PROGRESS:{msg}", flush=True)


def done(tokens_json: str):
    _RESULT_STATE["done"] = True
    print(f"DONE:{tokens_json}", flush=True)


def err(msg: str):
    # Don't emit ERROR: yet — we may retry. main() prints the final ERROR:
    # after the last attempt fails, so the parent process only sees ONE
    # terminal event per invocation.
    _RESULT_STATE["last_error"] = msg
    print(f"PROGRESS:attempt failed: {msg}", flush=True)


def emit_final_error(msg: str):
    """Print the terminal ERROR: line after all retries are exhausted."""
    print(f"ERROR:{msg}", flush=True)


# JavaScript injected into the page to read Auth0 SPA SDK tokens from localStorage.
EXTRACT_JS = """
() => {
    try {
        for (var i = 0; i < localStorage.length; i++) {
            var k = localStorage.key(i);
            if (k && k.indexOf('@@auth0spajs@@') !== -1) {
                var v = JSON.parse(localStorage.getItem(k));
                var b = v && v.body;
                if (b && b.access_token) {
                    return JSON.stringify({
                        access_token:  b.access_token,
                        refresh_token: b.refresh_token || '',
                        expires_in:    b.expires_in    || 86400
                    });
                }
            }
        }
    } catch (e) {}
    return null;
}
"""

_GOOGLE_SELECTORS = [
    "text=Continue with Google",
    "button:has-text('Google')",
    "a:has-text('Google')",
    "[data-provider='google']",
    "[data-connection='google']",
    "[data-action='google']",
    # Auth0 universal login page selectors
    "[data-provider='google-oauth2']",
    "[data-connection='google-oauth2']",
    "button:has-text('google')",
    "a[href*='google']",
    ".social-button.google",
    "[class*='google']",
    "form[data-provider='google-oauth2'] button",
]

# "I understand" in multiple locales (Google Workspace new-account interstitial)
_UNDERSTAND_TEXTS = [
    "I understand",   # EN
    "Saya mengerti",  # ID
    "Saya faham",     # MS (Malay)
    "Je comprends",   # FR
    "Ich verstehe",   # DE
    "Entendido",      # ES/PT
    "Ho capito",      # IT
]

# "Continue" in multiple locales (Google OAuth consent screen)
_CONTINUE_TEXTS = [
    "Continue",    # EN
    "Lanjutkan",   # ID
    "Teruskan",    # MS (Malay)
    "Continuer",   # FR
    "Weiter",      # DE
    "Continuar",   # ES/PT
    "Continua",    # IT
]


async def poll_tokens(page, tries: int = 90) -> str | None:
    """Poll localStorage every second until tokens appear or tries exhausted.

    Bumped from 30s to 90s to give Auth0's SPA SDK enough headroom on
    slow headless launches and constrained networks. Auth0 sometimes
    re-attempts the silent code-for-token exchange a few times after a
    Google redirect; 30s wasn't enough on first runs.
    """
    for _ in range(tries):
        result = await page.evaluate(EXTRACT_JS)
        if result and result != "null":
            return result
        await asyncio.sleep(1)
    return None


async def fill_google_credentials(login_page) -> bool:
    """Fill email + password on whichever page context Google opened in."""
    # Email
    prog("Entering Google email...")
    try:
        # Bumped from 25s → 45s: Google's first-paint on headless launches
        # can be slow, especially if Camoufox is fetching fresh fingerprints.
        await login_page.wait_for_selector('input[type="email"]', timeout=45000)
        await login_page.fill('input[type="email"]', EMAIL)
        await login_page.keyboard.press("Enter")
        # Wait for page to transition to password screen
        await login_page.wait_for_timeout(3000)
        try:
            await login_page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception:
            pass
    except Exception as e:
        err(f"Email input not found: {e}")
        return False

    # Password — skip Google's hidden decoy field (aria-hidden="true")
    # Try multiple selectors in case Google changes their markup
    prog("Entering Google password...")
    pwd_filled = False
    for pwd_sel in [
        'input[type="password"]:not([aria-hidden="true"])',
        'input[type="password"][aria-hidden="false"]',
        'input[name="Passwd"]',
        'input[name="password"]',
        'input[type="password"]',
    ]:
        try:
            await login_page.wait_for_selector(pwd_sel, state="visible", timeout=12000)
            await login_page.fill(pwd_sel, PASSWORD)
            await login_page.keyboard.press("Enter")
            pwd_filled = True
            break
        except Exception:
            continue
    if not pwd_filled:
        # Google may show a phone verification / "verify it's you" screen.
        # Capture current URL to help diagnose.
        try:
            cur = login_page.url
        except Exception:
            cur = "unknown"
        err(f"Password field not found (url={cur[:80]}) — possible Google verification screen or wrong credentials")
        return False

    return True


async def do_login(page) -> None:
    # Clear all cookies + storage so every session starts fresh and Auth0
    # always issues a new refresh_token with offline_access scope.
    await page.context.clear_cookies()
    try:
        await page.context.clear_permissions()
    except Exception:
        pass

    prog("Navigating to moclaw.ai...")
    await page.goto("https://moclaw.ai/", timeout=30000, wait_until="domcontentloaded")
    await page.wait_for_timeout(2500)

    # ── Click "Get Started" / "Login" ────────────────────────────────────────
    prog("Clicking Get Started...")
    clicked = False
    for selector in [
        "a:has-text('Get Started')",
        "button:has-text('Get Started')",
        "text=Get Started",
        "a:has-text('Login')",
        "button:has-text('Login')",
        "a:has-text('Log in')",
        "button:has-text('Log in')",
        "a:has-text('Sign in')",
        "button:has-text('Sign in')",
        "text=Login",
        "text=Log in",
    ]:
        try:
            await page.locator(selector).first.click(timeout=5000)
            clicked = True
            break
        except Exception:
            continue
    if not clicked:
        # /login 404s on moclaw.ai — just wait a bit longer on the homepage
        # and let the Google button appear naturally
        prog("Login button not found on homepage — waiting for Google button...")
        await page.wait_for_timeout(3000)
    await page.wait_for_timeout(1500)

    # ── Click "Continue with Google" — may open popup or redirect in-page ────
    prog("Clicking Continue with Google...")

    # If we landed on Auth0's login page, wait for it to fully render
    if "auth.moclaw.ai" in page.url or "auth0" in page.url:
        prog("Auth0 login page detected — waiting for social buttons...")
        await page.wait_for_load_state("networkidle", timeout=10000)
        await page.wait_for_timeout(2000)

    google_clicked = False
    for selector in _GOOGLE_SELECTORS:
        try:
            await page.locator(selector).first.click(timeout=4000)
            google_clicked = True
            break
        except Exception:
            continue

    if not google_clicked:
        # Last resort: try clicking any element that mentions "google" via JS
        try:
            await page.evaluate("""
                () => {
                    const els = document.querySelectorAll('button, a, [role="button"]');
                    for (const el of els) {
                        if ((el.textContent || '').toLowerCase().includes('google') ||
                            (el.className || '').toLowerCase().includes('google') ||
                            (el.getAttribute('data-provider') || '').includes('google')) {
                            el.click();
                            return true;
                        }
                    }
                    return false;
                }
            """)
            prog("Clicked Google button via JS fallback")
        except Exception:
            prog("WARNING: Could not find Google login button")

    # Wait a moment, then find whichever page/popup ended up on accounts.google.com
    await page.wait_for_timeout(3000)
    login_page = page
    for p in page.context.pages:
        try:
            if "accounts.google" in p.url or "google.com/o/oauth2" in p.url:
                await p.wait_for_load_state("domcontentloaded", timeout=5000)
                login_page = p
                prog(f"Google login page found: {p.url[:60]}")
                break
        except Exception:
            continue
    if login_page is page:
        prog("Google login in same tab")

    # ── Fill credentials in the correct page context ──────────────────────────
    if not await fill_google_credentials(login_page):
        return

    # ── Handle all Google interstitials in a polling loop ────────────────────
    # After password submit, Google may show multiple pages before redirecting:
    # "I understand" → "Continue (consent)" → final redirect to moclaw.ai.
    # We poll until we leave accounts.google.com or timeout.
    prog("Handling Google post-login screens...")
    deadline = asyncio.get_event_loop().time() + 40  # max 40s
    understand_sel = ", ".join(f"button:has-text('{t}')" for t in _UNDERSTAND_TEXTS)
    continue_sel   = ", ".join(f"button:has-text('{t}')" for t in _CONTINUE_TEXTS)
    while asyncio.get_event_loop().time() < deadline:
        try:
            current_url = login_page.url
        except Exception:
            break  # popup closed / navigated away
        if "accounts.google" not in current_url and "google.com" not in current_url:
            break  # left Google — done

        clicked = False
        for sel, label in [
            (understand_sel, "I understand"),
            (continue_sel,   "Continue"),
        ]:
            try:
                btn = login_page.locator(sel).first
                if await btn.is_visible(timeout=800):
                    prog(f"Clicking '{label}'...")
                    await btn.click()
                    await login_page.wait_for_timeout(1200)
                    clicked = True
                    break
            except Exception:
                pass

        if not clicked:
            await asyncio.sleep(1)

    # ── Wait for moclaw.ai to load (main page may redirect while popup closes)
    prog("Waiting for moclaw.ai to load...")
    try:
        await page.wait_for_url("*moclaw.ai*", timeout=30000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)

    # ── Extract tokens from localStorage ─────────────────────────────────────
    prog("Extracting tokens from localStorage...")
    tokens = await poll_tokens(page)
    if not tokens:
        hint = " (try non-headless mode to handle 2FA / captcha)" if HEADLESS else ""
        err(f"Login completed but tokens not found in localStorage{hint}")
        return

    # ── Activate free trial via API ───────────────────────────────────────────
    # 1. Check eligibility → 2. Enroll → 3. Sync to confirm.
    try:
        access_token = json.loads(tokens).get("access_token", "")
        headers = {
            "authorization": f"Bearer {access_token}",
            "content-type": "application/json",
            "content-length": "0",
            "origin": "https://moclaw.ai",
            "referer": "https://moclaw.ai/",
        }

        # Check eligibility first
        elig_req = urllib.request.Request(
            "https://api.moclaw.ai/api/subscription/trial/eligibility",
            headers=headers,
        )
        with urllib.request.urlopen(elig_req, timeout=10) as r:
            elig = json.loads(r.read())
        prog(f"Trial eligibility: {elig}")

        if elig.get("eligible", False):
            # Enroll in trial
            enroll_req = urllib.request.Request(
                "https://api.moclaw.ai/api/subscription/trial/enroll",
                data=b"",
                method="POST",
                headers=headers,
            )
            with urllib.request.urlopen(enroll_req, timeout=15) as r:
                enroll_data = json.loads(r.read())
            prog(f"Trial enrolled! {enroll_data}")
        else:
            prog(f"Trial not eligible (already used or not available): {elig}")

        # Sync to confirm final state
        sync_req = urllib.request.Request(
            "https://api.moclaw.ai/api/subscription/sync",
            data=b"",
            method="POST",
            headers=headers,
        )
        with urllib.request.urlopen(sync_req, timeout=15) as r:
            sync_data = json.loads(r.read())
        prog(f"Subscription sync: status={sync_data.get('status','?')}, changed={sync_data.get('changed','?')}")

    except Exception as e:
        prog(f"Trial activation skipped: {e}")

    # ── Validate balance via API ──────────────────────────────────────────────
    try:
        access_token = json.loads(tokens).get("access_token", "")
        req = urllib.request.Request(
            "https://api.moclaw.ai/api/credits/balance",
            headers={
                "authorization": f"Bearer {access_token}",
                "content-type": "application/json",
                "origin": "https://moclaw.ai",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            bal = json.loads(resp.read())
            total = bal.get("total_balance", "?")
            plan  = bal.get("plan", {}).get("display_name", "?")
            prog(f"Balance validated: {total} credits ({plan})")
    except Exception as e:
        prog(f"Balance check skipped: {e}")

    # ── Export session per account (cookies + localStorage) ──────────────────
    # MoClaw uses Auth0 SPA SDK — auth state lives in localStorage, not cookies.
    # We export both so the session can be fully restored later.
    try:
        import os
        session_dir = os.path.join(os.path.expanduser("~"), ".bifrost", "moclaw_sessions")
        os.makedirs(session_dir, exist_ok=True)
        safe_email = EMAIL.replace("/", "_").replace("\\", "_")

        # Cookies
        cookies = await page.context.cookies()

        # localStorage (Auth0 tokens live here)
        local_storage = await page.evaluate("""
            () => {
                const data = {};
                for (let i = 0; i < localStorage.length; i++) {
                    const key = localStorage.key(i);
                    data[key] = localStorage.getItem(key);
                }
                return data;
            }
        """)

        session_data = {
            "email": EMAIL,
            "cookies": cookies,
            "localStorage": local_storage,
        }
        session_path = os.path.join(session_dir, f"{safe_email}.json")
        with open(session_path, "w") as f:
            json.dump(session_data, f, indent=2)

        ls_count = len(local_storage)
        auth0_keys = [k for k in local_storage if "auth0" in k.lower()]
        prog(f"Session exported: {session_path} ({len(cookies)} cookies, {ls_count} localStorage items, {len(auth0_keys)} auth0 keys)")
    except Exception as e:
        prog(f"Session export skipped: {e}")

    done(tokens)


async def main() -> None:
    try:
        from camoufox.async_api import AsyncCamoufox  # type: ignore
    except ImportError:
        emit_final_error(
            "camoufox not installed. "
            "Install with: pip install 'camoufox[geoip]' && python -m camoufox fetch"
        )
        return

    # Tunables (env-overridable so callers don't need a redeploy):
    #   MOCLAW_LOGIN_RETRIES — total attempts per account (default 3)
    #   MOCLAW_LOGIN_GEOIP   — "1" to keep MaxMind GeoIP lookup (some macOS
    #                          setups hang on it; we default OFF to be safe).
    max_attempts = 3
    try:
        if os.environ.get("MOCLAW_LOGIN_RETRIES"):
            max_attempts = max(1, int(os.environ["MOCLAW_LOGIN_RETRIES"]))
    except Exception:
        pass

    geoip_enabled = os.environ.get("MOCLAW_LOGIN_GEOIP") == "1"

    last_error = "unknown"
    for attempt in range(1, max_attempts + 1):
        # Reset the result flag so a previous attempt's err()/done() doesn't
        # falsely satisfy the "succeeded" check below.
        _RESULT_STATE["done"] = False
        _RESULT_STATE["last_error"] = ""

        if attempt > 1:
            # Brief backoff between attempts to let any transient Google /
            # Auth0 rate-limit cool off. Grows linearly: 3s, 6s, 9s, ...
            backoff = 3 * (attempt - 1)
            prog(f"Retry {attempt}/{max_attempts} after {backoff}s...")
            await asyncio.sleep(backoff)
        else:
            prog(f"Attempt {attempt}/{max_attempts}: launching Camoufox (anti-detect Firefox)...")

        try:
            # Use a FRESH browser/context per attempt so leftover state
            # (partial OAuth cookies, half-written localStorage, lingering
            # popups) can't poison the next try.
            async with AsyncCamoufox(headless=HEADLESS, geoip=geoip_enabled) as browser:
                page = await browser.new_page()
                await do_login(page)
        except Exception as e:
            _RESULT_STATE["last_error"] = f"Camoufox error: {e}"
            prog(f"PROGRESS:attempt {attempt} crashed: {e}")

        if _RESULT_STATE["done"]:
            return  # done() already printed DONE: line — we're finished

        last_error = _RESULT_STATE["last_error"] or "no tokens"
        if attempt < max_attempts:
            prog(f"Attempt {attempt} failed ({last_error}); closing browser and retrying...")

    # All attempts exhausted — emit the final terminal error.
    emit_final_error(f"All {max_attempts} attempts failed. Last error: {last_error}")


asyncio.run(main())
