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
import sys
import urllib.request

EMAIL    = sys.argv[1]
PASSWORD = sys.argv[2]
HEADLESS = sys.argv[3].lower() == "true"


def prog(msg: str):
    print(f"PROGRESS:{msg}", flush=True)


def done(tokens_json: str):
    print(f"DONE:{tokens_json}", flush=True)


def err(msg: str):
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


async def poll_tokens(page, tries: int = 30) -> str | None:
    """Poll localStorage every second until tokens appear or tries exhausted."""
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
        await login_page.wait_for_selector('input[type="email"]', timeout=15000)
        await login_page.fill('input[type="email"]', EMAIL)
        await login_page.keyboard.press("Enter")
        # Wait for page to transition to password screen
        await login_page.wait_for_timeout(2500)
        try:
            await login_page.wait_for_load_state("domcontentloaded", timeout=8000)
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
    ]:
        try:
            await login_page.wait_for_selector(pwd_sel, state="visible", timeout=8000)
            await login_page.fill(pwd_sel, PASSWORD)
            await login_page.keyboard.press("Enter")
            pwd_filled = True
            break
        except Exception:
            continue
    if not pwd_filled:
        err("Password field not found — check credentials or 2FA requirement")
        return False

    return True


async def do_login(page) -> None:
    prog("Navigating to moclaw.ai...")
    await page.goto("https://moclaw.ai/", timeout=30000, wait_until="domcontentloaded")
    await page.wait_for_timeout(2500)

    # ── Click "Get Started" ───────────────────────────────────────────────────
    prog("Clicking Get Started...")
    clicked = False
    for selector in [
        "a:has-text('Get Started')",
        "button:has-text('Get Started')",
        "text=Get Started",
    ]:
        try:
            await page.locator(selector).first.click(timeout=4000)
            clicked = True
            break
        except Exception:
            continue
    if not clicked:
        prog("Get Started not found — navigating to /login directly...")
        try:
            await page.goto("https://moclaw.ai/login", timeout=15000, wait_until="domcontentloaded")
        except Exception:
            pass
    await page.wait_for_timeout(1500)

    # ── Click "Continue with Google" — may open popup or redirect in-page ────
    prog("Clicking Continue with Google...")

    for selector in _GOOGLE_SELECTORS:
        try:
            await page.locator(selector).first.click(timeout=4000)
            break
        except Exception:
            continue

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

    # ── Poll for "Start Free Trial" modal (new accounts, can take 5-30s) ──────
    # Don't use networkidle — dashboard keeps firing API calls indefinitely.
    # Just poll every second for up to 35 seconds.
    prog("Waiting for Free Trial modal...")
    trial_deadline = asyncio.get_event_loop().time() + 35
    trial_clicked = False
    while asyncio.get_event_loop().time() < trial_deadline:
        try:
            btn = page.locator("button:has-text('Start Free Trial')").first
            if await btn.is_visible(timeout=800):
                prog("Clicking Start Free Trial...")
                await btn.click()
                trial_clicked = True
                await page.wait_for_timeout(2000)
                break
        except Exception:
            pass
        await asyncio.sleep(1)

    if not trial_clicked:
        prog("Start Free Trial modal not seen — account may already have a plan")

    # ── "Trial started! → Get started" confirmation modal ────────────────────
    if trial_clicked:
        try:
            btn = page.locator("button:has-text('Get started')").first
            if await btn.is_visible(timeout=8000):
                prog("Trial started! Clicking Get started...")
                await btn.click()
                await page.wait_for_timeout(1000)
        except Exception:
            pass

    # ── Extract tokens from localStorage ─────────────────────────────────────
    prog("Extracting tokens from localStorage...")
    tokens = await poll_tokens(page)
    if not tokens:
        hint = " (try non-headless mode to handle 2FA / captcha)" if HEADLESS else ""
        err(f"Login completed but tokens not found in localStorage{hint}")
        return

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

    done(tokens)


async def main() -> None:
    # 1. Try camoufox (anti-detect Firefox — best for Google login)
    try:
        from camoufox.async_api import AsyncCamoufox  # type: ignore
        prog("Launching Camoufox (anti-detect Firefox)...")
        async with AsyncCamoufox(headless=HEADLESS, geoip=True) as browser:
            page = await browser.new_page()
            await do_login(page)
        return
    except ImportError:
        pass
    except Exception as e:
        prog(f"Camoufox error ({e}), falling back to Playwright...")

    # 2. Fallback: playwright (chromium)
    try:
        from playwright.async_api import async_playwright  # type: ignore
        prog("camoufox not installed — using Playwright Chromium (install camoufox for better success rate)...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=HEADLESS)
            page = await browser.new_page()
            await do_login(page)
            await browser.close()
        return
    except ImportError:
        pass

    err(
        "No browser automation library found. "
        "Install camoufox: pip install 'camoufox[geoip]' && python -m camoufox fetch"
    )


asyncio.run(main())
