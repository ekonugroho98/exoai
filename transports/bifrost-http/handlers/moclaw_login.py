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
    except Exception as e:
        err(f"Email input not found: {e}")
        return False

    # Password
    prog("Entering Google password...")
    try:
        await login_page.wait_for_selector('input[type="password"]', timeout=15000)
        await login_page.wait_for_timeout(500)
        await login_page.fill('input[type="password"]', PASSWORD)
        await login_page.keyboard.press("Enter")
    except Exception as e:
        err(f"Password field not found (2FA required? wrong credentials?): {e}")
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

    login_page = page  # default: same-page flow
    popup_detected = False

    try:
        # Listen for a popup BEFORE clicking so we don't miss it
        async with page.expect_popup(timeout=8000) as popup_info:
            for selector in _GOOGLE_SELECTORS:
                try:
                    await page.locator(selector).first.click(timeout=3000)
                    break
                except Exception:
                    continue
        popup = await popup_info.value
        await popup.wait_for_load_state("domcontentloaded")
        login_page = popup
        popup_detected = True
        prog("Google login opened in popup window")
    except Exception:
        # No popup within timeout — Google redirected in the same tab
        if not popup_detected:
            for selector in _GOOGLE_SELECTORS:
                try:
                    if await page.locator(selector).first.is_visible(timeout=2000):
                        await page.locator(selector).first.click()
                        break
                except Exception:
                    continue
        login_page = page
        prog("Google login in same tab")

    # ── Fill credentials in the correct page context ──────────────────────────
    if not await fill_google_credentials(login_page):
        return

    # ── Handle "I understand" interstitial (Workspace new accounts) ───────────
    try:
        selector = ", ".join(f"button:has-text('{t}')" for t in _UNDERSTAND_TEXTS)
        btn = login_page.locator(selector).first
        if await btn.is_visible(timeout=3000):
            prog("Clicking 'I understand'...")
            await btn.click()
            await login_page.wait_for_timeout(1000)
    except Exception:
        pass

    # ── Handle "Continue/Lanjutkan" OAuth consent screen ─────────────────────
    try:
        selector = ", ".join(f"button:has-text('{t}')" for t in _CONTINUE_TEXTS)
        btn = login_page.locator(selector).first
        if await btn.is_visible(timeout=4000):
            prog("Clicking Continue on Google consent screen...")
            await btn.click()
            await login_page.wait_for_timeout(1000)
    except Exception:
        pass

    # ── Wait for moclaw.ai to load (main page may redirect while popup closes)
    prog("Waiting for moclaw.ai to load...")
    try:
        await page.wait_for_url("*moclaw.ai*", timeout=30000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)

    # ── "Start Free Trial" modal (new accounts, slow to appear) ──────────────
    try:
        btn = page.locator("button:has-text('Start Free Trial')").first
        await btn.wait_for(state="visible", timeout=15000)
        prog("Clicking Start Free Trial...")
        await btn.click()
        await page.wait_for_timeout(1500)
    except Exception:
        pass

    # ── "Trial started! → Get started" confirmation modal ────────────────────
    try:
        btn = page.locator("button:has-text('Get started')").first
        if await btn.is_visible(timeout=5000):
            prog("Trial started! Clicking Get started...")
            await btn.click()
            await page.wait_for_timeout(1000)
    except Exception:
        pass

    # ── Extract tokens from localStorage ─────────────────────────────────────
    prog("Extracting tokens from localStorage...")
    tokens = await poll_tokens(page)
    if tokens:
        done(tokens)
    else:
        hint = " (try non-headless mode to handle 2FA / captcha)" if HEADLESS else ""
        err(f"Login completed but tokens not found in localStorage{hint}")


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
