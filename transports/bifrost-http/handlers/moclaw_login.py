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


async def poll_tokens(page, tries: int = 30) -> str | None:
    """Poll localStorage every second until tokens appear or tries exhausted."""
    for _ in range(tries):
        result = await page.evaluate(EXTRACT_JS)
        if result and result != "null":
            return result
        await asyncio.sleep(1)
    return None


async def do_login(page) -> None:
    prog("Navigating to moclaw.ai...")
    await page.goto("https://moclaw.ai/", timeout=30000, wait_until="domcontentloaded")
    await page.wait_for_timeout(2500)

    # Click "Get Started"
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

    # Click "Continue with Google"
    prog("Clicking Continue with Google...")
    google_clicked = False
    for selector in [
        "text=Continue with Google",
        "button:has-text('Google')",
        "a:has-text('Google')",
        "[data-provider='google']",
        "[data-connection='google']",
        "[data-action='google']",
    ]:
        try:
            await page.locator(selector).first.click(timeout=5000)
            google_clicked = True
            break
        except Exception:
            continue
    if not google_clicked:
        err("Cannot find 'Continue with Google' button — page layout may have changed")
        return

    # Google: enter email
    prog("Entering Google email...")
    try:
        await page.wait_for_selector('input[type="email"]', timeout=15000)
        await page.fill('input[type="email"]', EMAIL)
        await page.keyboard.press("Enter")
    except Exception as e:
        err(f"Email input not found: {e}")
        return

    # Google: enter password
    prog("Entering Google password...")
    try:
        await page.wait_for_selector('input[type="password"]', timeout=15000)
        await page.wait_for_timeout(500)
        await page.fill('input[type="password"]', PASSWORD)
        await page.keyboard.press("Enter")
    except Exception as e:
        err(f"Password field not found (2FA required? wrong credentials?): {e}")
        return

    # Handle Google "Welcome to your new account" / "I understand" interstitial.
    # Only present for new/Workspace accounts — silently skip if not found.
    try:
        btn = page.locator("button:has-text('I understand'), input[value='I understand']").first
        if await btn.is_visible(timeout=2000):
            prog("Clicking 'I understand'...")
            await btn.click()
            await page.wait_for_timeout(1000)
    except Exception:
        pass  # Not present — normal for existing/personal accounts

    # Handle Google OAuth consent screen ("Sign in to auth0.com" → Continue).
    # Appears on first-time authorization for new accounts.
    try:
        btn = page.locator("button:has-text('Continue')").first
        if await btn.is_visible(timeout=3000):
            prog("Clicking Continue on Google consent screen...")
            await btn.click()
            await page.wait_for_timeout(1000)
    except Exception:
        pass  # Not present — already authorized before

    # Wait for redirect back to moclaw.ai
    prog("Waiting for redirect to moclaw.ai...")
    try:
        await page.wait_for_url("*moclaw.ai*", timeout=30000)
    except Exception:
        pass  # May already be there or may have a different URL pattern
    await page.wait_for_timeout(2000)

    # Handle "Free Trial" modal — appears for new accounts, can be slow to load.
    try:
        btn = page.locator("button:has-text('Start Free Trial')").first
        await btn.wait_for(state="visible", timeout=15000)
        prog("Clicking Start Free Trial...")
        await btn.click()
        await page.wait_for_timeout(1500)
    except Exception:
        pass  # Not present — account already has a plan

    # Handle "Trial started!" confirmation modal → click "Get started" to dismiss.
    try:
        btn = page.locator("button:has-text('Get started')").first
        if await btn.is_visible(timeout=5000):
            prog("Trial started! Clicking Get started...")
            await btn.click()
            await page.wait_for_timeout(1000)
    except Exception:
        pass

    # Extract tokens
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
