#!/usr/bin/env python3
"""Scrape Instagram followers with Playwright browser automation, then enrich with Exa Answer API.

Usage:
    python3 scrape_and_enrich_ig.py <target_account> --ig-user <your_ig_username> --ig-pass <your_ig_password>
    python3 scrape_and_enrich_ig.py <target_account> --ig-user <email> --ig-pass <pass> --limit 5 --skip-enrich

Requires: pip install playwright && python -m playwright install chromium
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.request

# Flush print output immediately (for background runs)
print = lambda *a, **k: __builtins__.__dict__["print"](*a, **k, flush=True)  # noqa: A001

# ---------------------------------------------------------------------------
# Exa config
# ---------------------------------------------------------------------------

EXA_API_KEY = os.environ.get("EXA_API_KEY")

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "linkedin": {
            "type": "string",
            "description": "LinkedIn profile URL. Empty string if not found.",
        },
        "email": {
            "type": "string",
            "description": "Email address. Empty string if not found.",
        },
        "phone_number": {
            "type": "string",
            "description": "Phone number with country code. Empty string if not found.",
        },
        "role": {
            "type": "string",
            "description": "Current job title or role. Empty string if not found.",
        },
        "company": {
            "type": "string",
            "description": "Current company or organization. Empty string if not found.",
        },
    },
    "required": ["linkedin", "email", "phone_number", "role", "company"],
}

EMPTY_ENRICHMENT = {"linkedin": "", "email": "", "phone_number": "", "role": "", "company": ""}

# ---------------------------------------------------------------------------
# Step 1 — Scrape followers with Playwright
# ---------------------------------------------------------------------------


def scrape_followers(target: str, ig_user: str, ig_pass: str, limit: int | None) -> list[dict]:
    """Scrape followers using Playwright browser automation."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Error: playwright not installed.")
        print("Run: pip install playwright && python -m playwright install chromium")
        sys.exit(1)

    followers = []
    seen_usernames = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page = context.new_page()

        # --- Login ---
        print("Logging into Instagram...")
        page.goto("https://www.instagram.com/accounts/login/", wait_until="domcontentloaded")
        time.sleep(5)

        # Accept cookies if banner appears
        try:
            page.get_by_role("button", name="Allow all cookies").click(timeout=3000)
            time.sleep(1)
        except Exception:
            try:
                page.get_by_role("button", name="Accept").click(timeout=2000)
                time.sleep(1)
            except Exception:
                pass

        # Wait for login form to appear
        page.get_by_role("textbox", name="Mobile number, username or").wait_for(timeout=15000)

        # Fill login form using role-based selectors (from snapshot)
        page.get_by_role("textbox", name="Mobile number, username or").fill(ig_user)
        page.get_by_role("textbox", name="Password").fill(ig_pass)
        page.get_by_role("button", name="Log in", exact=True).click()
        print("Submitted login...")

        # Wait for navigation away from login page (flexible — may go to onetap, feed, or challenge)
        page.wait_for_function(
            """() => !window.location.pathname.includes('/accounts/login')""",
            timeout=30000,
        )
        print(f"Login successful! (redirected to {page.url})")

        # Dismiss "Save Login Info" dialog
        try:
            page.get_by_role("button", name="Not now").click(timeout=5000)
        except Exception:
            try:
                page.get_by_role("button", name="Not Now").click(timeout=3000)
            except Exception:
                pass
        time.sleep(2)

        # Dismiss "Turn on Notifications" if it appears
        try:
            page.get_by_role("button", name="Not Now").click(timeout=3000)
        except Exception:
            pass

        # --- Navigate to target profile ---
        print(f"Navigating to @{target}...")
        page.goto(f"https://www.instagram.com/{target}/", wait_until="domcontentloaded", timeout=60000)
        time.sleep(3)

        # --- Set up network response listener to capture follower data ---
        target_count = limit or 999999
        captured_queue = []

        def on_response(response):
            """Capture follower data from GraphQL/API responses."""
            try:
                url = response.url
                if "graphql" not in url and "friendships" not in url and "api/v1" not in url:
                    return
                if response.status != 200:
                    return
                body = response.json()
                _extract_users(body, captured_queue)
            except Exception:
                pass

        def _extract_users(obj, queue):
            """Recursively extract user objects from API response."""
            if not obj or not isinstance(obj, (dict, list)):
                return
            if isinstance(obj, list):
                for item in obj:
                    _extract_users(item, queue)
                return
            # A user node has "username" key with a string value
            if "username" in obj and isinstance(obj["username"], str) and len(obj["username"]) > 0:
                queue.append({
                    "username": obj["username"],
                    "full_name": obj.get("full_name", ""),
                    "is_verified": obj.get("is_verified", False),
                })
            for val in obj.values():
                if isinstance(val, (dict, list)):
                    _extract_users(val, queue)

        page.on("response", on_response)
        print("Network interception ready.")

        # --- Get target user ID for API calls ---
        print("Getting target user ID...")
        user_id = page.evaluate(f"""
            async () => {{
                try {{
                    const csrfToken = document.cookie.split('; ')
                        .find(row => row.startsWith('csrftoken='))
                        ?.split('=')[1] || '';
                    const resp = await fetch('https://www.instagram.com/api/v1/users/web_profile_info/?username={target}', {{
                        headers: {{
                            'X-IG-App-ID': '936619743392459',
                            'X-CSRFToken': csrfToken,
                            'X-Requested-With': 'XMLHttpRequest',
                        }},
                        credentials: 'include',
                    }});
                    const data = await resp.json();
                    return data?.data?.user?.id || null;
                }} catch (e) {{
                    return null;
                }}
            }}
        """)
        if user_id:
            print(f"Target user ID: {user_id}")
        else:
            print("Warning: Could not get user ID, falling back to dialog scroll")

        # --- Try API-based follower fetching first ---
        if user_id:
            print(f"Fetching followers via API (limit: {limit or 'all'})...")
            max_id = ""
            api_stall = 0
            while len(followers) < target_count:
                try:
                    url = f"https://www.instagram.com/api/v1/friendships/{user_id}/followers/?count=50&max_id={max_id}"
                    api_result = page.evaluate(f"""
                        async () => {{
                            try {{
                                const csrfToken = document.cookie.split('; ')
                                    .find(row => row.startsWith('csrftoken='))
                                    ?.split('=')[1] || '';
                                const resp = await fetch('{url}', {{
                                    headers: {{
                                        'X-IG-App-ID': '936619743392459',
                                        'X-CSRFToken': csrfToken,
                                        'X-Requested-With': 'XMLHttpRequest',
                                    }},
                                    credentials: 'include',
                                }});
                                if (!resp.ok) return {{ error: resp.status }};
                                const text = await resp.text();
                                try {{
                                    return JSON.parse(text);
                                }} catch (e) {{
                                    return {{ error: 'Invalid JSON: ' + text.substring(0, 100) }};
                                }}
                            }} catch (e) {{
                                return {{ error: e.message }};
                            }}
                        }}
                    """)

                    if not api_result or "error" in api_result:
                        err = api_result.get("error", "unknown") if api_result else "null response"
                        print(f"    API error: {err}")
                        api_stall += 1
                        if api_stall >= 5:
                            print("    Too many API errors, falling back to dialog scroll...")
                            break
                        time.sleep(10)
                        continue

                    users = api_result.get("users", [])
                    if not users:
                        print("    No users in response, may have reached the end.")
                        break

                    for u in users:
                        username = u.get("username", "")
                        if not username or username in seen_usernames or username == target:
                            continue
                        seen_usernames.add(username)
                        followers.append({
                            "username": username,
                            "full_name": u.get("full_name", ""),
                            "biography": "",
                            "external_url": "",
                            "followers_count": "",
                            "is_verified": u.get("is_verified", False),
                            "is_business_account": False,
                        })

                    next_max_id = api_result.get("next_max_id", "")
                    big_list = api_result.get("big_list", False)

                    if len(followers) % 500 < 50:
                        print(f"  ... {len(followers)} collected via API")

                    if len(followers) % 2000 < 50:
                        # Save progress
                        progress_path = f"{target}_followers_progress.csv"
                        try:
                            with open(progress_path, "w", newline="", encoding="utf-8") as f_out:
                                writer = csv.DictWriter(f_out, fieldnames=FOLLOWER_COLS, extrasaction="ignore")
                                writer.writeheader()
                                writer.writerows(followers)
                            print(f"  [progress saved: {len(followers)} -> {progress_path}]")
                        except Exception:
                            pass

                    if not next_max_id or not big_list:
                        print(f"  API pagination ended (big_list={big_list})")
                        break

                    max_id = next_max_id
                    api_stall = 0
                    time.sleep(1.5)  # Be gentle with rate limiting

                except Exception as e:
                    print(f"    API fetch exception: {e}")
                    api_stall += 1
                    if api_stall >= 5:
                        break
                    time.sleep(10)

            print(f"  API collection done: {len(followers)} followers")

            if len(followers) >= target_count or len(followers) > 1000:
                # Got a decent amount via API, skip dialog scroll
                browser.close()
                print(f"\nScraped {len(followers)} followers")
                return followers

        # --- Fallback: Open followers dialog and scroll ---
        print("Opening followers list...")
        page.get_by_role("link", name="followers").click()
        time.sleep(4)

        print(f"Collecting followers via dialog scroll (limit: {limit or 'all'})...")

        # Find the scrollable container inside the dialog via JS
        # and cache a reference to it for efficient scrolling
        _find_scroller_js = """
            () => {
                const dialog = document.querySelector("div[role='dialog']");
                if (!dialog) return null;
                // Find the deepest scrollable div inside the dialog
                let best = null;
                let bestHeight = 0;
                const divs = dialog.querySelectorAll("div");
                for (const div of divs) {
                    const overflow = window.getComputedStyle(div).overflowY;
                    if ((overflow === 'auto' || overflow === 'scroll' || overflow === 'hidden')
                        && div.scrollHeight > div.clientHeight + 10) {
                        if (div.scrollHeight > bestHeight) {
                            bestHeight = div.scrollHeight;
                            best = div;
                        }
                    }
                }
                if (best) {
                    best.setAttribute('data-follower-scroller', 'true');
                    return { scrollHeight: best.scrollHeight, clientHeight: best.clientHeight, scrollTop: best.scrollTop };
                }
                return null;
            }
        """

        _scroll_js = """
            (amount) => {
                const el = document.querySelector("[data-follower-scroller='true']");
                if (el) {
                    el.scrollTop += amount;
                    return { scrollTop: el.scrollTop, scrollHeight: el.scrollHeight, clientHeight: el.clientHeight };
                }
                // Fallback: scroll any scrollable div in dialog
                const dialog = document.querySelector("div[role='dialog']");
                if (!dialog) return null;
                const divs = dialog.querySelectorAll("div");
                for (const div of divs) {
                    if (div.scrollHeight > div.clientHeight + 50) {
                        div.scrollTop += amount;
                        return { scrollTop: div.scrollTop, scrollHeight: div.scrollHeight, clientHeight: div.clientHeight };
                    }
                }
                return null;
            }
        """

        # Initialize scroller reference
        scroller_info = page.evaluate(_find_scroller_js)
        if scroller_info:
            print(f"  Scroller found: height={scroller_info['scrollHeight']}, viewport={scroller_info['clientHeight']}")
        else:
            print("  Warning: Could not find scrollable container, will use mouse wheel fallback")

        no_new_count = 0
        max_no_new = 50  # More tolerance for large accounts
        save_interval = 500  # Save progress every 500 followers
        last_save_count = 0
        recovery_attempts = 0
        max_recovery_attempts = 3  # Max times to try close/reopen recovery

        def _process_queue():
            """Process captured network data into followers list."""
            added = 0
            while captured_queue:
                entry = captured_queue.pop(0)
                username = entry.get("username", "")
                if not username or username in seen_usernames or username == target:
                    continue
                seen_usernames.add(username)
                followers.append({
                    "username": username,
                    "full_name": entry.get("full_name", ""),
                    "biography": "",
                    "external_url": "",
                    "followers_count": "",
                    "is_verified": entry.get("is_verified", False),
                    "is_business_account": False,
                })
                added += 1
                if len(followers) % 1000 == 0:
                    print(f"  [{len(followers)}] @{username} — {entry.get('full_name', '')}")
            return added

        def _scan_dom():
            """Scan DOM for usernames as fallback."""
            added = 0
            try:
                dom_entries = page.evaluate("""
                    () => {
                        const dialog = document.querySelector("div[role='dialog']");
                        if (!dialog) return [];
                        const results = [];
                        const links = dialog.querySelectorAll("a[href^='/']");
                        for (const link of links) {
                            const href = link.getAttribute("href") || "";
                            if (!href.match(/^\\/[a-zA-Z0-9._]+\\/$/)) continue;
                            const username = href.replace(/\\//g, "");
                            const skip = ["explore", "reels", "direct", "accounts", "p", "stories"];
                            if (skip.includes(username)) continue;
                            results.push({ username });
                        }
                        return results;
                    }
                """)
                for entry in dom_entries:
                    username = entry["username"]
                    if username in seen_usernames or username == target:
                        continue
                    seen_usernames.add(username)
                    followers.append({
                        "username": username, "full_name": "", "biography": "",
                        "external_url": "", "followers_count": "",
                        "is_verified": False, "is_business_account": False,
                    })
                    added += 1
            except Exception:
                pass
            return added

        while len(followers) < target_count and no_new_count < max_no_new:
            prev_count = len(followers)

            _process_queue()
            _scan_dom()

            if len(followers) > prev_count:
                no_new_count = 0
                print(f"  ... {len(followers)} collected (+{len(followers) - prev_count})")
            else:
                no_new_count += 1
                if no_new_count % 5 == 0:
                    print(f"    (stalled at {len(followers)}, attempt {no_new_count}/{max_no_new})")
                    # On stall, try mouse wheel as additional trigger
                    try:
                        dialog_el = page.locator("div[role='dialog']")
                        dialog_el.hover()
                        page.mouse.wheel(0, 3000)
                    except Exception:
                        pass
                    time.sleep(5)

                if no_new_count == 20 and recovery_attempts < max_recovery_attempts:
                    # Major stall recovery: close and reopen the followers dialog
                    recovery_attempts += 1
                    print(f"    Attempting stall recovery #{recovery_attempts}: close/reopen followers dialog...")
                    try:
                        page.keyboard.press("Escape")
                        time.sleep(2)
                        page.get_by_role("link", name="followers").click()
                        time.sleep(4)
                        # Re-find scroller
                        scroller_info = page.evaluate(_find_scroller_js)
                        if scroller_info:
                            # Scroll back to bottom quickly
                            page.evaluate("""
                                () => {
                                    const el = document.querySelector("[data-follower-scroller='true']");
                                    if (el) el.scrollTop = el.scrollHeight;
                                }
                            """)
                            time.sleep(3)
                        no_new_count = 10  # Give it more chances after recovery
                    except Exception as e:
                        print(f"    Recovery failed: {e}")
                elif no_new_count == 30 and recovery_attempts < max_recovery_attempts:
                    # Second-chance recovery at a higher stall count
                    recovery_attempts += 1
                    print(f"    Late recovery attempt #{recovery_attempts}: navigate away and back...")
                    try:
                        # Navigate to feed and come back to trigger fresh state
                        page.goto(f"https://www.instagram.com/{target}/", wait_until="domcontentloaded", timeout=30000)
                        time.sleep(3)
                        page.get_by_role("link", name="followers").click()
                        time.sleep(4)
                        scroller_info = page.evaluate(_find_scroller_js)
                        if scroller_info:
                            page.evaluate("""
                                () => {
                                    const el = document.querySelector("[data-follower-scroller='true']");
                                    if (el) el.scrollTop = el.scrollHeight;
                                }
                            """)
                            time.sleep(3)
                        no_new_count = 20  # Reset partially
                    except Exception as e:
                        print(f"    Late recovery failed: {e}")

            if len(followers) >= target_count:
                break

            # Save progress periodically
            if len(followers) - last_save_count >= save_interval:
                progress_path = f"{target}_followers_progress.csv"
                try:
                    with open(progress_path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=FOLLOWER_COLS, extrasaction="ignore")
                        writer.writeheader()
                        writer.writerows(followers)
                    print(f"  [progress saved: {len(followers)} -> {progress_path}]")
                    last_save_count = len(followers)
                except Exception:
                    pass

            # Scroll: use JS scroll on the identified container + mouse wheel combo
            try:
                scroll_result = page.evaluate(_scroll_js, 800)
                if scroll_result:
                    at_bottom = scroll_result["scrollTop"] + scroll_result["clientHeight"] >= scroll_result["scrollHeight"] - 5
                    if at_bottom:
                        # At the bottom — wait for more content to load
                        time.sleep(3)
                        # Small reverse scroll + re-scroll to trigger loading
                        page.evaluate(_scroll_js, -200)
                        time.sleep(0.5)
                        page.evaluate(_scroll_js, 400)
                        time.sleep(2)
                    else:
                        time.sleep(1.5)
                else:
                    # Fallback to mouse wheel
                    dialog_el = page.locator("div[role='dialog']")
                    dialog_el.hover()
                    page.mouse.wheel(0, 1500)
                    time.sleep(2)
            except Exception:
                try:
                    dialog_el = page.locator("div[role='dialog']")
                    dialog_el.hover()
                    page.mouse.wheel(0, 1500)
                except Exception:
                    pass
                time.sleep(2)

        browser.close()

    print(f"\nScraped {len(followers)} followers")
    return followers


# ---------------------------------------------------------------------------
# Step 2 — Enrich via Exa
# ---------------------------------------------------------------------------


def enrich_follower(follower: dict) -> dict:
    """Use Exa Answer API to find LinkedIn, email, phone for a follower."""

    name = follower["full_name"] or follower["username"]

    lines = [f"Find the LinkedIn profile, email address, and phone number for {name}."]
    lines.append(f"Their Instagram is @{follower['username']}.")

    if follower["biography"]:
        lines.append(f"Bio: {follower['biography']}.")
    if follower["external_url"]:
        lines.append(f"Website: {follower['external_url']}.")
    if follower.get("is_business_account"):
        lines.append("This is a business account.")

    lines.append(
        "Search LinkedIn, personal websites, company pages, Crunchbase, AngelList, "
        "public directories, and any public records. "
        "Return their LinkedIn URL, email, phone number, job title/role, and company."
    )

    query = " ".join(lines)

    body = json.dumps({
        "query": query,
        "text": True,
        "outputSchema": OUTPUT_SCHEMA,
    }).encode()

    req = urllib.request.Request(
        "https://api.exa.ai/answer",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "ig-enrich/1.0",
            "x-api-key": EXA_API_KEY,
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        print(f"    API error ({e.code}): {err_body[:200]}")
        return EMPTY_ENRICHMENT.copy()
    except Exception as e:
        print(f"    Request failed: {e}")
        return EMPTY_ENRICHMENT.copy()

    answer = data.get("answer", {})

    if isinstance(answer, str):
        try:
            answer = json.loads(answer)
        except (json.JSONDecodeError, TypeError):
            return EMPTY_ENRICHMENT.copy()

    return {
        "linkedin": answer.get("linkedin", ""),
        "email": answer.get("email", ""),
        "phone_number": answer.get("phone_number", ""),
        "role": answer.get("role", ""),
        "company": answer.get("company", ""),
    }


# ---------------------------------------------------------------------------
# Step 3 — Save CSV
# ---------------------------------------------------------------------------

FOLLOWER_COLS = ["username", "full_name", "biography", "external_url", "followers_count", "is_verified", "is_business_account"]
ENRICH_COLS = ["linkedin", "email", "phone_number", "role", "company"]
ALL_COLS = FOLLOWER_COLS + ENRICH_COLS


def save_csv(followers: list[dict], path: str):
    """Save followers to CSV."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ALL_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(followers)
    print(f"Saved -> {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Scrape Instagram followers and enrich with Exa")
    parser.add_argument("target", help="Target Instagram account to scrape followers from")
    parser.add_argument("--ig-user", required=True, help="Your Instagram username/email for login")
    parser.add_argument("--ig-pass", required=True, help="Your Instagram password")
    parser.add_argument("--limit", type=int, default=None, help="Only scrape first N followers")
    parser.add_argument("--skip-enrich", action="store_true", help="Skip Exa enrichment, just scrape")
    parser.add_argument("--output", default=None, help="Output CSV path")
    parser.add_argument("--resume", default=None, help="Resume from a previous progress CSV (merge new followers)")
    args = parser.parse_args()

    output_path = args.output or f"{args.target}_followers.csv"

    # --- Scrape ---
    followers = scrape_followers(args.target, args.ig_user, args.ig_pass, args.limit)

    # Merge with previous progress CSV if --resume is given
    if args.resume and os.path.exists(args.resume):
        print(f"\nMerging with previous data from {args.resume}...")
        with open(args.resume, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing = list(reader)
        existing_usernames = {r["username"] for r in existing}
        new_count = 0
        for f_row in followers:
            if f_row["username"] not in existing_usernames:
                existing.append(f_row)
                new_count += 1
        followers = existing
        print(f"Merged: {len(existing) - new_count} existing + {new_count} new = {len(followers)} total")

    if not followers:
        print("No followers found.")
        return

    # Save raw follower data immediately
    raw_path = output_path.replace(".csv", "_raw.csv")
    save_csv(followers, raw_path)

    if args.skip_enrich:
        save_csv(followers, output_path)
        print(f"\nDone! {len(followers)} followers scraped (enrichment skipped).")
        return

    # --- Enrich ---
    if not EXA_API_KEY:
        print("\nWarning: EXA_API_KEY not set. Skipping enrichment.")
        save_csv(followers, output_path)
        return

    print(f"\nEnriching {len(followers)} followers via Exa...\n")

    enriched_count = 0
    for i, follower in enumerate(followers):
        name = follower.get("full_name") or follower["username"]
        label = f"@{follower['username']}"
        if follower["full_name"]:
            label += f" ({follower['full_name']})"
        print(f"[{i + 1}/{len(followers)}] {label}")

        result = enrich_follower(follower)
        follower.update(result)

        found_items = []
        if result["linkedin"]:
            found_items.append(f"LI: {result['linkedin']}")
        if result["email"]:
            found_items.append(f"email: {result['email']}")
        if result["phone_number"]:
            found_items.append(f"phone: {result['phone_number']}")
        if result["role"] or result["company"]:
            role_str = " ".join(filter(None, [result["role"], f"@ {result['company']}" if result["company"] else ""]))
            found_items.append(role_str)

        if found_items:
            enriched_count += 1
            print(f"    {' | '.join(found_items)}")
        else:
            print(f"    no data found")

        if i < len(followers) - 1:
            time.sleep(0.75)

    # Save enriched CSV
    save_csv(followers, output_path)

    print(f"\nDone! {enriched_count}/{len(followers)} followers enriched.")
    print(f"Raw followers -> {raw_path}")
    print(f"Enriched CSV  -> {output_path}")


if __name__ == "__main__":
    main()
