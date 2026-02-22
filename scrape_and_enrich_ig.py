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
        time.sleep(3)

        # Fill login form using role-based selectors (from snapshot)
        page.get_by_role("textbox", name="Mobile number, username or").fill(ig_user)
        page.get_by_role("textbox", name="Password").fill(ig_pass)
        page.get_by_role("button", name="Log in", exact=True).click()
        print("Submitted login...")

        # Wait for navigation
        page.wait_for_url("**/accounts/onetap/**", timeout=15000)
        print("Login successful!")

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

        # --- Open followers dialog ---
        print("Opening followers list...")
        page.get_by_role("link", name="followers").click()
        time.sleep(3)

        # --- Scroll and collect followers from dialog ---
        target_count = limit or 999999
        print(f"Collecting followers (limit: {limit or 'all'})...")

        no_new_count = 0
        max_no_new = 8

        while len(followers) < target_count and no_new_count < max_no_new:
            # Extract follower entries from the dialog
            # Each follower entry has a link with href="/username/" and nearby text with display name
            entries = page.evaluate("""
                () => {
                    const dialog = document.querySelector("div[role='dialog']");
                    if (!dialog) return [];

                    const results = [];
                    // Find all links inside the dialog that point to user profiles
                    const links = dialog.querySelectorAll("a[href^='/']");
                    const seen = new Set();

                    for (const link of links) {
                        const href = link.getAttribute("href") || "";
                        // Profile links are like /username/ (exactly two slashes)
                        if (!href.match(/^\/[a-zA-Z0-9._]+\/$/)) continue;

                        const username = href.replace(/\//g, "");

                        // Skip non-profile paths
                        const skip = ["explore", "reels", "direct", "accounts", "p", "stories"];
                        if (skip.includes(username)) continue;
                        if (seen.has(username)) continue;
                        seen.add(username);

                        // Get display name: look for text near this link
                        // The username link's text content is the username itself
                        // The display name is typically in a sibling or parent's child element
                        let fullName = "";
                        const parent = link.closest("div[class]");
                        if (parent) {
                            // Find the container that has both username and full name
                            const container = parent.parentElement;
                            if (container) {
                                const spans = container.querySelectorAll("span");
                                for (const span of spans) {
                                    const text = span.textContent.trim();
                                    if (text && text !== username && !text.startsWith("Follow")
                                        && text !== "Verified" && text.length > 0
                                        && !text.includes("profile picture")) {
                                        fullName = text;
                                        break;
                                    }
                                }
                            }
                        }

                        results.push({ username, fullName });
                    }
                    return results;
                }
            """)

            prev_count = len(followers)

            for entry in entries:
                if len(followers) >= target_count:
                    break

                username = entry["username"]
                if username in seen_usernames or username == target:
                    continue

                seen_usernames.add(username)
                data = {
                    "username": username,
                    "full_name": entry.get("fullName", ""),
                    "biography": "",
                    "external_url": "",
                    "followers_count": "",
                    "is_verified": False,
                    "is_business_account": False,
                }
                followers.append(data)
                print(f"  [{len(followers)}] @{username} — {data['full_name']}")

            if len(followers) == prev_count:
                no_new_count += 1
            else:
                no_new_count = 0

            if len(followers) >= target_count:
                break

            # Scroll down inside the dialog
            page.evaluate("""
                () => {
                    const dialog = document.querySelector("div[role='dialog']");
                    if (!dialog) return;
                    // Find the scrollable div inside dialog
                    const divs = dialog.querySelectorAll("div");
                    for (const div of divs) {
                        if (div.scrollHeight > div.clientHeight + 50) {
                            div.scrollTop = div.scrollHeight;
                            return true;
                        }
                    }
                    return false;
                }
            """)
            time.sleep(2.5)

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
    args = parser.parse_args()

    output_path = args.output or f"{args.target}_followers.csv"

    # --- Scrape ---
    followers = scrape_followers(args.target, args.ig_user, args.ig_pass, args.limit)

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
