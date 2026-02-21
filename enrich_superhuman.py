#!/usr/bin/env python3
"""Enrich Superhuman user CSV with phone numbers using Exa Answer API.

Parses existing data (email, bio, LinkedIn, location) from the CSV
and uses it as context for each Exa query to maximize hit rate.
"""

import csv
import json
import os
import sys
import time
import urllib.request

EXA_API_KEY = os.environ.get("EXA_API_KEY")
if not EXA_API_KEY:
    print("Error: EXA_API_KEY environment variable not set")
    sys.exit(1)

INPUT_CSV = sys.argv[1] if len(sys.argv) > 1 else "us_superhuman_users_final_comprehensive.csv"
OUTPUT_CSV = sys.argv[2] if len(sys.argv) > 2 else INPUT_CSV.replace(".csv", "_enriched.csv")
LIMIT = int(sys.argv[3]) if len(sys.argv) > 3 else None  # Optional: only process first N rows

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "phone_number": {
            "type": "string",
            "description": "Phone number with country code. Empty string if not found.",
        },
        "phone_source": {
            "type": "string",
            "description": "URL or name of site where phone was found.",
        },
        "personal_email": {
            "type": "string",
            "description": "Personal or alternate email if found. Empty string if not found.",
        },
        "twitter": {
            "type": "string",
            "description": "Twitter/X handle or URL. Empty string if not found.",
        },
        "website": {
            "type": "string",
            "description": "Personal website URL. Empty string if not found.",
        },
    },
    "required": ["phone_number", "phone_source", "personal_email", "twitter", "website"],
}


def parse_raw(row: dict) -> dict:
    """Extract useful context from the raw_response JSON."""
    ctx = {
        "name": row.get("name", "").strip(),
        "email": row.get("email", "").strip(),
        "bio": row.get("bio", "").strip(),
        "location": row.get("location", "").strip(),
        "linkedin": "",
        "links": [],
        "twitter": "",
    }

    raw = row.get("raw_response", "").strip()
    if not raw:
        return ctx

    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return ctx

    for link in data.get("links", []):
        url = link.get("url", "")
        title = link.get("title", "")
        if "linkedin.com" in url:
            ctx["linkedin"] = url
        elif "twitter.com" in url or title.startswith("@"):
            ctx["twitter"] = url
        else:
            ctx["links"].append(url)

    if data.get("twitterHandle"):
        ctx["twitter"] = f"@{data['twitterHandle']}"

    return ctx


def find_phone(ctx: dict) -> dict:
    """Use Exa Answer API to find a person's phone number with full context."""

    # Build a rich context query
    lines = [f"Find the phone number for {ctx['name']}."]

    if ctx["bio"]:
        lines.append(f"They are {ctx['bio']}.")
    if ctx["location"]:
        lines.append(f"Located in {ctx['location']}.")
    if ctx["email"]:
        lines.append(f"Their email is {ctx['email']}.")
    if ctx["linkedin"]:
        lines.append(f"Their LinkedIn is {ctx['linkedin']}.")
    if ctx["twitter"]:
        lines.append(f"Their Twitter is {ctx['twitter']}.")
    if ctx["links"]:
        lines.append(f"Other profiles: {', '.join(ctx['links'])}.")

    lines.append(
        "Search public directories, personal websites, contact pages, "
        "Crunchbase, AngelList, company about pages, and any public records. "
        "Return their phone number, any alternate email, Twitter, and personal website."
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
            "User-Agent": "exa-enrich/1.0",
            "x-api-key": EXA_API_KEY,
        },
    )

    empty = {"phone_number": "", "phone_source": "", "personal_email": "", "twitter": "", "website": ""}

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        print(f"    API error ({e.code}): {err_body[:200]}")
        return empty
    except Exception as e:
        print(f"    Request failed: {e}")
        return empty

    answer = data.get("answer", {})

    if isinstance(answer, str):
        try:
            answer = json.loads(answer)
        except (json.JSONDecodeError, TypeError):
            return empty

    return {
        "phone_number": answer.get("phone_number", ""),
        "phone_source": answer.get("phone_source", ""),
        "personal_email": answer.get("personal_email", ""),
        "twitter": answer.get("twitter", ""),
        "website": answer.get("website", ""),
    }


def main():
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        rows = list(reader)

    if LIMIT:
        rows = rows[:LIMIT]

    # Add enrichment columns
    enrich_cols = ["phone_number", "phone_source", "personal_email", "found_twitter", "found_website"]
    for col in enrich_cols:
        if col not in fieldnames:
            fieldnames.append(col)

    print(f"Processing {len(rows)} people from {INPUT_CSV}...\n")

    found = 0
    for i, row in enumerate(rows):
        ctx = parse_raw(row)
        name = ctx["name"]
        if not name:
            continue

        label = name
        if ctx["bio"]:
            label += f" — {ctx['bio']}"
        elif ctx["location"]:
            label += f" — {ctx['location']}"
        print(f"[{i + 1}/{len(rows)}] {label}")

        result = find_phone(ctx)
        row["phone_number"] = result["phone_number"]
        row["phone_source"] = result["phone_source"]
        row["personal_email"] = result["personal_email"]
        row["found_twitter"] = result["twitter"]
        row["found_website"] = result["website"]

        if result["phone_number"]:
            found += 1
            print(f"    PHONE: {result['phone_number']} (via {result['phone_source']})")
        else:
            print(f"    no phone")

        extras = []
        if result["personal_email"]:
            extras.append(f"email: {result['personal_email']}")
        if result["twitter"]:
            extras.append(f"twitter: {result['twitter']}")
        if result["website"]:
            extras.append(f"web: {result['website']}")
        if extras:
            print(f"    {' | '.join(extras)}")

        # Rate limit
        if i < len(rows) - 1:
            time.sleep(0.5)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone! {found}/{len(rows)} phone numbers found.")
    print(f"Enriched CSV -> {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
