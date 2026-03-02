#!/usr/bin/env python3
"""Enrich scraped Instagram followers CSV with LinkedIn, email, phone via Exa Answer API.

Reads a raw followers CSV (from scrape_and_enrich_ig.py) and enriches each row.
Saves progress every 50 rows so it can resume if interrupted.

Usage:
    export EXA_API_KEY="..."
    python3 enrich_ig_followers.py fyxerofficial_all_followers.csv
    python3 enrich_ig_followers.py fyxerofficial_all_followers.csv --start 500  # resume from row 500
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
import urllib.request

print = lambda *a, **k: __builtins__.__dict__["print"](*a, **k, flush=True)  # noqa: A001

EXA_API_KEY = os.environ.get("EXA_API_KEY")
if not EXA_API_KEY:
    print("Error: EXA_API_KEY not set")
    sys.exit(1)

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "linkedin": {"type": "string", "description": "LinkedIn profile URL. Empty string if not found."},
        "email": {"type": "string", "description": "Email address. Empty string if not found."},
        "phone_number": {"type": "string", "description": "Phone number with country code. Empty string if not found."},
        "role": {"type": "string", "description": "Current job title or role. Empty string if not found."},
        "company": {"type": "string", "description": "Current company or organization. Empty string if not found."},
    },
    "required": ["linkedin", "email", "phone_number", "role", "company"],
}

EMPTY = {"linkedin": "", "email": "", "phone_number": "", "role": "", "company": ""}


def enrich(username: str, full_name: str = "", bio: str = "", url: str = "") -> dict:
    name = full_name or username
    lines = [f"Find the LinkedIn profile, email address, and phone number for {name}."]
    lines.append(f"Their Instagram is @{username}.")
    if bio:
        lines.append(f"Bio: {bio}.")
    if url:
        lines.append(f"Website: {url}.")
    lines.append(
        "Search LinkedIn, personal websites, company pages, Crunchbase, AngelList, "
        "public directories, and any public records. "
        "Return their LinkedIn URL, email, phone number, job title/role, and company."
    )
    query = " ".join(lines)

    body = json.dumps({"query": query, "text": True, "outputSchema": OUTPUT_SCHEMA}).encode()
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
        return EMPTY.copy()
    except Exception as e:
        print(f"    Request failed: {e}")
        return EMPTY.copy()

    answer = data.get("answer", {})
    if isinstance(answer, str):
        try:
            answer = json.loads(answer)
        except (json.JSONDecodeError, TypeError):
            return EMPTY.copy()

    return {
        "linkedin": answer.get("linkedin", ""),
        "email": answer.get("email", ""),
        "phone_number": answer.get("phone_number", ""),
        "role": answer.get("role", ""),
        "company": answer.get("company", ""),
    }


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", help="Input CSV file")
    parser.add_argument("--start", type=int, default=0, help="Row index to start from (for resuming)")
    parser.add_argument("--output", default=None, help="Output CSV path")
    args = parser.parse_args()

    output_path = args.output or args.input_csv.replace(".csv", "_enriched.csv")

    # Read input
    with open(args.input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        in_fields = list(reader.fieldnames)
        rows = list(reader)

    # If resuming, load existing enriched data
    out_fields = list(in_fields)
    for col in ["linkedin", "email", "phone_number", "role", "company"]:
        if col not in out_fields:
            out_fields.append(col)

    if args.start > 0 and os.path.exists(output_path):
        print(f"Resuming from row {args.start}, loading existing {output_path}...")
        with open(output_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing = list(reader)
        # Merge existing enrichment into rows
        for i, ex in enumerate(existing):
            if i < len(rows):
                for col in ["linkedin", "email", "phone_number", "role", "company"]:
                    if ex.get(col):
                        rows[i][col] = ex[col]

    total = len(rows)
    start = args.start
    enriched_count = 0
    save_interval = 50

    print(f"Enriching {total} followers (starting from {start})...\n")

    for i in range(start, total):
        row = rows[i]
        username = row.get("username", "")
        full_name = row.get("full_name", "")
        label = f"@{username}"
        if full_name:
            label += f" ({full_name})"
        print(f"[{i+1}/{total}] {label}")

        result = enrich(
            username,
            full_name=full_name,
            bio=row.get("biography", ""),
            url=row.get("external_url", ""),
        )
        row.update(result)

        found = []
        if result["linkedin"]:
            found.append(f"LI: {result['linkedin']}")
        if result["email"]:
            found.append(f"email: {result['email']}")
        if result["phone_number"]:
            found.append(f"phone: {result['phone_number']}")
        if result["role"] or result["company"]:
            found.append(f"{result['role']} @ {result['company']}".strip(" @"))

        if found:
            enriched_count += 1
            print(f"    {' | '.join(found)}")
        else:
            print(f"    —")

        # Save progress periodically
        if (i + 1) % save_interval == 0 or i == total - 1:
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            print(f"  [saved progress: {i+1}/{total}]")

        time.sleep(0.75)

    print(f"\nDone! {enriched_count}/{total - start} enriched.")
    print(f"Output -> {output_path}")


if __name__ == "__main__":
    main()
