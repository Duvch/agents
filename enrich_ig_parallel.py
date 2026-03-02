#!/usr/bin/env python3
"""Parallel enrichment of Instagram followers via Exa Answer API.

Runs multiple concurrent requests to speed up enrichment.
Saves progress every 50 rows.

Usage:
    export EXA_API_KEY="..."
    python3 enrich_ig_parallel.py input.csv --start 4400 --output output.csv --workers 5
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

print = lambda *a, **k: __builtins__.__dict__["print"](*a, **k, flush=True)

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
        if "NO_MORE_CREDITS" in err_body or e.code == 402:
            print(f"    CREDITS EXHAUSTED - stopping")
            return {"_stop": True, **EMPTY}
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


def process_row(i, row):
    """Process a single row and return (index, result)."""
    username = row.get("username", "")
    full_name = row.get("full_name", "")
    result = enrich(
        username,
        full_name=full_name,
        bio=row.get("biography", ""),
        url=row.get("external_url", ""),
    )
    return i, result


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", help="Input CSV file")
    parser.add_argument("--start", type=int, default=0, help="Row index to start from")
    parser.add_argument("--output", default=None, help="Output CSV path")
    parser.add_argument("--workers", type=int, default=5, help="Number of parallel workers")
    args = parser.parse_args()

    output_path = args.output or args.input_csv.replace(".csv", "_enriched.csv")
    workers = args.workers

    # Read input
    with open(args.input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        in_fields = list(reader.fieldnames)
        rows = list(reader)

    out_fields = list(in_fields)
    for col in ["linkedin", "email", "phone_number", "role", "company"]:
        if col not in out_fields:
            out_fields.append(col)

    # Load existing enriched data if resuming
    if args.start > 0 and os.path.exists(output_path):
        print(f"Resuming from row {args.start}, loading existing {output_path}...")
        with open(output_path, newline="", encoding="utf-8") as f:
            content = f.read().replace('\x00', '')
        import io
        existing = list(csv.DictReader(io.StringIO(content)))
        for i, ex in enumerate(existing):
            if i < len(rows):
                for col in ["linkedin", "email", "phone_number", "role", "company"]:
                    if ex.get(col):
                        rows[i][col] = ex[col]

    total = len(rows)
    start = args.start
    enriched_count = 0
    save_interval = 50
    credits_exhausted = False

    print(f"Enriching {total} followers (starting from {start}, {workers} workers)...\n")

    # Process in batches
    batch_size = workers
    i = start
    while i < total and not credits_exhausted:
        batch_end = min(i + batch_size, total)
        batch_indices = list(range(i, batch_end))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for idx in batch_indices:
                row = rows[idx]
                future = executor.submit(process_row, idx, row)
                futures[future] = idx

            for future in as_completed(futures):
                idx, result = future.result()
                if result.get("_stop"):
                    credits_exhausted = True
                    result.pop("_stop", None)

                row = rows[idx]
                username = row.get("username", "")
                full_name = row.get("full_name", "")
                label = f"@{username}"
                if full_name:
                    label += f" ({full_name})"

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
                    print(f"[{idx+1}/{total}] {label}")
                    print(f"    {' | '.join(found)}")
                else:
                    print(f"[{idx+1}/{total}] {label}")
                    print(f"    —")

        i = batch_end

        # Save progress periodically
        if i % save_interval == 0 or i >= total or credits_exhausted:
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            print(f"  [saved progress: {i}/{total}]")

    if credits_exhausted:
        print(f"\nCredits exhausted at row {i}.")
        # Final save
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    print(f"\nDone! {enriched_count}/{i - start} enriched.")
    print(f"Output -> {output_path}")


if __name__ == "__main__":
    main()
