#!/usr/bin/env python3
"""Enrich a CSV of people with LinkedIn, role, and email using Exa Answer API (batched)."""

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

INPUT_CSV = sys.argv[1] if len(sys.argv) > 1 else "people.csv"
OUTPUT_CSV = sys.argv[2] if len(sys.argv) > 2 else INPUT_CSV.replace(".csv", "_enriched.csv")
BATCH_SIZE = 5  # People per API call

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "people": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "The person's full name as provided.",
                    },
                    "role": {
                        "type": "string",
                        "description": "Their current role/title and company.",
                    },
                    "email": {
                        "type": "string",
                        "description": "Their email address. Empty string if not found.",
                    },
                    "linkedin": {
                        "type": "string",
                        "description": "Their LinkedIn profile URL (linkedin.com/in/...).",
                    },
                    "additional_links": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Any other relevant profile links (Twitter, personal site, etc.).",
                    },
                },
                "required": ["name", "role", "email", "linkedin", "additional_links"],
            },
        },
    },
    "required": ["people"],
}


def build_people_list(rows: list[dict]) -> str:
    """Build a text list of people from CSV rows."""
    lines = []
    for row in rows:
        name = row.get("name", "").strip()
        parts = [name]
        title = row.get("title", "").strip()
        company = row.get("company", "").strip()
        instagram = row.get("instagram", "").strip()
        if title:
            parts.append(title)
        if company:
            parts.append(f"at {company}")
        if instagram:
            parts.append(f"(Instagram: @{instagram.lstrip('@')})")
        lines.append(" - ".join(parts))
    return "\n".join(lines)


def enrich_batch(rows: list[dict]) -> list[dict]:
    """Send a batch of people to Exa Answer API and get enriched data back."""
    people_list = build_people_list(rows)

    query = (
        f"Here are some people:\n\n{people_list}\n\n"
        f"What are the LinkedIns of these people and their role and their email? "
        f"Give in array structure."
    )

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

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        print(f"  API error ({e.code}): {err_body}")
        return []
    except Exception as e:
        print(f"  Request failed: {e}")
        return []

    answer = data.get("answer", [])

    if isinstance(answer, str):
        try:
            answer = json.loads(answer)
        except (json.JSONDecodeError, TypeError):
            print(f"  Could not parse response: {answer[:200]}")
            return []

    if isinstance(answer, dict):
        answer = answer.get("people", [answer])

    return answer if isinstance(answer, list) else []


def match_results(rows: list[dict], results: list[dict]) -> None:
    """Match API results back to CSV rows by name."""
    # Index results by lowercase name for fuzzy matching
    result_map = {}
    for r in results:
        rname = r.get("name", "").strip().lower()
        if rname:
            result_map[rname] = r

    for row in rows:
        name = row.get("name", "").strip().lower()
        match = result_map.get(name)
        if match:
            row["linkedin"] = match.get("linkedin", "")
            row["role"] = match.get("role", row.get("role", ""))
            row["email"] = match.get("email", "")
            row["additional_links"] = "; ".join(match.get("additional_links", []))
        else:
            row.setdefault("linkedin", "")
            row.setdefault("email", "")
            row.setdefault("additional_links", "")


def main():
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        rows = list(reader)

    # Add enrichment columns
    for col in ("linkedin", "role", "email", "additional_links"):
        if col not in fieldnames:
            fieldnames.append(col)

    print(f"Processing {len(rows)} people from {INPUT_CSV} (batch size: {BATCH_SIZE})...\n")

    # Process in batches
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        batch_num = (start // BATCH_SIZE) + 1
        total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE

        names = [r.get("name", "").strip() for r in batch]
        print(f"[Batch {batch_num}/{total_batches}] {', '.join(names)}")

        results = enrich_batch(batch)

        if results:
            match_results(batch, results)
            for r in results:
                name = r.get("name", "")
                linkedin = r.get("linkedin", "")
                role = r.get("role", "")
                email = r.get("email", "")
                links = r.get("additional_links", [])
                print(f"  {name}")
                print(f"    role:     {role}")
                print(f"    email:    {email or '-'}")
                print(f"    linkedin: {linkedin or '-'}")
                if links:
                    print(f"    links:    {', '.join(links)}")
        else:
            print("  No results returned")

        # Rate limit between batches
        if start + BATCH_SIZE < len(rows):
            time.sleep(1)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone! Enriched CSV -> {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
