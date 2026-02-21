#!/usr/bin/env python3
"""Enrich a CSV of people with LinkedIn profile URLs using Exa Answer API."""

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

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "linkedin_url": {
            "type": "string",
            "description": "The LinkedIn profile URL for this person. Empty string if not found.",
        },
        "linkedin_headline": {
            "type": "string",
            "description": "The person's LinkedIn headline or current role description.",
        },
        "confidence": {
            "type": "string",
            "enum": ["high", "medium", "low"],
            "description": "How confident you are this is the right person.",
        },
    },
    "required": ["linkedin_url", "linkedin_headline", "confidence"],
}


def find_linkedin(name: str, company: str = "", title: str = "") -> dict:
    """Use Exa Answer API to find a person's LinkedIn profile."""

    # Build a natural-language prompt
    person_desc = name
    if title:
        person_desc += f", {title}"
    if company:
        person_desc += f" at {company}"

    query = (
        f"Find the LinkedIn profile URL for {person_desc}. "
        f"Search LinkedIn specifically. "
        f"Return their exact linkedin.com/in/ profile URL and their headline."
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
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        print(f"  API error ({e.code}): {err_body}")
        return {"linkedin_url": "", "linkedin_headline": "", "confidence": ""}
    except Exception as e:
        print(f"  Request failed: {e}")
        return {"linkedin_url": "", "linkedin_headline": "", "confidence": ""}

    answer = data.get("answer", {})

    # answer could be a string (unstructured) or dict (structured via outputSchema)
    if isinstance(answer, str):
        # Fallback: try to parse as JSON in case it's a JSON string
        try:
            answer = json.loads(answer)
        except (json.JSONDecodeError, TypeError):
            # Try to extract a linkedin URL from the text
            linkedin_url = ""
            for word in answer.split():
                if "linkedin.com/in/" in word:
                    linkedin_url = word.strip("(),\"'")
                    break
            return {
                "linkedin_url": linkedin_url,
                "linkedin_headline": answer[:200] if answer else "",
                "confidence": "",
            }

    return {
        "linkedin_url": answer.get("linkedin_url", ""),
        "linkedin_headline": answer.get("linkedin_headline", ""),
        "confidence": answer.get("confidence", ""),
    }


def main():
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        rows = list(reader)

    # Add enrichment columns
    for col in ("linkedin_url", "linkedin_headline", "confidence"):
        if col not in fieldnames:
            fieldnames.append(col)

    print(f"Processing {len(rows)} people from {INPUT_CSV}...\n")

    for i, row in enumerate(rows):
        name = row.get("name", "").strip()
        if not name:
            continue

        company = row.get("company", "").strip()
        title = row.get("title", "").strip()

        label = name
        if title:
            label += f" ({title})"
        if company:
            label += f" at {company}"
        print(f"[{i + 1}/{len(rows)}] {label}")

        result = find_linkedin(name, company, title)
        row["linkedin_url"] = result["linkedin_url"]
        row["linkedin_headline"] = result["linkedin_headline"]
        row["confidence"] = result["confidence"]

        if result["linkedin_url"]:
            print(f"  -> {result['linkedin_url']}")
            print(f"     {result['linkedin_headline']} [{result['confidence']}]")
        else:
            print("  -> No LinkedIn found")

        # Rate limit
        if i < len(rows) - 1:
            time.sleep(1)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone! Enriched CSV -> {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
