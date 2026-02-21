#!/usr/bin/env python3
"""Enrich a CSV of people (name + Instagram) with phone numbers using Exa Answer API."""

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
OUTPUT_CSV = sys.argv[2] if len(sys.argv) > 2 else INPUT_CSV.replace(".csv", "_phones.csv")

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "phone_number": {
            "type": "string",
            "description": "The person's phone number including country code. Empty string if not found.",
        },
        "phone_type": {
            "type": "string",
            "enum": ["mobile", "work", "unknown"],
            "description": "Type of phone number if determinable.",
        },
        "email": {
            "type": "string",
            "description": "Email address if found. Empty string if not found.",
        },
        "source": {
            "type": "string",
            "description": "Where the contact info was found (e.g. website name, directory).",
        },
        "confidence": {
            "type": "string",
            "enum": ["high", "medium", "low"],
            "description": "How confident you are this contact info belongs to the right person.",
        },
    },
    "required": ["phone_number", "phone_type", "email", "source", "confidence"],
}


def find_phone(name: str, instagram: str = "", company: str = "", title: str = "") -> dict:
    """Use Exa Answer API to find a person's phone number."""

    person_desc = name
    if title:
        person_desc += f", {title}"
    if company:
        person_desc += f" at {company}"

    query = f"Find the phone number and contact information for {person_desc}."
    if instagram:
        ig = instagram.lstrip("@")
        query += f" Their Instagram handle is @{ig} (instagram.com/{ig})."
    query += (
        " Search public directories, personal websites, about pages, and contact pages."
        " Return their phone number with country code, email if available, and where you found it."
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

    empty = {"phone_number": "", "phone_type": "", "email": "", "source": "", "confidence": ""}

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        print(f"  API error ({e.code}): {err_body}")
        return empty
    except Exception as e:
        print(f"  Request failed: {e}")
        return empty

    answer = data.get("answer", {})

    if isinstance(answer, str):
        try:
            answer = json.loads(answer)
        except (json.JSONDecodeError, TypeError):
            return {**empty, "source": answer[:200] if answer else ""}

    return {
        "phone_number": answer.get("phone_number", ""),
        "phone_type": answer.get("phone_type", ""),
        "email": answer.get("email", ""),
        "source": answer.get("source", ""),
        "confidence": answer.get("confidence", ""),
    }


def main():
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        rows = list(reader)

    for col in ("phone_number", "phone_type", "email", "source", "confidence"):
        if col not in fieldnames:
            fieldnames.append(col)

    print(f"Processing {len(rows)} people from {INPUT_CSV}...\n")

    for i, row in enumerate(rows):
        name = row.get("name", "").strip()
        if not name:
            continue

        instagram = row.get("instagram", "").strip()
        company = row.get("company", "").strip()
        title = row.get("title", "").strip()

        label = name
        if instagram:
            label += f" (@{instagram.lstrip('@')})"
        if company:
            label += f" at {company}"
        print(f"[{i + 1}/{len(rows)}] {label}")

        result = find_phone(name, instagram, company, title)
        for key in ("phone_number", "phone_type", "email", "source", "confidence"):
            row[key] = result[key]

        if result["phone_number"]:
            print(f"  -> {result['phone_number']} ({result['phone_type']}) [{result['confidence']}]")
            if result["email"]:
                print(f"     {result['email']}")
            print(f"     via {result['source']}")
        else:
            print("  -> No phone found")
            if result["email"]:
                print(f"     Email: {result['email']}")

        if i < len(rows) - 1:
            time.sleep(1)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone! Enriched CSV -> {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
