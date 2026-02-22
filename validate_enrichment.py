#!/usr/bin/env python3
"""Validate enriched CSV data using heuristics + Exa Answer API for verification.

Flags suspicious phone numbers, removes bad data, and adds a quality score.
"""

import csv
import io
import json
import os
import re
import sys
import time
import urllib.request

EXA_API_KEY = os.environ.get("EXA_API_KEY")
if not EXA_API_KEY:
    print("Error: EXA_API_KEY environment variable not set")
    sys.exit(1)

INPUT_CSV = sys.argv[1] if len(sys.argv) > 1 else "us_superhuman_users_enriched.csv"
OUTPUT_CSV = sys.argv[2] if len(sys.argv) > 2 else INPUT_CSV.replace(".csv", "_validated.csv")

VERIFY_SCHEMA = {
    "type": "object",
    "properties": {
        "is_valid": {
            "type": "boolean",
            "description": "True if the phone number is likely correct for this specific person.",
        },
        "reason": {
            "type": "string",
            "description": "Brief explanation of why it is or isn't valid.",
        },
        "corrected_phone": {
            "type": "string",
            "description": "Corrected phone number if you found the right one. Empty string otherwise.",
        },
    },
    "required": ["is_valid", "reason", "corrected_phone"],
}


def heuristic_check(phone: str, name: str, source: str) -> tuple[str, str]:
    """Run heuristic checks on a phone number. Returns (status, reason)."""
    if not phone.strip():
        return ("empty", "no phone")

    cleaned = re.sub(r"[^\dX*x]", "", phone)

    # Masked/partial numbers (XXXX, ****, ....)
    if re.search(r"[Xx*\.]{3,}", phone):
        return ("partial", f"masked/partial number: {phone}")

    # 555 fake numbers
    if re.search(r"555-?\d{4}", phone):
        return ("fake", f"likely fake 555 number: {phone}")

    # Too short (less than 7 digits)
    digits = re.sub(r"\D", "", cleaned)
    if len(digits) < 7:
        return ("invalid", f"too few digits ({len(digits)}): {phone}")

    # Too long (more than 15 digits)
    if len(digits) > 15:
        return ("invalid", f"too many digits ({len(digits)}): {phone}")

    # Generic placeholder patterns
    if re.search(r"123-?4567", phone):
        return ("fake", f"placeholder number: {phone}")

    if re.search(r"000-?0000", phone):
        return ("fake", f"zeros pattern: {phone}")

    # Source is a generic company page, not personal
    company_page_patterns = [
        r"kroger\.com", r"fiu\.edu", r"rit\.edu", r"utdallas\.edu",
        r"fremont\.gov", r"umich\.edu", r"stanford\.edu",
        r"/contact-us", r"/contact$", r"/faqs",
    ]
    for pattern in company_page_patterns:
        if re.search(pattern, source, re.IGNORECASE):
            return ("suspect", f"likely company/org number from {source}")

    # Looks OK by heuristics
    return ("ok", "passed heuristic checks")


def verify_with_exa(name: str, phone: str, bio: str, location: str, email: str) -> dict:
    """Use Exa to verify if a phone number belongs to the person."""
    person_desc = name
    if bio:
        person_desc += f" ({bio[:100]})"
    if location:
        person_desc += f" in {location}"

    query = (
        f"Verify: does the phone number {phone} belong to {person_desc}? "
        f"Their email is {email}. "
        f"Search for this person and check if this phone number is actually theirs, "
        f"or if it's a company main line / wrong person / generic number. "
        f"If you find their real phone number, provide it."
    )

    body = json.dumps({
        "query": query,
        "text": True,
        "outputSchema": VERIFY_SCHEMA,
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
    except Exception as e:
        return {"is_valid": None, "reason": f"API error: {e}", "corrected_phone": ""}

    answer = data.get("answer", {})
    if isinstance(answer, str):
        try:
            answer = json.loads(answer)
        except (json.JSONDecodeError, TypeError):
            return {"is_valid": None, "reason": answer[:200], "corrected_phone": ""}

    return {
        "is_valid": answer.get("is_valid"),
        "reason": answer.get("reason", ""),
        "corrected_phone": answer.get("corrected_phone", ""),
    }


def main():
    # Read CSV handling NUL bytes
    with open(INPUT_CSV, newline="", encoding="utf-8", errors="replace") as f:
        content = f.read().replace("\x00", "")
    rows = list(csv.DictReader(io.StringIO(content)))
    fieldnames = list(rows[0].keys()) if rows else []

    # Add validation columns
    for col in ("phone_status", "phone_review_note", "verified_phone"):
        if col not in fieldnames:
            fieldnames.append(col)

    total = len(rows)
    stats = {"empty": 0, "ok": 0, "fake": 0, "partial": 0, "invalid": 0, "suspect": 0, "verified": 0, "rejected": 0}

    print(f"Validating {total} rows from {INPUT_CSV}...\n")
    print("=" * 70)
    print("PHASE 1: Heuristic checks")
    print("=" * 70)

    suspect_rows = []

    for i, row in enumerate(rows):
        phone = row.get("phone_number", "").strip()
        name = row.get("name", "").strip()
        source = row.get("phone_source", "").strip()

        status, reason = heuristic_check(phone, name, source)
        stats[status] += 1

        row["phone_status"] = status
        row["phone_review_note"] = reason
        row["verified_phone"] = ""

        if status == "fake":
            # Clear fake numbers
            row["phone_number"] = ""
            row["phone_status"] = "removed"
            print(f"  REMOVED  {name}: {reason}")
        elif status == "partial":
            row["phone_status"] = "partial"
            print(f"  PARTIAL  {name}: {reason}")
        elif status == "invalid":
            row["phone_number"] = ""
            row["phone_status"] = "removed"
            print(f"  REMOVED  {name}: {reason}")
        elif status == "suspect":
            suspect_rows.append((i, row))
            print(f"  SUSPECT  {name}: {reason}")

    print(f"\nHeuristic results:")
    print(f"  Empty (no phone):  {stats['empty']}")
    print(f"  Passed:            {stats['ok']}")
    print(f"  Fake (removed):    {stats['fake']}")
    print(f"  Partial (masked):  {stats['partial']}")
    print(f"  Invalid (removed): {stats['invalid']}")
    print(f"  Suspect (to verify): {stats['suspect']}")

    # Phase 2: Verify suspect numbers with Exa
    if suspect_rows:
        print(f"\n{'=' * 70}")
        print(f"PHASE 2: Verifying {len(suspect_rows)} suspect numbers with Exa")
        print(f"{'=' * 70}")

        for j, (idx, row) in enumerate(suspect_rows):
            name = row.get("name", "").strip()
            phone = row.get("phone_number", "").strip()
            bio = row.get("bio", "").strip()
            location = row.get("location", "").strip()
            email = row.get("email", "").strip()

            print(f"\n  [{j + 1}/{len(suspect_rows)}] {name} — {phone}")

            result = verify_with_exa(name, phone, bio, location, email)

            if result["is_valid"] is True:
                row["phone_status"] = "verified"
                row["phone_review_note"] = f"Verified: {result['reason']}"
                stats["verified"] += 1
                print(f"    VERIFIED: {result['reason']}")
            elif result["is_valid"] is False:
                row["phone_status"] = "rejected"
                row["phone_review_note"] = f"Rejected: {result['reason']}"
                stats["rejected"] += 1
                print(f"    REJECTED: {result['reason']}")
                if result["corrected_phone"]:
                    row["verified_phone"] = result["corrected_phone"]
                    print(f"    CORRECTED -> {result['corrected_phone']}")
                else:
                    row["phone_number"] = ""
            else:
                row["phone_status"] = "unverified"
                row["phone_review_note"] = f"Could not verify: {result['reason']}"
                print(f"    UNVERIFIED: {result['reason']}")

            if j < len(suspect_rows) - 1:
                time.sleep(0.5)

    # Write output
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Final stats
    final_phones = sum(1 for r in rows if r.get("phone_number", "").strip())
    removed = total - stats["empty"] - final_phones - stats["partial"]

    print(f"\n{'=' * 70}")
    print(f"FINAL SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Total rows:          {total}")
    print(f"  Phones remaining:    {final_phones}")
    print(f"  Partial (masked):    {stats['partial']}")
    print(f"  Removed (bad data):  {stats['fake'] + stats['invalid'] + stats['rejected']}")
    print(f"  No phone found:      {stats['empty']}")
    print(f"\nValidated CSV -> {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
