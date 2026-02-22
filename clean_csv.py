#!/usr/bin/env python3
"""Clean and validate the enriched followers CSV.

Fixes:
- Fake phone numbers (555, 123-456-7890, placeholder patterns)
- Fake emails (@example.com, URLs instead of emails)
- "Not publicly available" / "Not provided" / "Preparing profile" junk values
- "Verified" suffix in full_name
- URLs in email field
- Heavily masked data (more *'s than real chars)
- "Unknown" role/company placeholders
"""

from __future__ import annotations

import csv
import re
import sys

INPUT = sys.argv[1] if len(sys.argv) > 1 else "fyxerofficial_500_followers.csv"
OUTPUT = INPUT.replace(".csv", "_clean.csv")

# --- Fake phone patterns ---
FAKE_PHONES = {
    "123-456-7890", "+1 123-456-7890", "123 456 7890",
    "+1-202-555-0123", "+1-512-555-1234", "+1 234 567 8900",
    "+1-555-123-4567", "+1-407-555-1234", "+1 416-123-4567",
}

def is_fake_phone(phone: str) -> bool:
    if not phone:
        return False
    p = phone.strip()
    if p in FAKE_PHONES:
        return True
    # 555 area code (US fake numbers)
    if re.search(r'\b555[-.\s]?\d{4}\b', p):
        return True
    # All same digits
    digits = re.sub(r'\D', '', p)
    if len(digits) >= 7 and len(set(digits)) <= 2:
        return True
    return False


def is_junk(val: str) -> bool:
    """Check if a value is a placeholder/junk."""
    if not val:
        return False
    low = val.strip().lower()
    junk_phrases = [
        "not publicly available", "not provided", "not found",
        "preparing profile", "unknown", "n/a", "none", "null",
        "protected", "http://click-to-open",
    ]
    return low in junk_phrases


def is_url_not_email(val: str) -> bool:
    """Check if value is a URL instead of an email."""
    if not val:
        return False
    v = val.strip()
    return v.startswith("http") or v.startswith("www.") or "rocketreach.co" in v or "contactout.com" in v


def is_too_masked(val: str) -> bool:
    """Check if value has too many mask characters to be useful."""
    if not val:
        return False
    stars = val.count("*")
    total = len(val)
    if total == 0:
        return False
    # If more than 60% masked, it's unusable
    return stars > 0 and (stars / total) > 0.6


def clean_full_name(name: str) -> str:
    """Remove 'Verified' suffix from names like 'usernameVerified'."""
    if not name:
        return name
    # Remove trailing "Verified"
    if name.endswith("Verified"):
        name = name[:-len("Verified")].strip()
    return name


def clean_email(email: str) -> str:
    """Clean and validate email field."""
    if not email:
        return ""
    # Handle multiple emails (keep first valid one)
    emails = [e.strip() for e in email.split(",")]
    valid = []
    for e in emails:
        if is_junk(e) or is_url_not_email(e) or is_too_masked(e):
            continue
        if "@example.com" in e:
            continue
        if "@" in e and "." in e and not e.startswith("http"):
            valid.append(e)
    return valid[0] if valid else ""


def clean_phone(phone: str) -> str:
    """Clean and validate phone field."""
    if not phone:
        return ""
    # Handle multiple phones
    phones = [p.strip() for p in phone.split(",")]
    valid = []
    for p in phones:
        if is_junk(p) or is_fake_phone(p) or is_too_masked(p):
            continue
        # Must have at least 7 digits
        digits = re.sub(r'\D', '', p)
        if len(digits) >= 7:
            valid.append(p)
    return valid[0] if valid else ""


def clean_linkedin(url: str) -> str:
    """Clean LinkedIn URL."""
    if not url:
        return ""
    if is_junk(url):
        return ""
    # Remove bare "https://linkedin.com" with no profile path
    if url.strip().rstrip("/") in ("https://linkedin.com", "http://linkedin.com", "https://www.linkedin.com"):
        return ""
    # Must contain linkedin.com
    if "linkedin.com" not in url:
        return ""
    # Remove authwall URLs
    if "authwall" in url:
        return ""
    return url.strip()


def clean_role(role: str) -> str:
    if is_junk(role):
        return ""
    return role.strip() if role else ""


def clean_company(company: str) -> str:
    if is_junk(company):
        return ""
    return company.strip() if company else ""


def main():
    with open(INPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        rows = list(reader)

    stats = {"total": len(rows), "cleaned_phones": 0, "cleaned_emails": 0,
             "cleaned_linkedin": 0, "cleaned_names": 0, "cleaned_roles": 0}

    for row in rows:
        # Clean full_name
        orig = row.get("full_name", "")
        row["full_name"] = clean_full_name(orig)
        if row["full_name"] != orig:
            stats["cleaned_names"] += 1

        # Clean email
        orig = row.get("email", "")
        row["email"] = clean_email(orig)
        if row["email"] != orig:
            stats["cleaned_emails"] += 1

        # Clean phone
        orig = row.get("phone_number", "")
        row["phone_number"] = clean_phone(orig)
        if row["phone_number"] != orig:
            stats["cleaned_phones"] += 1

        # Clean linkedin
        orig = row.get("linkedin", "")
        row["linkedin"] = clean_linkedin(orig)
        if row["linkedin"] != orig:
            stats["cleaned_linkedin"] += 1

        # Clean role & company
        orig_r = row.get("role", "")
        row["role"] = clean_role(orig_r)
        orig_c = row.get("company", "")
        row["company"] = clean_company(orig_c)
        if row["role"] != orig_r or row["company"] != orig_c:
            stats["cleaned_roles"] += 1

    # Write cleaned CSV
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Print summary
    print(f"Cleaned {INPUT} -> {OUTPUT}")
    print(f"  Total rows:       {stats['total']}")
    print(f"  Names fixed:      {stats['cleaned_names']}")
    print(f"  Emails cleaned:   {stats['cleaned_emails']}")
    print(f"  Phones cleaned:   {stats['cleaned_phones']}")
    print(f"  LinkedIn cleaned: {stats['cleaned_linkedin']}")
    print(f"  Roles cleaned:    {stats['cleaned_roles']}")

    # Print final stats
    has_linkedin = sum(1 for r in rows if r.get("linkedin"))
    has_email = sum(1 for r in rows if r.get("email"))
    has_phone = sum(1 for r in rows if r.get("phone_number"))
    has_role = sum(1 for r in rows if r.get("role"))
    has_any = sum(1 for r in rows if r.get("linkedin") or r.get("email") or r.get("phone_number"))

    print(f"\n  Final data quality:")
    print(f"    With LinkedIn:  {has_linkedin}/{stats['total']}")
    print(f"    With email:     {has_email}/{stats['total']}")
    print(f"    With phone:     {has_phone}/{stats['total']}")
    print(f"    With role:      {has_role}/{stats['total']}")
    print(f"    With any data:  {has_any}/{stats['total']}")


if __name__ == "__main__":
    main()
