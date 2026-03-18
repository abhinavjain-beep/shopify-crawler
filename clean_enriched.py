#!/usr/bin/env python3
"""Clean up enriched CSV: fix name artifacts from LinkedIn slug extraction."""

import csv
import re

INPUT = "woo_data_enriched.csv"
OUTPUT = "woo_data_enriched.csv"  # overwrite in place


def clean_linkedin_url(url):
    if not url or "linkedin.com" not in url:
        return url
    url = re.sub(r"[#?].*$", "", url).rstrip("/")
    # Remove trailing locale suffixes like /es /nl /en /ru /fr
    url = re.sub(r"/(en|es|nl|ru|fr|de|pt|pl|it|tr|zh|ja|ar|ko|sv|fi|da|nb|hu|cs|ro|bg|hr|sk|sl|lt|lv|et)$", "", url)
    return url.rstrip("/")


def clean_name(name):
    if not name or str(name).strip().lower() in ("x", "", "none"):
        return name
    name = str(name).strip()

    # Fix multiline junk (e.g., "Rob Twells\nCo" -> "Rob Twells")
    name = name.split("\n")[0].strip()

    # Remove possessive post artifacts: "Justin Herring's Post" -> "Justin Herring"
    name = re.sub(r"'s?\s+(Post|Profile|Page|Update|Article).*$", "", name, flags=re.IGNORECASE).strip()

    # Remove LinkedIn hash IDs at the end: "Rob Twells 9a19bb178" -> "Rob Twells"
    name = re.sub(r"\s+[0-9a-f]{6,12}$", "", name, flags=re.IGNORECASE).strip()

    # Remove known non-name suffixes (single words that aren't names)
    junk_words = {
        "web", "dev", "developer", "design", "designer", "digital", "agency",
        "creative", "studio", "llc", "inc", "ltd", "co", "post", "media",
        "solutions", "technologies", "consulting", "services"
    }
    parts = name.split()
    if len(parts) > 2:
        while len(parts) > 2 and parts[-1].lower() in junk_words:
            parts.pop()
        name = " ".join(parts)

    # Final sanity check: must be at least 2 words, not a URL, not all caps junk
    if not name or len(name.split()) < 2 or name.startswith("http") or len(name) > 50:
        return ""

    return name.strip()


def main():
    with open(INPUT, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(rows[0].keys())

    cleaned = 0
    for row in rows:
        # Clean founder name
        orig = row.get("Founder Name", "")
        new = clean_name(orig)
        if new != orig:
            row["Founder Name"] = new
            cleaned += 1

        # Clean LinkedIn URLs
        for field in ["Founder LinkedIn", "Agency LinkedIn", "Partnership POC LinkedIn"]:
            li = row.get(field, "")
            if li and "linkedin.com" in li:
                row[field] = clean_linkedin_url(li)

        # Clean bad emails (filenames from cloudwp etc)
        for field in ["Contact Email", "Founder Email", "Partnership POC Email"]:
            em = row.get(field, "")
            if em and "@" not in em:
                row[field] = ""

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    # Stats
    total = len(rows)
    has_email = sum(1 for r in rows if r.get("Contact Email", "").strip() and "@" in r.get("Contact Email", ""))
    has_founder = sum(1 for r in rows if len(r.get("Founder Name", "").split()) >= 2)
    has_founder_li = sum(1 for r in rows if "linkedin.com/in" in r.get("Founder LinkedIn", ""))
    has_agency_li = sum(1 for r in rows if "linkedin.com/company" in r.get("Agency LinkedIn", ""))
    print(f"Cleaned {cleaned} names. Final stats ({total} rows):")
    print(f"  Contact Email:    {has_email}/{total}")
    print(f"  Founder Name:     {has_founder}/{total}")
    print(f"  Founder LinkedIn: {has_founder_li}/{total}")
    print(f"  Agency LinkedIn:  {has_agency_li}/{total}")
    print(f"Saved → {OUTPUT}")


if __name__ == "__main__":
    main()
