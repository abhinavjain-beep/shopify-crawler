#!/usr/bin/env python3
"""
Lusha API enrichment for WooCommerce agency data.
Fills in: Agency LinkedIn, Founder Email, Founder LinkedIn, POC info.
"""

import csv
import os
import time
import requests
from urllib.parse import urlparse

API_KEY = os.environ.get("LUSHA_API_KEY", "")
BASE_URL = "https://api.lusha.com"
INPUT_FILE = "woo_data.csv"
OUTPUT_FILE = "woo_data_enriched.csv"

session = requests.Session()
session.headers.update({"api_key": API_KEY})

RATE_LIMIT_DELAY = 0.1  # 10 req/sec (safe under 25/sec limit)


def get_domain(url):
    """Extract bare domain from a URL."""
    if not url or url.strip() in ("X", "", "x"):
        return None
    try:
        parsed = urlparse(url if url.startswith("http") else "https://" + url)
        domain = parsed.netloc.replace("www.", "")
        return domain or None
    except Exception:
        return None


def get_company_info(domain):
    """Fetch company info from Lusha /company endpoint."""
    try:
        resp = session.get(f"{BASE_URL}/company", params={"domain": domain}, timeout=10)
        time.sleep(RATE_LIMIT_DELAY)
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            linkedin = data.get("social", {}).get("linkedin", {})
            if isinstance(linkedin, dict):
                return linkedin.get("url", "")
            return str(linkedin) if linkedin else ""
        return ""
    except Exception as e:
        print(f"  Company error for {domain}: {e}")
        return ""


def get_person_info(first_name, last_name, company_domain):
    """Fetch person contact info from Lusha /v2/person endpoint."""
    try:
        params = {
            "firstName": first_name,
            "lastName": last_name,
            "companyDomain": company_domain,
        }
        resp = session.get(f"{BASE_URL}/v2/person", params=params, timeout=10)
        time.sleep(RATE_LIMIT_DELAY)
        if resp.status_code == 200:
            contact = resp.json().get("contact", {})
            if contact.get("error") or not contact.get("data"):
                return None
            return contact["data"]
        return None
    except Exception as e:
        print(f"  Person error for {first_name} {last_name}: {e}")
        return None


def best_email(email_list):
    """Return the highest confidence email from the list."""
    if not email_list:
        return ""
    # Sort by confidence: A+ > A > B > etc.
    priority = {"A+": 0, "A": 1, "B": 2, "C": 3}
    sorted_emails = sorted(
        email_list,
        key=lambda e: priority.get(e.get("emailConfidence", "C"), 99)
    )
    return sorted_emails[0].get("email", "")


def parse_name(full_name):
    """Split a full name into first and last name."""
    parts = full_name.strip().split()
    if len(parts) == 0:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def is_empty(val):
    return not val or val.strip() in ("X", "x", "")


def main():
    with open(INPUT_FILE, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    fieldnames = list(rows[0].keys())
    # Ensure we have the enrichment columns
    for col in ["Founder Email", "Partnership POC Email"]:
        if col not in fieldnames:
            fieldnames.append(col)

    total = len(rows)
    enriched = 0

    print(f"Processing {total} rows...\n")

    for i, row in enumerate(rows):
        name = row.get("Name", "")
        website = row.get("Website", "")
        domain = get_domain(website)

        print(f"[{i+1}/{total}] {name} ({domain})")

        # 1. Get Agency LinkedIn if missing
        if is_empty(row.get("Agency LinkedIn")) and domain:
            print(f"  -> Fetching company LinkedIn...")
            li = get_company_info(domain)
            if li:
                row["Agency LinkedIn"] = li
                print(f"     Company LinkedIn: {li}")
                enriched += 1

        # 2. Enrich Founder if name is known
        founder_name = row.get("Founder Name", "")
        if not is_empty(founder_name) and domain:
            first, last = parse_name(founder_name)
            if first and last:
                # Only call if email or LinkedIn is missing
                needs_email = is_empty(row.get("Contact Email")) and is_empty(row.get("Founder Email", ""))
                needs_li = is_empty(row.get("Founder LinkedIn"))
                if needs_email or needs_li:
                    print(f"  -> Fetching person info for {founder_name}...")
                    person = get_person_info(first, last, domain)
                    if person:
                        if needs_email:
                            email = best_email(person.get("emailAddresses", []))
                            if email:
                                row["Founder Email"] = email
                                if is_empty(row.get("Contact Email")):
                                    row["Contact Email"] = email
                                print(f"     Founder email: {email}")
                                enriched += 1
                        if needs_li:
                            li_url = person.get("socialLinks", {}).get("linkedin", "")
                            if li_url:
                                row["Founder LinkedIn"] = li_url
                                print(f"     Founder LinkedIn: {li_url}")
                                enriched += 1
                    else:
                        print(f"     No data found for {founder_name}")

        # 3. Enrich Partnership POC if name is known
        poc_name = row.get("Partnership POC Name", "")
        if not is_empty(poc_name) and domain:
            first, last = parse_name(poc_name)
            if first and last:
                needs_poc_email = is_empty(row.get("Partnership POC Email", ""))
                needs_poc_li = is_empty(row.get("Partnership POC LinkedIn"))
                if needs_poc_email or needs_poc_li:
                    print(f"  -> Fetching POC info for {poc_name}...")
                    person = get_person_info(first, last, domain)
                    if person:
                        if needs_poc_email:
                            email = best_email(person.get("emailAddresses", []))
                            if email:
                                row["Partnership POC Email"] = email
                                print(f"     POC email: {email}")
                                enriched += 1
                        if needs_poc_li:
                            li_url = person.get("socialLinks", {}).get("linkedin", "")
                            if li_url:
                                row["Partnership POC LinkedIn"] = li_url
                                print(f"     POC LinkedIn: {li_url}")
                                enriched += 1
                    else:
                        print(f"     No data found for POC {poc_name}")

    # Write enriched data
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone! {enriched} fields enriched. Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
