#!/usr/bin/env python3
"""
Apify + Lusha combined enrichment for WooCommerce agency data.
Uses:
  1. website-email-phone-finder → Contact Emails from website
  2. google-search-scraper → Founder names via Google
  3. lusha /v2/person → Founder email + LinkedIn once name is known
  4. lusha /company → Agency LinkedIn URL
"""

import csv
import os
import re
import time
import requests
from urllib.parse import urlparse

APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")
LUSHA_KEY = os.environ.get("LUSHA_API_KEY", "")

APIFY_BASE = "https://api.apify.com/v2"
LUSHA_BASE = "https://api.lusha.com"

INPUT_FILE = "woo_data.csv"
OUTPUT_FILE = "woo_data_enriched.csv"

apify = requests.Session()
apify.headers.update({"Authorization": f"Bearer {APIFY_TOKEN}"})

lusha = requests.Session()
lusha.headers.update({"api_key": LUSHA_KEY})

# ─── helpers ────────────────────────────────────────────────────────────────

def is_empty(v):
    return not v or str(v).strip().lower() in ("x", "")

def get_domain(url):
    if is_empty(url):
        return None
    try:
        u = url if url.startswith("http") else "https://" + url
        d = urlparse(u).netloc.replace("www.", "")
        return d or None
    except Exception:
        return None

def run_actor_sync(actor_id, payload, timeout=120, memory=512):
    """Run an Apify actor synchronously and return dataset items."""
    url = f"{APIFY_BASE}/acts/{actor_id}/run-sync-get-dataset-items"
    params = {"timeout": timeout, "memory": memory}
    try:
        r = apify.post(url, json=payload, params=params, timeout=timeout + 15)
        if r.status_code in (200, 201):
            return r.json()
        return {"error": r.text[:200]}
    except Exception as e:
        return {"error": str(e)}

def best_email(email_list):
    if not email_list:
        return ""
    priority = {"A+": 0, "A": 1, "B": 2, "C": 3}
    return sorted(email_list, key=lambda e: priority.get(e.get("emailConfidence", "C"), 99))[0].get("email", "")

def parse_name(full_name):
    parts = str(full_name).strip().split()
    if len(parts) < 2:
        return "", ""
    return parts[0], " ".join(parts[1:])

def lusha_company_linkedin(domain):
    try:
        r = lusha.get(f"{LUSHA_BASE}/company", params={"domain": domain}, timeout=10)
        time.sleep(0.12)
        if r.status_code == 200:
            li = r.json().get("data", {}).get("social", {}).get("linkedin", {})
            return (li.get("url", "") if isinstance(li, dict) else str(li)) or ""
    except Exception:
        pass
    return ""

def lusha_person(first, last, domain):
    try:
        r = lusha.get(f"{LUSHA_BASE}/v2/person",
                      params={"firstName": first, "lastName": last, "companyDomain": domain},
                      timeout=10)
        time.sleep(0.12)
        if r.status_code == 200:
            contact = r.json().get("contact", {})
            if not contact.get("error") and contact.get("data"):
                return contact["data"]
    except Exception:
        pass
    return None

# ─── step 1: batch email finder ─────────────────────────────────────────────

def fetch_emails_batch(domains):
    """Use Apify website-email-phone-finder on a list of domains."""
    print(f"\n[Email Finder] Fetching emails for {len(domains)} domains...")
    result = run_actor_sync("mD5Xdfj9XeMS5FWPc",
                            {"domains": domains, "maxDepth": 0},
                            timeout=180, memory=512)
    if isinstance(result, list):
        return {item["domain"]: item.get("emails", []) for item in result}
    print(f"  Error: {result}")
    return {}

# ─── step 2: google search for founders ─────────────────────────────────────

TITLE_KEYWORDS = [
    "founder", "co-founder", "ceo", "managing director", "owner",
    "president", "director", "head"
]

def extract_linkedin_name_from_url(url):
    """Extract person slug from LinkedIn profile URL."""
    m = re.search(r"linkedin\.com/in/([\w\-]+)", url)
    return m.group(1) if m else None

def extract_name_from_google_result(title, snippet, url):
    """Try to extract a person name from a Google search result."""
    # If it's a LinkedIn profile URL, get slug and convert to name
    if "linkedin.com/in/" in url:
        slug = extract_linkedin_name_from_url(url)
        if slug:
            # Convert slug like "rob-twells" to "Rob Twells"
            name = " ".join(w.capitalize() for w in slug.replace("-", " ").split())
            # Validate it looks like a real name (2+ words, not all numbers)
            words = name.split()
            if len(words) >= 2 and all(w[0].isupper() for w in words[:2] if w):
                return name
    # Try to extract from title "FirstName LastName - Title at Company"
    if " - " in title:
        name_part = title.split(" - ")[0].strip()
        words = name_part.split()
        if 2 <= len(words) <= 4 and all(w[0].isupper() for w in words if w):
            # Check if it looks like a person name (not "Our Team" etc.)
            blacklist = {"our", "the", "a", "an", "about", "meet", "team", "contact"}
            if not any(w.lower() in blacklist for w in words):
                return name_part
    return None

def google_search_founder(company_name, domain, company_linkedin=""):
    """Google for the founder/CEO of a company."""
    queries = [
        f'site:linkedin.com/in "{company_name}" founder OR CEO OR "managing director"',
        f'"{company_name}" {domain.split(".")[0]} founder OR CEO linkedin',
    ]
    if company_linkedin and "linkedin.com/company/" in company_linkedin:
        co_slug = company_linkedin.rstrip("/").split("/")[-1]
        queries.insert(0, f'site:linkedin.com/in founder OR CEO "{co_slug}"')

    payload = {
        "queries": "\n".join(queries),
        "maxPagesPerQuery": 1,
        "resultsPerPage": 5,
        "languageCode": "en"
    }
    result = run_actor_sync("apify~google-search-scraper", payload, timeout=60, memory=256)
    time.sleep(0.5)  # courtesy delay

    if not isinstance(result, list):
        return None

    for page in result:
        for item in page.get("organicResults", []):
            title = item.get("title", "")
            snippet = item.get("description", "")
            url = item.get("url", "")
            name = extract_name_from_google_result(title, snippet, url)
            if name:
                return name, url if "linkedin.com/in/" in url else None
    return None

# ─── main ────────────────────────────────────────────────────────────────────

def main():
    with open(INPUT_FILE, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    fieldnames = list(rows[0].keys())
    for col in ["Founder Email", "Partnership POC Email"]:
        if col not in fieldnames:
            fieldnames.append(col)

    total = len(rows)
    print(f"Total rows: {total}")

    # ── Step 1: Batch email finder for all domains ──────────────────────────
    all_domains = []
    domain_map = {}  # domain -> list of row indices
    for i, row in enumerate(rows):
        domain = get_domain(row.get("Website", ""))
        if domain and is_empty(row.get("Contact Email")):
            all_domains.append(domain)
            domain_map.setdefault(domain, []).append(i)

    # Batch in groups of 20
    email_data = {}
    batch_size = 20
    for start in range(0, len(all_domains), batch_size):
        batch = list(set(all_domains[start:start + batch_size]))
        results = fetch_emails_batch(batch)
        email_data.update(results)

    # Apply emails
    for domain, indices in domain_map.items():
        emails = email_data.get(domain, [])
        if emails:
            for i in indices:
                if is_empty(rows[i].get("Contact Email")):
                    rows[i]["Contact Email"] = emails[0]
                    print(f"  Email [{rows[i]['Name']}]: {emails[0]}")

    # ── Step 2: Google-search for missing founder names ─────────────────────
    print(f"\n[Google] Searching for founders...")
    google_budget = 80  # max searches to save credits
    searches_done = 0

    for i, row in enumerate(rows):
        if searches_done >= google_budget:
            print(f"  Google budget ({google_budget}) reached, stopping.")
            break
        if not is_empty(row.get("Founder Name")):
            continue
        domain = get_domain(row.get("Website", ""))
        if not domain:
            continue

        name_result = row.get("Name", "")
        company_linkedin = row.get("Agency LinkedIn", "")
        print(f"  [{i+1}/{total}] Googling founder for: {name_result}")
        result = google_search_founder(name_result, domain, company_linkedin)
        searches_done += 1

        if result:
            founder_name, founder_li_url = result
            rows[i]["Founder Name"] = founder_name
            print(f"    Found: {founder_name}")
            if founder_li_url and is_empty(rows[i].get("Founder LinkedIn")):
                rows[i]["Founder LinkedIn"] = founder_li_url
                print(f"    LinkedIn: {founder_li_url}")

    # ── Step 3: Lusha enrichment for company LinkedIn + founder contacts ─────
    print(f"\n[Lusha] Enriching contacts and company LinkedIn...")
    for i, row in enumerate(rows):
        domain = get_domain(row.get("Website", ""))
        if not domain:
            continue

        # Company LinkedIn
        if is_empty(row.get("Agency LinkedIn")) and domain:
            li = lusha_company_linkedin(domain)
            if li:
                rows[i]["Agency LinkedIn"] = li
                print(f"  [{row['Name']}] Company LinkedIn: {li}")

        # Founder enrichment via Lusha
        founder_name = row.get("Founder Name", "")
        if not is_empty(founder_name):
            first, last = parse_name(founder_name)
            if first and last:
                needs_email = is_empty(row.get("Founder Email")) and is_empty(row.get("Contact Email"))
                needs_li = is_empty(row.get("Founder LinkedIn"))
                if needs_email or needs_li:
                    person = lusha_person(first, last, domain)
                    if person:
                        if needs_email:
                            em = best_email(person.get("emailAddresses", []))
                            if em:
                                rows[i]["Founder Email"] = em
                                if is_empty(rows[i].get("Contact Email")):
                                    rows[i]["Contact Email"] = em
                                print(f"  [{row['Name']}] Founder email: {em}")
                        if needs_li:
                            li_url = person.get("socialLinks", {}).get("linkedin", "")
                            if li_url:
                                rows[i]["Founder LinkedIn"] = li_url
                                print(f"  [{row['Name']}] Founder LinkedIn: {li_url}")

        # POC enrichment via Lusha
        poc_name = row.get("Partnership POC Name", "")
        if not is_empty(poc_name):
            first, last = parse_name(poc_name)
            if first and last:
                needs_poc_email = is_empty(row.get("Partnership POC Email"))
                needs_poc_li = is_empty(row.get("Partnership POC LinkedIn"))
                if needs_poc_email or needs_poc_li:
                    person = lusha_person(first, last, domain)
                    if person:
                        if needs_poc_email:
                            em = best_email(person.get("emailAddresses", []))
                            if em:
                                rows[i]["Partnership POC Email"] = em
                                print(f"  [{row['Name']}] POC email: {em}")
                        if needs_poc_li:
                            li_url = person.get("socialLinks", {}).get("linkedin", "")
                            if li_url:
                                rows[i]["Partnership POC LinkedIn"] = li_url
                                print(f"  [{row['Name']}] POC LinkedIn: {li_url}")

    # ── Save ─────────────────────────────────────────────────────────────────
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    # Summary
    has_email = sum(1 for r in rows if not is_empty(r.get("Contact Email")))
    has_founder = sum(1 for r in rows if not is_empty(r.get("Founder Name")))
    has_founder_li = sum(1 for r in rows if "linkedin.com/in" in r.get("Founder LinkedIn", ""))
    has_agency_li = sum(1 for r in rows if "linkedin.com/company" in r.get("Agency LinkedIn", ""))
    print(f"\n{'='*50}")
    print(f"SUMMARY ({total} rows):")
    print(f"  Contact Email:    {has_email}/{total}")
    print(f"  Founder Name:     {has_founder}/{total}")
    print(f"  Founder LinkedIn: {has_founder_li}/{total}")
    print(f"  Agency LinkedIn:  {has_agency_li}/{total}")
    print(f"  Saved → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
