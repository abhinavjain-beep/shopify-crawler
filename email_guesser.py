"""
Email Candidate Generator — second-pass script
For freelancers that have no scraped email, visits their Guru profile,
extracts the website domain, checks MX via mailcheck.ai, then constructs
a candidate email (info@domain or firstname@domain).

Outputs freelancers_emails_final.csv with columns:
  ...all original columns...
  Email        — real scraped email OR best candidate
  Email_Source — 'scraped' | 'candidate-info' | 'candidate-name' | ''
"""

import re
import asyncio
import logging
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

from scraper import Scraper

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("email_guesser")

BASE_DIR      = Path(__file__).resolve().parent
SCRAPED_FILE  = BASE_DIR / "freelancers_emails.csv"
OUTPUT_FILE   = BASE_DIR / "freelancers_emails_final.csv"

BATCH_SIZE    = 15
RATE          = 4

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

SKIP_DOMAINS = {
    "guru.com", "linkedin.com", "facebook.com", "twitter.com",
    "x.com", "instagram.com", "youtube.com", "github.com",
    "t.co", "behance.net", "dribbble.com", "clutch.co",
    "apple.com", "apps.apple.com", "play.google.com", "google.com",
    "yelp.com", "crunchbase.com", "upwork.com", "fiverr.com",
    "wa.me", "whatsapp.com", "telegram.me", "medium.com",
}

SOCIAL_CLASSES = {"socialIcon", "profile-web__social__icon",
                  "c-footer__socials__social"}


def get_website_url(soup: BeautifulSoup) -> str | None:
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href.startswith("http"):
            continue
        classes = set(a.get("class") or [])
        if classes & SOCIAL_CLASSES:
            continue
        try:
            domain = href.split("/")[2]
        except IndexError:
            continue
        if any(domain == d or domain.endswith("." + d) for d in SKIP_DOMAINS):
            continue
        return href
    return None


def clean_domain(url: str) -> str | None:
    """Extract bare domain (no www, no port) from a URL."""
    try:
        host = url.split("/")[2]
        host = host.lstrip("www.")
        host = host.split(":")[0]
        return host if "." in host else None
    except Exception:
        return None


def first_name(full_name: str) -> str:
    """Return lowercased first word of name, stripped of non-alpha chars."""
    word = full_name.strip().split()[0] if full_name.strip() else ""
    clean = re.sub(r"[^a-z]", "", word.lower())
    return clean


async def check_mx(domain: str, scraper: Scraper) -> bool:
    """Return True if the domain has a valid MX record (via mailcheck.ai HTTP API)."""
    try:
        resp = await scraper.get_json(
            f"https://api.mailcheck.ai/email/info@{domain}",
            headers={"Accept": "application/json"},
        )
        if resp:
            return bool(resp.get("mx", False))
    except Exception:
        pass
    return False


async def process_row(row: dict, scraper: Scraper, index: int, total: int) -> dict:
    email = str(row.get("Email", "") or "").strip()
    name = row.get("Name", "")

    # Already has a real email — keep it
    if email and "@" in email:
        print(f"  [{index:4}/{total}]  {name[:28]:28}  ✓ [kept] {email}")
        return {**row, "Email": email, "Email_Source": "scraped"}

    # Visit Guru profile to find website
    soup = await scraper.get_soup(row["Profile_URL"], headers=HEADERS)
    if not soup:
        print(f"  [{index:4}/{total}]  {name[:28]:28}  ✗ no profile response")
        return {**row, "Email": "", "Email_Source": ""}

    website = get_website_url(soup)
    if not website:
        print(f"  [{index:4}/{total}]  {name[:28]:28}  ✗ no website")
        return {**row, "Email": "", "Email_Source": ""}

    domain = clean_domain(website)
    if not domain:
        print(f"  [{index:4}/{total}]  {name[:28]:28}  ✗ bad domain")
        return {**row, "Email": "", "Email_Source": ""}

    # Check if domain accepts email (MX record)
    has_mx = await check_mx(domain, scraper)
    if not has_mx:
        print(f"  [{index:4}/{total}]  {name[:28]:28}  ✗ no MX ({domain})")
        return {**row, "Email": "", "Email_Source": ""}

    # Prefer info@ for companies, firstname@ for individuals
    fname = first_name(name)
    # Heuristic: if name has multiple words AND no "Inc/LLC/Ltd/Co" → individual
    name_parts = name.strip().split()
    is_individual = (
        len(name_parts) >= 2
        and not any(w.lower() in {"inc", "llc", "ltd", "co", "corp", "agency", "solutions",
                                   "group", "studio", "tech", "labs", "consulting"}
                    for w in name_parts)
    )

    if is_individual and fname:
        candidate = f"{fname}@{domain}"
        source = "candidate-name"
    else:
        candidate = f"info@{domain}"
        source = "candidate-info"

    print(f"  [{index:4}/{total}]  {name[:28]:28}  ~ [{source}] {candidate}")
    return {**row, "Email": candidate, "Email_Source": source}


async def main():
    df = pd.read_csv(SCRAPED_FILE, encoding="utf-8-sig")
    rows = df.to_dict("records")
    total = len(rows)
    print(f"Loaded {total} rows from {SCRAPED_FILE}")

    # Resume support
    done_urls: set = set()
    if OUTPUT_FILE.exists():
        try:
            done_df = pd.read_csv(OUTPUT_FILE, usecols=["Profile_URL"], encoding="utf-8-sig")
            done_urls = set(done_df["Profile_URL"].dropna().tolist())
            print(f"Resuming — {len(done_urls)} already done.")
        except Exception as e:
            logger.warning(f"Could not read output: {e}")

    pending = [r for r in rows if r["Profile_URL"] not in done_urls]
    print(f"{len(pending)} to process.\n")
    if not pending:
        print("Nothing to do.")
        return

    scraper = Scraper(requests_per_second=RATE, timeout=15)
    total_found = 0

    try:
        for batch_start in range(0, len(pending), BATCH_SIZE):
            batch = pending[batch_start:batch_start + BATCH_SIZE]
            tasks = [
                process_row(row, scraper,
                            batch_start + i + len(done_urls) + 1, total)
                for i, row in enumerate(batch)
            ]
            results = await asyncio.gather(*tasks)

            out_df = pd.DataFrame(results)
            out_df.to_csv(
                OUTPUT_FILE,
                mode="a",
                header=not OUTPUT_FILE.exists(),
                index=False,
                encoding="utf-8-sig",
            )
            batch_found = sum(1 for r in results if r.get("Email"))
            total_found += batch_found
            done_urls.update(r["Profile_URL"] for r in results)

    finally:
        await scraper.session.aclose()

    # Print summary breakdown
    final_df = pd.read_csv(OUTPUT_FILE, encoding="utf-8-sig")
    scraped   = (final_df["Email_Source"] == "scraped").sum()
    cand_info = (final_df["Email_Source"] == "candidate-info").sum()
    cand_name = (final_df["Email_Source"] == "candidate-name").sum()
    empty     = (final_df["Email"].isna() | (final_df["Email"] == "")).sum()

    print(f"\n{'='*60}")
    print(f"DONE  — {OUTPUT_FILE.name}")
    print(f"  Scraped (real)     : {scraped}")
    print(f"  Candidate info@    : {cand_info}")
    print(f"  Candidate firstname: {cand_name}")
    print(f"  No email found     : {empty}")
    print(f"  TOTAL WITH EMAIL   : {scraped + cand_info + cand_name}")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
