"""
Email Extractor for Guru.com US Freelancers
Visits each profile URL from freelancers.csv, extracts contact email
from the profile page or linked personal website, and saves results to
freelancers_emails.csv.
"""

import re
import asyncio
import logging
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

from scraper import Scraper

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("email_extractor")

BASE_DIR    = Path(__file__).resolve().parent
INPUT_FILE  = BASE_DIR / "freelancers.csv"
OUTPUT_FILE = BASE_DIR / "freelancers_emails.csv"

EMAIL_RE   = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
BATCH_SIZE = 10
RATE       = 2

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Domains we don't want to follow as "personal websites"
SKIP_DOMAINS = {"guru.com", "linkedin.com", "facebook.com", "twitter.com",
                "instagram.com", "youtube.com", "github.com", "t.co"}


def extract_email_from_soup(soup: BeautifulSoup) -> str:
    """Try mailto: links first, then regex over visible text."""
    mailto = soup.select_one("a[href^='mailto:']")
    if mailto:
        return mailto["href"].replace("mailto:", "").strip().split("?")[0]
    match = EMAIL_RE.search(soup.get_text(" "))
    return match.group(0) if match else ""


def get_website_link(soup: BeautifulSoup) -> str | None:
    """Return the freelancer's personal website URL from their Guru profile."""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href.startswith("http"):
            continue
        # Skip guru.com internal links and social media
        domain = href.split("/")[2].lstrip("www.")
        if any(href.split("/")[2].endswith(d) for d in SKIP_DOMAINS):
            continue
        if "guru.com" in href:
            continue
        return href
    return None


async def fetch_email(row: dict, scraper: Scraper, index: int, total: int) -> dict:
    """Visit a profile URL (and optionally its linked website) to find an email."""
    profile_url = row["Profile_URL"]
    name = row.get("Name", "?")

    soup = await scraper.get_soup(profile_url, headers=HEADERS)
    if not soup:
        logger.warning(f"[{index}/{total}] No response — {name}")
        return {**row, "Email": ""}

    email = extract_email_from_soup(soup)

    # If no email on profile, try the personal website
    if not email:
        website = get_website_link(soup)
        if website:
            site_soup = await scraper.get_soup(website, headers=HEADERS)
            if site_soup:
                email = extract_email_from_soup(site_soup)

    status = f"✓ {email}" if email else "✗ not found"
    print(f"  [{index:4}/{total}]  {name[:30]:30}  {status}")
    return {**row, "Email": email}


async def run_in_batches(tasks: list, batch_size: int) -> list:
    results = []
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i + batch_size]
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)
    return results


async def main():
    # Load input
    df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
    rows = df.to_dict("records")
    total = len(rows)
    print(f"Loaded {total} freelancers from {INPUT_FILE}")

    # Resume support — skip already-processed profiles
    done_urls: set = set()
    if OUTPUT_FILE.exists():
        try:
            done_df = pd.read_csv(OUTPUT_FILE, usecols=["Profile_URL"], encoding="utf-8-sig")
            done_urls = set(done_df["Profile_URL"].dropna().tolist())
            print(f"Resuming — {len(done_urls)} already processed.")
        except Exception as e:
            logger.warning(f"Could not read existing output: {e}")

    pending = [r for r in rows if r["Profile_URL"] not in done_urls]
    print(f"{len(pending)} profiles to process.\n")

    if not pending:
        print("Nothing to do.")
        return

    scraper = Scraper(requests_per_second=RATE, timeout=30)

    try:
        tasks = [
            fetch_email(row, scraper, i + len(done_urls) + 1, total)
            for i, row in enumerate(pending)
        ]

        results = await run_in_batches(tasks, BATCH_SIZE)

        # Append to output CSV in one go
        out_df = pd.DataFrame(results)
        out_df.to_csv(
            OUTPUT_FILE,
            mode="a",
            header=not OUTPUT_FILE.exists(),
            index=False,
            encoding="utf-8-sig",
        )

    finally:
        await scraper.session.aclose()

    found = sum(1 for r in results if r.get("Email"))
    print(f"\n{'='*60}")
    print(f"Done — {found}/{len(results)} emails found.")
    print(f"Output: {OUTPUT_FILE}")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
