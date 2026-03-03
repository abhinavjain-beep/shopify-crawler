"""
Domain Extractor — fast Guru profile scraper
Visits every pending profile and saves the freelancer's website domain.
No email scraping, no MX check — just grab the URL and move on.
Output: freelancers_domains.csv  (Profile_URL, Domain)
"""

import re
import asyncio
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from scraper import Scraper

BASE_DIR    = Path(__file__).resolve().parent
INPUT_FILE  = BASE_DIR / "freelancers_emails_final.csv"
OUTPUT_FILE = BASE_DIR / "freelancers_domains.csv"

BATCH_SIZE  = 20
RATE        = 5   # req/sec — fast, we only need to grab one link per page

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

SKIP_DOMAINS = {
    "guru.com", "linkedin.com", "facebook.com", "twitter.com",
    "x.com", "instagram.com", "youtube.com", "github.com",
    "t.co", "behance.net", "dribbble.com", "clutch.co",
    "apple.com", "apps.apple.com", "play.google.com", "google.com",
    "yelp.com", "crunchbase.com", "upwork.com", "fiverr.com",
    "wa.me", "whatsapp.com", "telegram.me", "medium.com",
}
SOCIAL_CLASSES = {"socialIcon", "profile-web__social__icon", "c-footer__socials__social"}


def get_website_domain(soup: BeautifulSoup) -> str | None:
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href.startswith("http"):
            continue
        classes = set(a.get("class") or [])
        if classes & SOCIAL_CLASSES:
            continue
        try:
            host = href.split("/")[2]
        except IndexError:
            continue
        host = host.lstrip("www.").split(":")[0]
        if any(host == d or host.endswith("." + d) for d in SKIP_DOMAINS):
            continue
        if "." not in host:
            continue
        return host
    return None


async def extract_domain(row: dict, scraper: Scraper, idx: int, total: int) -> dict:
    soup = await scraper.get_soup(row["Profile_URL"], headers=HEADERS)
    domain = get_website_domain(soup) if soup else None
    status = domain or "—"
    print(f"  [{idx:4}/{total}]  {str(row.get('Name',''))[:28]:28}  {status}")
    return {"Profile_URL": row["Profile_URL"], "Domain": domain or ""}


async def main():
    df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")

    # Only process rows that don't already have a scraped email
    # (scraped rows we keep as-is; we need domains for the rest)
    pending = df[df["Email_Source"].fillna("") != "scraped"].to_dict("records")

    # Resume
    done_urls: set[str] = set()
    if OUTPUT_FILE.exists():
        done_df = pd.read_csv(OUTPUT_FILE, encoding="utf-8-sig")
        done_urls = set(done_df["Profile_URL"].dropna())
        print(f"Resuming — {len(done_urls)} already extracted.\n")

    to_process = [r for r in pending if r["Profile_URL"] not in done_urls]
    total = len(to_process)
    print(f"{total} profiles to visit for domain extraction.\n")
    if not total:
        print("Done.")
        return

    scraper = Scraper(requests_per_second=RATE, timeout=15)
    try:
        for batch_start in range(0, total, BATCH_SIZE):
            batch = to_process[batch_start: batch_start + BATCH_SIZE]
            tasks = [
                extract_domain(row, scraper, batch_start + i + len(done_urls) + 1, total)
                for i, row in enumerate(batch)
            ]
            results = await asyncio.gather(*tasks)
            out = pd.DataFrame(results)
            out.to_csv(OUTPUT_FILE, mode="a", header=not OUTPUT_FILE.exists(),
                       index=False, encoding="utf-8-sig")
            done_urls.update(r["Profile_URL"] for r in results)
    finally:
        await scraper.session.aclose()

    final = pd.read_csv(OUTPUT_FILE, encoding="utf-8-sig")
    with_domain = (final["Domain"] != "").sum()
    print(f"\nDone — {with_domain}/{len(final)} profiles have a website domain.")


if __name__ == "__main__":
    asyncio.run(main())
