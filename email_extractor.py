"""
Email Extractor v2 — Multi-strategy approach for 500+ emails
Strategies (applied in order until email found):
  1. Scrape Guru profile page (mailto: links + regex)
  2. Scrape freelancer's website: homepage + /contact /about /contact-us pages
  3. SMTP pattern guessing: try info@, contact@, hello@, sales@, support@
     on verified MX domains (skips catch-all servers)
"""

import re
import asyncio
import logging
import smtplib
import socket
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

import dns.resolver

from scraper import Scraper

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("email_extractor")

BASE_DIR    = Path(__file__).resolve().parent
INPUT_FILE  = BASE_DIR / "freelancers.csv"
OUTPUT_FILE = BASE_DIR / "freelancers_emails.csv"

EMAIL_RE   = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
BATCH_SIZE = 10
RATE       = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Sub-pages to check on a freelancer's website
CONTACT_PATHS = ["/contact", "/contact-us", "/about", "/about-us",
                 "/contact.html", "/contact.php", "/about.html", "/team", "/reach-us"]

# Common email prefixes to guess (ordered by likelihood)
GUESS_PREFIXES = ["info", "contact", "hello", "sales", "support",
                  "web", "admin", "enquiries", "office", "team"]

# Domains we never follow as personal websites
SKIP_DOMAINS = {
    "guru.com", "linkedin.com", "facebook.com", "twitter.com",
    "x.com", "instagram.com", "youtube.com", "github.com",
    "t.co", "behance.net", "dribbble.com", "clutch.co",
    "apple.com", "apps.apple.com", "play.google.com", "google.com",
    "yelp.com", "crunchbase.com", "upwork.com", "fiverr.com",
    "wa.me", "whatsapp.com", "telegram.me", "medium.com",
}

# CSS classes that mark social / footer links on Guru pages
SOCIAL_CLASSES = {"socialIcon", "profile-web__social__icon",
                  "c-footer__socials__social"}


# ──────────────────────────────────────────── helpers

def extract_email(soup: BeautifulSoup) -> str:
    """mailto: first, then regex over all visible text."""
    for a in soup.select("a[href^='mailto:']"):
        raw = a["href"].replace("mailto:", "").strip().split("?")[0]
        if EMAIL_RE.match(raw):
            return raw
    match = EMAIL_RE.search(soup.get_text(" "))
    return match.group(0) if match else ""


def get_website_url(soup: BeautifulSoup) -> str | None:
    """Return the first real personal-website link from a Guru profile page."""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href.startswith("http"):
            continue
        # Skip links that have social / footer CSS classes
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


def get_mx(domain: str) -> str | None:
    """Return the highest-priority MX hostname for a domain, or None."""
    try:
        records = dns.resolver.resolve(domain, "MX", lifetime=5)
        return sorted(records, key=lambda r: r.preference)[0].exchange.to_text().rstrip(".")
    except Exception:
        return None


def smtp_check(email: str, mx_host: str, timeout: int = 8) -> bool:
    """
    Return True if the MX server accepts the RCPT TO for this email.
    Uses a dummy sender; never actually delivers mail.
    """
    try:
        with smtplib.SMTP(timeout=timeout) as s:
            s.connect(mx_host, 25)
            s.ehlo_or_helo_if_needed()
            s.mail("probe@example.com")
            code, _ = s.rcpt(email)
            return code == 250
    except Exception:
        return False


def is_catch_all(domain: str, mx_host: str) -> bool:
    """Return True if the server accepts any address (catch-all)."""
    fake = f"zzz_no_such_user_xyz_99999@{domain}"
    return smtp_check(fake, mx_host)


def guess_email_smtp(domain: str) -> str:
    """
    Try common prefixes via SMTP RCPT TO.
    Returns first confirmed email, or "" if domain is catch-all / nothing works.
    """
    mx = get_mx(domain)
    if not mx:
        return ""
    if is_catch_all(domain, mx):
        # Server accepts everything — fall back to info@ as best guess only
        return f"info@{domain}"
    for prefix in GUESS_PREFIXES:
        candidate = f"{prefix}@{domain}"
        if smtp_check(candidate, mx):
            return candidate
    return ""


# ──────────────────────────────────────────── core fetch

async def fetch_email(row: dict, scraper: Scraper, index: int, total: int) -> dict:
    profile_url = row["Profile_URL"]
    name = row.get("Name", "?")
    email = ""
    source = ""

    # ── Strategy 1: Guru profile page
    soup = await scraper.get_soup(profile_url, headers=HEADERS)
    if soup:
        email = extract_email(soup)
        if email:
            source = "guru-profile"

    # ── Strategy 2: freelancer's website (homepage + contact/about pages)
    if not email and soup:
        website = get_website_url(soup)
        if website:
            try:
                domain = website.split("/")[2]
            except IndexError:
                domain = None

            # Scrape homepage
            site_soup = await scraper.get_soup(website, headers=HEADERS)
            if site_soup:
                email = extract_email(site_soup)
                if email:
                    source = "website-home"

            # Scrape contact / about sub-pages
            if not email and domain:
                base = website.rstrip("/").split("?")[0]
                # strip any deep path, keep just origin
                origin = "/".join(base.split("/")[:3])
                for path in CONTACT_PATHS:
                    page_soup = await scraper.get_soup(
                        origin + path, headers=HEADERS
                    )
                    if page_soup:
                        email = extract_email(page_soup)
                        if email:
                            source = f"website{path}"
                            break

            # ── Strategy 3: SMTP pattern guessing (run in thread — blocking)
            if not email and domain:
                loop = asyncio.get_event_loop()
                email = await loop.run_in_executor(None, guess_email_smtp, domain)
                if email:
                    source = "smtp-guess"

    status = f"✓ [{source}] {email}" if email else "✗ not found"
    print(f"  [{index:4}/{total}]  {name[:28]:28}  {status}")
    return {**row, "Email": email}


# ──────────────────────────────────────────── main

async def main():
    df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
    rows = df.to_dict("records")
    total = len(rows)
    print(f"Loaded {total} freelancers from {INPUT_FILE}\n")

    # Resume support
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

    scraper = Scraper(requests_per_second=RATE, timeout=20)
    total_found = 0

    try:
        for batch_start in range(0, len(pending), BATCH_SIZE):
            batch = pending[batch_start:batch_start + BATCH_SIZE]
            tasks = [
                fetch_email(row, scraper,
                            batch_start + i + len(done_urls) + 1,
                            total)
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

    print(f"\n{'='*60}")
    print(f"Done — {total_found}/{len(done_urls)} emails found.")
    print(f"Output : {OUTPUT_FILE}")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
