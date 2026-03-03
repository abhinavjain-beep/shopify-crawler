"""
Freelancer Finder
=================
Scrapes the Shopify Partners Directory for US-based freelancers / agencies
in web development, performance marketing, full-stack, and Shopify development.

Key insight: pagination works only with a shared httpx session + clean ?page=N URL.

Collects: Name, Email, LinkedIn, Phone, Website, Twitter, Facebook,
          Instagram, Youtube, Location, Shopify_Profile_URL

Target: 100 records where (Email OR LinkedIn) AND location is United States.
"""

import asyncio
import csv
import re
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

BASE_DIR    = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "freelancers.csv"

BASE_URL      = "https://www.shopify.com"
DIRECTORY_URL = f"{BASE_URL}/partners/directory/services"

TARGET     = 100
MAX_PAGES  = 400         # hard ceiling
BATCH_SIZE = 16          # concurrent profile fetches

HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.5",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) "
        "Gecko/20100101 Firefox/131.0"
    ),
}

CSV_FIELDS = [
    "Name", "Email", "LinkedIn", "Phone", "Website",
    "Twitter", "Facebook", "Instagram", "Youtube",
    "Location", "Shopify_Profile_URL",
]

_US_RE = re.compile(
    r"\b(United States|USA|"
    r"Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|"
    r"Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|"
    r"Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|"
    r"Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|"
    r"New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|"
    r"Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|"
    r"Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming|"
    r"District of Columbia|D\.C\.)\b"
)

# ---------------------------------------------------------------------------
# Location extraction (multiple fallbacks)
# ---------------------------------------------------------------------------

def extract_location(soup) -> str:
    selectors = [
        "div.flex.flex-col.gap-y-1:-soup-contains('Primary location') p:nth-child(2)",
        "div.flex.flex-col.gap-y-1:-soup-contains('Primary Location') p:nth-child(2)",
        "div:-soup-contains('Primary location') p",
    ]
    for sel in selectors:
        try:
            tag = soup.select_one(sel)
            if tag:
                return tag.get_text(strip=True)
        except Exception:
            pass

    # Fallback: find leaf nodes matching US patterns
    for tag in soup.find_all(["p", "span"], limit=600):
        if tag.find_all(recursive=False):
            continue
        text = tag.get_text(strip=True)
        if 2 < len(text) < 80 and _US_RE.search(text):
            return text

    return ""


def is_us(location: str) -> bool:
    return bool(location and _US_RE.search(location))


# ---------------------------------------------------------------------------
# Profile parser
# ---------------------------------------------------------------------------

def parse_profile(soup, url: str) -> dict:
    data = {f: "" for f in CSV_FIELDS}
    data["Shopify_Profile_URL"] = url

    title = soup.select_one("h1.richtext.text-t4") or soup.select_one("h1")
    data["Name"] = title.get_text(strip=True) if title else ""

    phone_tag = soup.select_one("a[href*='tel:']")
    data["Phone"] = phone_tag["href"].replace("tel:", "").strip() if phone_tag else ""

    email_tag = soup.select_one("a[href*='mailto:']")
    data["Email"] = email_tag["href"].replace("mailto:", "").strip() if email_tag else ""

    website_tag = soup.select_one(
        "div.flex.flex-wrap.gap-x-2.items-center a[rel='nofollow']"
    )
    data["Website"] = website_tag["href"].strip() if website_tag else ""

    data["Location"] = extract_location(soup)

    # Socials — scoped to the partner's "Social links" section only
    social_section = None
    for sel in [
        "div.flex.flex-col.gap-y-1:-soup-contains('Social links')",
        "div:-soup-contains('Social links')",
    ]:
        try:
            social_section = soup.select_one(sel)
            if social_section:
                break
        except Exception:
            pass

    social_links = social_section.select("a[href]") if social_section else []

    for a in social_links:
        href = a.get("href", "")
        if not href:
            continue
        if "linkedin.com" in href and not data["LinkedIn"]:
            data["LinkedIn"] = href
        elif ("twitter.com" in href or "/x.com/" in href) and not data["Twitter"]:
            data["Twitter"] = href
        elif "instagram.com" in href and not data["Instagram"]:
            data["Instagram"] = href
        elif "facebook.com" in href and not data["Facebook"]:
            data["Facebook"] = href
        elif "youtube.com" in href and not data["Youtube"]:
            data["Youtube"] = href

    return data


def has_contact(d: dict) -> bool:
    return bool(d["Email"]) or bool(d["LinkedIn"])


# ---------------------------------------------------------------------------
# CSV writer with deduplication
# ---------------------------------------------------------------------------

class CSVWriter:
    def __init__(self, path: Path):
        self._path   = path
        self._seen   = set()
        self._count  = 0
        self._fh     = None
        self._writer = None

    def open(self):
        self._fh = open(self._path, "w", newline="", encoding="utf-8-sig")
        self._writer = csv.DictWriter(self._fh, fieldnames=CSV_FIELDS)
        self._writer.writeheader()

    def close(self):
        if self._fh:
            self._fh.close()

    def write(self, row: dict) -> bool:
        key = row.get("Shopify_Profile_URL", "")
        if key in self._seen:
            return False
        self._seen.add(key)
        self._writer.writerow({f: row.get(f, "") for f in CSV_FIELDS})
        self._fh.flush()
        self._count += 1
        return True

    @property
    def count(self) -> int:
        return self._count


# ---------------------------------------------------------------------------
# Network helpers  (shared httpx session is REQUIRED for pagination to work)
# ---------------------------------------------------------------------------

async def fetch_html(client: httpx.AsyncClient, url: str) -> BeautifulSoup | None:
    try:
        r = await client.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return BeautifulSoup(r.content, "html.parser")
    except Exception:
        pass
    return None


async def get_listing_page(client: httpx.AsyncClient, page: int):
    url  = f"{DIRECTORY_URL}?page={page}"
    soup = await fetch_html(client, url)
    if not soup:
        return [], False
    cards    = soup.select('[data-component-name="listing-profile-card"] a[href]')
    urls     = [BASE_URL + a["href"] for a in cards if a.get("href")]
    nxt      = soup.find(attrs={"data-component-name": "next-page"})
    has_next = nxt is not None and nxt.get("aria-disabled") != "true"
    return urls, has_next


async def get_profile(client: httpx.AsyncClient, url: str) -> dict | None:
    soup = await fetch_html(client, url)
    if not soup:
        return None
    return parse_profile(soup, url)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    writer       = CSVWriter(OUTPUT_FILE)
    writer.open()
    visited      = set()
    total_scanned = 0

    print(f"Target : {TARGET} US-based Shopify partners with email/LinkedIn")
    print(f"Output : {OUTPUT_FILE}\n")

    # Semaphore limits concurrent profile fetches to avoid overloading the server
    sem = asyncio.Semaphore(BATCH_SIZE)

    async def fetch_limited(client, url):
        async with sem:
            return await get_profile(client, url)

    try:
        # A SINGLE shared AsyncClient is essential — cookie/session state enables pagination
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
            for page in range(1, MAX_PAGES + 1):
                if writer.count >= TARGET:
                    break

                listing_urls, has_next = await get_listing_page(client, page)
                new_urls = [u for u in listing_urls if u not in visited]
                visited.update(listing_urls)

                print(
                    f"Page {page:>3}: {len(listing_urls):>2} listings, "
                    f"{len(new_urls):>2} new  [saved {writer.count}/{TARGET}]"
                )

                if not new_urls and page > 1:
                    print("No new listings — reached end of directory.")
                    break

                # Fetch all profiles on this page concurrently
                tasks   = [fetch_limited(client, u) for u in new_urls]
                results = await asyncio.gather(*tasks)

                for data in results:
                    if data is None:
                        continue
                    total_scanned += 1
                    if not is_us(data["Location"]):
                        continue
                    if not has_contact(data):
                        continue
                    if writer.write(data):
                        print(
                            f"  ✔ [{writer.count:>3}] "
                            f"{data['Name'][:45]:<45} "
                            f"{'✉ ' if data['Email'] else '  '}"
                            f"{'🔗' if data['LinkedIn'] else '  '} "
                            f"{data['Location']}"
                        )
                    if writer.count >= TARGET:
                        break

                if not has_next:
                    print("Last page of directory reached.")
                    break

    finally:
        writer.close()

    print(f"\n{'='*60}")
    print(f"Scanned : {total_scanned} profiles")
    print(f"Saved   : {writer.count} records → {OUTPUT_FILE}")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
