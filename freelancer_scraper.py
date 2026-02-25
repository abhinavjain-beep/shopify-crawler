"""
Guru.com US Freelancer Scraper
Collects 1000 US-based freelancers across Web Development,
Performance Marketing, and Full Stack Development.

Data extracted directly from listing cards (no profile visits needed):
  Name, Title, Location, Bio, Skills, Hourly_Rate, Feedback,
  Earnings_Per_Year, Category, Profile_URL
"""

import re
import asyncio
import logging
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

from scraper import Scraper

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("freelancer_scraper")

BASE_DIR    = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "freelancers.csv"

BASE_URL     = "https://www.guru.com"
TARGET_TOTAL = 1000
MAX_PAGES    = 200   # per-category ceiling (20 results/page × 200 = 4 000 raw cards)

# Guru.com URL patterns for each target category
# (all have large pools and decent % US freelancers)
CATEGORIES = [
    {
        "name":  "Web Development",
        "url":   "https://www.guru.com/d/freelancers/c/programming-development/sc/web-development-design/",
        "quota": 500,   # target US freelancers from this category (dedup skips already-saved)
    },
    {
        "name":  "Performance Marketing",
        "url":   "https://www.guru.com/d/freelancers/c/sales-marketing/",
        "quota": 150,   # generous buffer above 100 already collected
    },
    {
        "name":  "Full Stack Development",
        "url":   "https://www.guru.com/d/freelancers/skill/full-stack/",  # 39k results vs 24k
        "quota": 500,
    },
    {
        "name":  "Full Stack Development",   # supplemental — different URL pool
        "url":   "https://www.guru.com/d/freelancers/c/programming-development/sc/programming-software/",
        "quota": 200,
    },
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
}


async def run_in_batches(tasks: list, max_concurrent: int = 10) -> list:
    results = []
    for i in range(0, len(tasks), max_concurrent):
        batch = tasks[i:i + max_concurrent]
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)
    return results


class GurufScraper:

    def __init__(self, proxies: list = None):
        self.scraper = Scraper(requests_per_second=3, timeout=30, proxies=proxies or [])
        self.headers = HEADERS
        self.seen_urls: set = set()
        self._csv_preloaded = False
        self._total_written = 0

    # ------------------------------------------------------------------ CSV

    def to_csv(self, data: dict):
        df = pd.json_normalize(data)
        df.to_csv(
            OUTPUT_FILE,
            mode="a",
            header=not OUTPUT_FILE.exists(),
            index=False,
            encoding="utf-8-sig",
        )
        self._total_written += 1

    # --------------------------------------------------------- Deduplication

    def _preload_seen(self):
        if not OUTPUT_FILE.exists():
            return
        try:
            df = pd.read_csv(OUTPUT_FILE, usecols=["Profile_URL"], encoding="utf-8-sig")
            self.seen_urls.update(df["Profile_URL"].dropna().tolist())
            self._total_written = len(self.seen_urls)
            logger.info(f"Resuming — {self._total_written} profiles already in CSV.")
        except Exception as e:
            logger.warning(f"Pre-load failed: {e}")

    def _is_new(self, url: str) -> bool:
        if not self._csv_preloaded:
            self._preload_seen()
            self._csv_preloaded = True
        return url not in self.seen_urls

    # -------------------------------------------- Parse one listing card

    @staticmethod
    def _text(el, selector: str) -> str:
        found = el.select_one(selector)
        return found.get_text(strip=True) if found else "N/A"

    def _parse_card(self, card, category: str) -> dict | None:
        """Extract all fields from a Guru.com freelancer listing card."""

        # Profile URL & Name
        name_link = card.select_one("h3.freelancerAvatar__screenName a")
        if not name_link:
            return None
        profile_url = BASE_URL + name_link.get("href", "")
        name = name_link.get_text(strip=True)

        # Location parts
        city    = self._text(card, ".freelancerAvatar__location--city").rstrip(",").strip()
        state   = self._text(card, ".freelancerAvatar__location--state").rstrip(",").strip()
        country = self._text(card, ".freelancerAvatar__location--country")

        # Only keep US freelancers
        if country != "United States":
            return None

        location = ", ".join(filter(lambda x: x and x != "N/A", [city, state, country]))

        # Service title (closest thing to "professional headline")
        title = self._text(card, ".serviceListing__title a")

        # Bio / description from the service card
        bio = self._text(card, ".serviceListing__desc")

        # Skills from the skill list (skip the first which is the category badge)
        skill_tags = card.select(".skillsList__skill a")
        skills = " | ".join(t.get_text(strip=True) for t in skill_tags) or "N/A"

        # Hourly rate (from service listing)
        rate_raw = self._text(card, ".serviceListing__rates")
        # extract just the hourly part: "$20/hr"
        rate_match = re.search(r'\$[\d,]+/hr', rate_raw)
        hourly_rate = rate_match.group(0) if rate_match else rate_raw

        # Feedback score
        feedback = self._text(card, ".freelancerAvatar__feedback")

        # Earnings per year
        earnings = self._text(card, ".earnings__amount")
        if earnings != "N/A":
            earnings = f"${earnings}/yr"

        return {
            "Name":            name,
            "Title":           title,
            "Location":        location,
            "Bio":             bio[:800],
            "Skills":          skills,
            "Hourly_Rate":     hourly_rate,
            "Feedback":        feedback,
            "Earnings_Per_Yr": earnings,
            "Category":        category,
            "Profile_URL":     profile_url,
        }

    # ----------------------------------------- Fetch + parse one search page

    async def fetch_page(self, url: str, page: int) -> tuple[list, bool, int]:
        """
        Fetch one Guru.com directory page.
        Returns (us_freelancer_dicts, has_next_page, raw_card_count).
        raw_card_count lets the caller distinguish "no US on this page"
        from "page was empty" (so it keeps paginating when needed).
        """
        page_url = url if page == 1 else f"{url}pg/{page}/"
        hdrs = {**self.headers, "Referer": url if page > 1 else BASE_URL + "/"}

        response = await self.scraper.get(page_url, headers=hdrs)
        if response is None:
            logger.warning(f"No response for {page_url}")
            return [], False, 0

        if response.status_code == 404:
            return [], False, 0

        if response.status_code != 200:
            logger.warning(f"HTTP {response.status_code} on {page_url}")
            return [], False, 0

        soup = BeautifulSoup(response.content, "html.parser")
        cards = soup.select("div.record.findGuruRecord")

        # Extract US freelancers from cards
        results = []
        for card in cards:
            data = self._parse_card(card, "")   # category injected by caller
            if data and self._is_new(data["Profile_URL"]):
                self.seen_urls.add(data["Profile_URL"])
                results.append(data)

        has_next = bool(cards) and page < MAX_PAGES
        return results, has_next, len(cards)

    # ----------------------------------------------- Per-category loop

    async def scrape_category(self, category: dict):
        name  = category["name"]
        url   = category["url"]
        quota = category["quota"]

        print(f"\n{'='*60}")
        print(f"Category : {name}")
        print(f"URL      : {url}")
        print(f"Quota    : {quota} US freelancers")
        print(f"{'='*60}")

        category_count = 0
        consecutive_empty = 0   # stop only after 3 consecutive empty pages

        for page in range(1, MAX_PAGES + 1):
            if category_count >= quota or self._total_written >= TARGET_TOTAL:
                break

            results, has_next, raw_count = await self.fetch_page(url, page)

            # Track consecutive empty pages to handle intermittent blank responses
            if raw_count == 0:
                consecutive_empty += 1
                print(f"  Page {page:3d} — empty (consecutive empty: {consecutive_empty})")
                if consecutive_empty >= 3:
                    print(f"  3 consecutive empty pages — stopping.")
                    break
                continue
            else:
                consecutive_empty = 0

            # Inject category name
            for r in results:
                r["Category"] = name

            # Write new records
            written_this_page = 0
            for r in results:
                if category_count >= quota or self._total_written >= TARGET_TOTAL:
                    break
                self.to_csv(r)
                category_count += 1
                written_this_page += 1
                print(
                    f"  [{self._total_written:4}/{TARGET_TOTAL}]"
                    f"  {r['Name'][:30]:30}"
                    f"  {r['Location'][:35]:35}"
                    f"  {r['Hourly_Rate']}"
                )

            print(
                f"  Page {page:3d} — raw={raw_count} cards,"
                f" {written_this_page} new US (category total: {category_count})"
            )

            if not has_next:
                print(f"  No more pages for '{name}'.")
                break

        print(f"\n  '{name}' done — {category_count} US freelancers written.")

    # -------------------------------------------------- Entry point

    async def main(self):
        print(f"Guru.com US Freelancer Scraper")
        print(f"Target   : {TARGET_TOTAL} US freelancers")
        print(f"Output   : {OUTPUT_FILE}")

        try:
            for cat in CATEGORIES:
                if self._total_written >= TARGET_TOTAL:
                    print(f"\nReached {TARGET_TOTAL} target — stopping.")
                    break
                await self.scrape_category(cat)
        finally:
            await self.scraper.session.aclose()

        print(f"\n{'='*60}")
        print(f"DONE — {self._total_written} US freelancers saved to:")
        print(f"  {OUTPUT_FILE}")
        print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(GurufScraper().main())
