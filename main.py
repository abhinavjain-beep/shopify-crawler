import asyncio
import aiofiles
import pandas as pd
from pathlib import Path

import httpx
from scraper import Scraper

BASE_DIR   = Path(__file__).resolve().parent
URLS_FILE  = BASE_DIR / "urls.txt"
OUTPUT_FILE = BASE_DIR / "data.csv"

# Countries we want — matched against the agency's Primary Location text
TARGET_COUNTRIES = {
    "United States",
    "United Kingdom",
    "Australia",
}


async def run_in_batches(tasks, max_concurrent_tasks: int = 10):
    results = []
    for i in range(0, len(tasks), max_concurrent_tasks):
        batch = tasks[i:i + max_concurrent_tasks]
        print(f"  Batch {i // max_concurrent_tasks + 1}/{(len(tasks) - 1) // max_concurrent_tasks + 1}: {len(batch)} tasks")
        results.extend(await asyncio.gather(*batch))
    return results


class Shopify:

    BASE_URL      = "https://www.shopify.com"
    DIRECTORY_URL = "https://www.shopify.com/partners/directory/services"
    HEADERS = {
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    }

    def __init__(self):
        self.profile_scraper = Scraper(requests_per_second=8)
        self.scraped_urls: set = set()

    # ------------------------------------------------------------------ I/O --

    async def load_scraped_urls(self) -> set:
        if not URLS_FILE.exists():
            return set()
        async with aiofiles.open(URLS_FILE, "r") as f:
            lines = await f.readlines()
        return {l.strip() for l in lines if l.strip()}

    async def mark_done(self, url: str):
        self.scraped_urls.add(url)
        async with aiofiles.open(URLS_FILE, "a") as f:
            await f.write(url + "\n")

    def to_csv(self, row: dict):
        pd.json_normalize(row).to_csv(
            OUTPUT_FILE, mode="a",
            header=not OUTPUT_FILE.exists(),
            index=False, encoding="utf-8-sig",
        )

    # ------------------------------------------------------- listing phase ---

    async def _fetch_listing_page(self, page: int) -> bytes | None:
        """Fetch one listing page with a brand-new HTTP client (no shared session state)."""
        url = f"{self.DIRECTORY_URL}?sort=DEFAULT" + (f"&page={page}" if page > 1 else "")
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
                resp = await client.get(url, headers=self.HEADERS)
                return resp.content
        except Exception as e:
            print(f"  Error fetching listing page {page}: {e}")
            return None

    async def collect_all_listing_urls(self) -> list[str]:
        """
        Phase 1 — fetch every listing page using a fresh HTTP client per request
        (eliminates server-side session/cache issues that return stale pages).

        Stop conditions (whichever comes first):
          1. Page returns 0 cards
          2. has_next button is disabled
          3. Cycle detected (any URL already seen this session)
        """
        from bs4 import BeautifulSoup
        all_urls: list[str] = []
        seen: set[str] = set()
        page = 1
        while True:
            content = await self._fetch_listing_page(page)
            if not content:
                print(f"  Error on listing page {page}, stopping.")
                break
            soup = BeautifulSoup(content, "html.parser")
            cards = soup.select('[data-component-name="listing-profile-card"] a[href]')
            if not cards:
                print(f"  No cards on page {page} — done collecting.")
                break
            hrefs = [self.BASE_URL + a["href"] for a in cards]

            if any(h in seen for h in hrefs):
                print(f"  Cycle detected on page {page} — done collecting.")
                break

            seen.update(hrefs)
            all_urls.extend(hrefs)
            next_btn = soup.find(attrs={"data-component-name": "next-page"})
            has_next = next_btn is not None and next_btn.get("aria-disabled") != "true"
            print(f"  Listing page {page}: {len(cards)} cards | total so far: {len(all_urls)}")
            if not has_next:
                print(f"  Last page reached (has_next=False).")
                break
            page += 1
            await asyncio.sleep(0.5)   # polite delay between pages
        return all_urls

    # ------------------------------------------------------ profile phase ---

    def _country_from_location(self, location_text: str) -> str | None:
        """Return the matched country name or None."""
        for country in TARGET_COUNTRIES:
            if country in location_text:
                return country
        return None

    def _extract_partner_tier(self, soup) -> str:
        for tier in ("Platinum", "Premier", "Plus", "Select"):
            if soup.find(string=lambda t, k=tier: t and f"{k} tier" in t):
                return f"{tier} Partner"
        badge = soup.find(attrs={"data-partner-tier": True})
        return badge["data-partner-tier"] if badge else "X"

    async def scrape_profile(self, url: str):
        """Fetch one agency profile. Returns dict if country matches, else None."""
        if url in self.scraped_urls:
            return None

        soup = await self.profile_scraper.get_soup(url, headers=self.HEADERS)
        if not soup:
            print(f"  Error fetching {url}")
            return None

        location_el = soup.select_one(
            "div.flex.flex-col.gap-y-1:-soup-contains('Primary location') p:nth-child(2)"
        )
        location_text = location_el.get_text(strip=True) if location_el else ""
        country = self._country_from_location(location_text)

        # Mark visited regardless so we don't retry on resume
        await self.mark_done(url)

        if not country:
            return None   # not a target country — skip

        LinkedIn = Instagram = Facebook = Twitter = Youtube = ""
        for social in soup.select("div.flex.flex-col.gap-y-1:-soup-contains('Social links') a"):
            href = social.get("href", "")
            if "linkedin.com" in href:     LinkedIn   = href
            elif "instagram"  in href:     Instagram  = href
            elif "facebook"   in href:     Facebook   = href
            elif "twitter"    in href or "x.com" in href: Twitter = href
            elif "youtube"    in href:     Youtube    = href

        title    = soup.select_one("h1.richtext.text-t4")
        desc     = soup.select_one("section[data-section-name='description']")
        phone    = soup.select_one("a[href*='tel:']")
        email    = soup.select_one("a[href*='mailto:']")
        website  = soup.select_one("div.flex.flex-wrap.gap-x-2.items-center a[rel='nofollow']")
        languages = soup.select_one(
            "div.flex.flex-col.gap-y-1:-soup-contains('Languages') p:nth-child(2)"
        )

        row = {
            "Name":         title.get_text(strip=True) if title else "X",
            "Country":      country,
            "Partner Tier": self._extract_partner_tier(soup),
            "Description":  desc.get_text(strip=True) if desc else "X",
            "Phone Number": phone["href"].replace("tel:", "") if phone else "X",
            "Website":      website["href"] if website else "X",
            "Email":        email["href"].replace("mailto:", "") if email else "X",
            "LinkedIn":     LinkedIn  or "X",
            "Location":     location_text or "X",
            "Languages":    languages.get_text(strip=True) if languages else "X",
            "URL":          url,
            "Instagram":    Instagram or "X",
            "Facebook":     Facebook  or "X",
            "Twitter":      Twitter   or "X",
            "Youtube":      Youtube   or "X",
        }
        self.to_csv(row)
        return row

    # ----------------------------------------------------------------- main --

    async def main(self):
        self.scraped_urls = await self.load_scraped_urls()
        print(f"Resuming — {len(self.scraped_urls)} URLs already done.\n")

        # Phase 1 — collect every listing URL (fresh session, no contamination)
        print("=== Phase 1: Collecting all listing URLs ===")
        all_urls = await self.collect_all_listing_urls()
        unique   = list(dict.fromkeys(all_urls))
        new_urls = [u for u in unique if u not in self.scraped_urls]
        print(f"\nTotal unique URLs: {len(unique)}  |  New to scrape: {len(new_urls)}\n")

        # Phase 2 — scrape profiles, keep only US / UK / AU
        print("=== Phase 2: Scraping profiles (filter: US / UK / AU) ===")
        tasks   = [self.scrape_profile(u) for u in new_urls]
        results = await run_in_batches(tasks, max_concurrent_tasks=10)

        saved = [r for r in results if r]
        print(f"\nDone!  Saved {len(saved)} agencies to {OUTPUT_FILE}")
        await self.profile_scraper.session.aclose()


if __name__ == "__main__":
    asyncio.run(Shopify().main())
