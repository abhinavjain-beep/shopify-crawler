import asyncio
import aiofiles
import pandas as pd
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from scraper import Scraper

BASE_DIR  =  Path(__file__).resolve().parent

URLS_FILE = BASE_DIR / "urls.txt"
OUPTUT_FILE = BASE_DIR / "data.csv"

# All location codes discovered from the Shopify partner directory page
ALL_LOCATION_CODES = [
    'loc-us', 'loc-gb', 'loc-ca', 'loc-au', 'loc-de', 'loc-fr', 'loc-nl',
    'loc-in', 'loc-sg', 'loc-ae', 'loc-br', 'loc-mx', 'loc-es', 'loc-it',
    'loc-se', 'loc-no', 'loc-dk', 'loc-fi', 'loc-ch', 'loc-at', 'loc-be',
    'loc-pt', 'loc-pl', 'loc-cz', 'loc-hu', 'loc-ro', 'loc-bg', 'loc-sk',
    'loc-ee', 'loc-lv', 'loc-lt', 'loc-ie', 'loc-nz', 'loc-hk', 'loc-jp',
    'loc-kr', 'loc-tw', 'loc-cn', 'loc-th', 'loc-my', 'loc-id', 'loc-ph',
    'loc-vn', 'loc-lk', 'loc-pk', 'loc-bd', 'loc-np', 'loc-eg', 'loc-za',
    'loc-ng', 'loc-ma', 'loc-il', 'loc-tr', 'loc-ua', 'loc-ru', 'loc-by',
    'loc-rs', 'loc-gr', 'loc-cy', 'loc-lu', 'loc-is', 'loc-ad', 'loc-sm',
    'loc-xk', 'loc-ba', 'loc-ar', 'loc-cl', 'loc-co', 'loc-pe', 'loc-ec',
    'loc-gt', 'loc-pa', 'loc-lb', 'loc-bh', 'loc-kw',
]


async def run_in_batches(tasks, max_concurrent_tasks: int = 10):
    results = []
    for i in range(0, len(tasks), max_concurrent_tasks):
        batch = tasks[i:i + max_concurrent_tasks]
        batch_num = i // max_concurrent_tasks + 1
        total_batches = (len(tasks) + max_concurrent_tasks - 1) // max_concurrent_tasks
        print(f"Running batch {batch_num}/{total_batches}: {len(batch)} tasks")
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)
        print(f"Finished batch {batch_num} | Scraped so far: {i + len(batch)}")
    return results


class Shopify:

    def __init__(self) -> None:
        self.scraper = Scraper()
        self.base_url = "https://www.shopify.com"
        self.directory_url = "https://www.shopify.com/partners/directory/services"
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0"
        }
        self.first_write = True

    async def store_url(self, url: str):
        async with aiofiles.open(URLS_FILE, "a") as f:
            await f.write(url + "\n")

    def to_csv(self, data: dict):
        df = pd.json_normalize(data)
        if self.first_write:
            df.to_csv(OUPTUT_FILE, mode="w", header=True, index=False, encoding="utf-8-sig")
            self.first_write = False
        else:
            df.to_csv(OUPTUT_FILE, mode="a", header=False, index=False, encoding="utf-8-sig")

    async def url_handler(self, url: str):
        retries = 3
        soup = None
        for attempt in range(retries):
            soup = await self.scraper.get_soup(url, headers=self.headers)
            if soup:
                break
            print(f"Retry {attempt + 1}/{retries} for {url}")
            await asyncio.sleep(2)

        if not soup:
            print(f"Failed after {retries} attempts: {url}")
            return None

        LinkedIn = ""

        title = soup.select_one("h1.richtext.text-t4")
        email = soup.select_one("a[href*='mailto:']")
        location = soup.select_one("div.flex.flex-col.gap-y-1:-soup-contains('Primary location') p:nth-child(2)")

        # Partner tier badge: <title id="*-badge-title">Select tier</title>
        tier_title = soup.select_one("title[id$='-badge-title']")
        partner_plan = tier_title.get_text(strip=True).replace(" tier", "").title() if tier_title else ""

        # Country: last part of location string (e.g. "Gretna, United States" → "United States")
        location_text = location.get_text(strip=True) if location else ""
        country = location_text.split(",")[-1].strip() if location_text else ""

        socials = soup.select("div.flex.flex-col.gap-y-1:-soup-contains('Social links') a")
        for social in socials:
            href = social.get('href', '')
            if "linkedin.com" in href:
                LinkedIn = href

        data = {
            "Agency Name": title.get_text(strip=True) if title else "",
            "Shopify Tier": partner_plan,
            "Country": country,
            "Location": location_text,
            "Email": email.get('href').replace('mailto:', '') if email else "",
            "LinkedIn": LinkedIn,
            "Shopify Page": url,
        }

        await self.store_url(url)
        self.to_csv(data)
        return data

    async def get_page_urls(self, location: str, page: int):
        """Fetch agency card URLs for one location, one page."""
        url = f"{self.directory_url}?locationCodes={location}&page={page}"
        async with httpx.AsyncClient(follow_redirects=True) as client:
            try:
                resp = await client.get(url, headers=self.headers, timeout=30)
                soup = BeautifulSoup(resp.content, 'html.parser')
            except Exception as e:
                print(f"Error fetching {url}: {e}")
                return [], False
        cards = soup.select('[data-component-name="listing-profile-card"] a[href]')
        next_el = soup.find(attrs={'data-component-name': 'next-page'})
        has_next = next_el is not None and next_el.get('aria-disabled') != 'true'
        return [self.base_url + card.get('href') for card in cards], has_next

    async def collect_for_location(self, location: str, global_seen: set, idx: int, total: int):
        """Paginate all pages for a location and return new unique URLs."""
        new_urls = []
        page = 1
        while True:
            page_urls, has_next = await self.get_page_urls(location, page)
            added = [u for u in page_urls if u not in global_seen]
            global_seen.update(added)
            new_urls.extend(added)
            if not has_next:
                break
            page += 1
            await asyncio.sleep(0.4)
        print(f"  [{idx}/{total}] {location}: {len(new_urls)} new agencies (paginated {page} page(s))")
        return new_urls

    async def main(self):
        # Clean start
        if OUPTUT_FILE.exists():
            OUPTUT_FILE.unlink()
        if URLS_FILE.exists():
            URLS_FILE.unlink()

        location_codes = list(dict.fromkeys(ALL_LOCATION_CODES))

        print("Phase 1: Collecting agency URLs for all countries")
        print(f"  Locations to crawl: {len(location_codes)}\n")

        global_seen: set = set()
        all_urls: list = []

        for idx, loc in enumerate(location_codes, 1):
            urls = await self.collect_for_location(loc, global_seen, idx, len(location_codes))
            all_urls.extend(urls)
            await asyncio.sleep(0.3)

        # Sweep without location filter to catch agencies not tied to any country
        print("\nPhase 1b: No-location sweep (catches agencies with no country set)...")
        page = 1
        no_loc_new = 0
        while True:
            url = f"{self.directory_url}?page={page}"
            async with httpx.AsyncClient(follow_redirects=True) as client:
                try:
                    resp = await client.get(url, headers=self.headers, timeout=30)
                    soup = BeautifulSoup(resp.content, 'html.parser')
                except Exception as e:
                    print(f"Error fetching page {page}: {e}")
                    break
            cards = soup.select('[data-component-name="listing-profile-card"] a[href]')
            next_el = soup.find(attrs={'data-component-name': 'next-page'})
            has_next = next_el is not None and next_el.get('aria-disabled') != 'true'
            added = [self.base_url + c.get('href') for c in cards
                     if (self.base_url + c.get('href')) not in global_seen]
            global_seen.update(added)
            all_urls.extend(added)
            no_loc_new += len(added)
            if not has_next or not cards:
                break
            page += 1
            await asyncio.sleep(0.4)
        print(f"  No-location sweep: {no_loc_new} additional agencies found (swept {page} pages)")

        print(f"\nPhase 1 complete: {len(all_urls)} unique agency URLs collected.\n")

        # Phase 2: scrape each agency detail page
        print("Phase 2: Scraping agency detail pages...")
        tasks = [self.url_handler(url) for url in all_urls]
        await run_in_batches(tasks, max_concurrent_tasks=10)

        print(f"\nDone! Data saved to {OUPTUT_FILE}")
        df = pd.read_csv(OUPTUT_FILE)
        print(f"Total agencies in CSV: {len(df)}")


if __name__ == "__main__":
    shopify = Shopify()
    asyncio.run(shopify.main())
