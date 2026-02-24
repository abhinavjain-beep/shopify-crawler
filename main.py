import os
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



async def run_in_batches(tasks, max_concurrent_tasks : int = 10):
    """
    Runs tasks in batches of the specified size and collects their results.

    Returns:
        List of results returned by the tasks.
    """
    results = []
    for i in range(0, len(tasks), max_concurrent_tasks):
        batch = tasks[i:i + max_concurrent_tasks]
        print(f"Running batch {i // max_concurrent_tasks + 1}/{(len(tasks) + max_concurrent_tasks - 1) // max_concurrent_tasks}: {len(batch)} tasks")
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)
        print(f"Finished batch {i // max_concurrent_tasks + 1} | Scraped so far: {i + len(batch)}")

    return results

class Shopify:

    def __init__(self) -> None:
        self.scraper = Scraper()
        self.base_url = "https://www.shopify.com"
        self.page_url = "https://www.shopify.com/partners/directory/services?locationCodes=loc-us"
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0"
        }
        self.first_write = True

    async def store_url(self, url: str):
        async with aiofiles.open(URLS_FILE, "a") as f:
            await f.write(url + "\n")

    async def load_urls(self):
        if not URLS_FILE.exists():
            return set()
        async with aiofiles.open(URLS_FILE, "r") as f:
            lines = await f.readlines()
            return set(line.strip() for line in lines if line.strip())

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

        data = {}

        LinkedIn = ""
        Instagram = ""
        Facebook = ""
        Twitter = ""
        Youtube = ""

        title = soup.select_one("h1.richtext.text-t4")
        description = soup.select_one("section[data-section-name='description']")
        phone = soup.select_one("a[href*='tel:']")
        email = soup.select_one("a[href*='mailto:']")
        website = soup.select_one("div.flex.flex-wrap.gap-x-2.items-center a[rel='nofollow']")
        location = soup.select_one("div.flex.flex-col.gap-y-1:-soup-contains('Primary location') p:nth-child(2)")
        languages = soup.select_one("div.flex.flex-col.gap-y-1:-soup-contains('Languages') p:nth-child(2)")

        # Social links are JS-rendered; attempt to parse but will usually be empty
        socials = soup.select("div.flex.flex-col.gap-y-1:-soup-contains('Social links') a")
        for social in socials:
            href = social.get('href', '')
            if "linkedin.com" in href:
                LinkedIn = href
            elif "instagram" in href:
                Instagram = href
            elif "facebook" in href:
                Facebook = href
            elif "twitter" in href or "x.com" in href:
                Twitter = href
            elif "youtube" in href:
                Youtube = href

        data["Name"] = title.get_text(strip=True) if title else ""
        data["Description"] = description.get_text(strip=True) if description else ""
        data["Phone Number"] = phone.get('href').replace('tel:', '') if phone else ""
        data["Website"] = website.get('href') if website else ""
        data["Email"] = email.get('href').replace('mailto:', '') if email else ""
        data["Location"] = location.get_text(strip=True) if location else ""
        data["Languages"] = languages.get_text(strip=True) if languages else ""
        data["LinkedIn"] = LinkedIn
        data["Instagram"] = Instagram
        data["Facebook"] = Facebook
        data["Twitter"] = Twitter
        data["Youtube"] = Youtube
        data["URL"] = url

        await self.store_url(url)
        self.to_csv(data)
        return data

    async def get_page_urls(self, page: int):
        url = self.page_url + f"&page={page}"
        async with httpx.AsyncClient(follow_redirects=True) as client:
            try:
                resp = await client.get(url, headers=self.headers, timeout=30)
                soup = BeautifulSoup(resp.content, 'html.parser')
            except Exception as e:
                print(f"Error getting page {page}: {e}")
                return [], False
        cards = soup.select('[data-component-name="listing-profile-card"] a[href]')
        next_page = soup.find(attrs={'data-component-name': 'next-page'})
        has_next = next_page is not None and next_page.get('aria-disabled') != 'true'
        return [self.base_url + card.get('href') for card in cards], has_next

    async def main(self):
        # Clean start — remove old output files
        if OUPTUT_FILE.exists():
            OUPTUT_FILE.unlink()
        if URLS_FILE.exists():
            URLS_FILE.unlink()

        # Phase 1: collect all agency URLs across all pages
        print("Phase 1: Collecting agency URLs from all pages...")
        all_urls = []
        seen = set()
        index = 1
        while True:
            urls, has_next = await self.get_page_urls(index)
            new_urls = [u for u in urls if u not in seen]
            seen.update(new_urls)
            all_urls.extend(new_urls)
            print(f"  Page {index}: found {len(urls)} listings ({len(new_urls)} new) | Total so far: {len(all_urls)}")
            if not has_next:
                print("  No more pages.")
                break
            index += 1

        print(f"\nPhase 1 complete: {len(all_urls)} unique agency URLs found.\n")

        # Phase 2: scrape each agency page
        print("Phase 2: Scraping agency detail pages...")
        tasks = [self.url_handler(url) for url in all_urls]
        await run_in_batches(tasks, max_concurrent_tasks=10)

        print(f"\nDone! Data saved to {OUPTUT_FILE}")


if __name__ == "__main__":
    shopify = Shopify()
    asyncio.run(shopify.main())
