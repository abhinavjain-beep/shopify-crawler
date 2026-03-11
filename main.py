import os
import asyncio
import aiofiles
import pandas as pd
from pathlib import Path

from scraper import Scraper

BASE_DIR  =  Path(__file__).resolve().parent

URLS_FILE = BASE_DIR / "urls.txt"
OUPTUT_FILE = BASE_DIR / "data.csv"

# Countries to scrape: (display_name, country_code_for_url)
COUNTRIES = [
    ("United States", "us"),
    ("United Kingdom", "gb"),
    ("Australia", "au"),
]


async def run_in_batches(tasks, max_concurrent_tasks : int = 10):
    """
    Runs tasks in batches of the specified size and collects their results.

    Returns:
        List of results returned by the tasks.
    """
    results = []
    for i in range(0, len(tasks), max_concurrent_tasks):
        batch = tasks[i:i + max_concurrent_tasks]
        print(f"Running batch {i // max_concurrent_tasks + 1}: {len(batch)} tasks")
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)
        print(f"Finished batch {i // max_concurrent_tasks + 1}")

    return results

class Shopify:

    def __init__(self) -> None:
        self.scraper =  Scraper()
        self.base_url = "https://www.shopify.com"
        self.directory_url = "https://www.shopify.com/partners/directory/services"
        self.headers  = {
                        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
                         "Accept-Language":"en-US,en;q=0.5",
                         "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0"
                         }
        # Track already-scraped URLs to avoid duplicates across country runs
        self.scraped_urls = set()

    async def store_url(self, url: str):
        async with aiofiles.open(URLS_FILE, "a") as f:
            await f.write(url + "\n")

    async def load_scraped_urls(self):
        """Load already-scraped URLs to support resume."""
        if not URLS_FILE.exists():
            return set()
        async with aiofiles.open(URLS_FILE, "r") as f:
            lines = await f.readlines()
        return {line.strip() for line in lines if line.strip()}

    def to_csv(self, data: dict):
        df = pd.json_normalize(data)
        df.to_csv(OUPTUT_FILE, mode="a", header=not OUPTUT_FILE.exists(), index=False, encoding="utf-8-sig")

    def _extract_partner_tier(self, soup) -> str:
        """
        Extracts the partner tier (Platinum, Premier, Plus, Select) from an agency profile page.
        Returns the tier string or 'X' if not found.
        """
        tiers = ["Platinum", "Premier", "Plus", "Select"]
        for tier in tiers:
            # Look for "<Tier> tier" text in the page
            element = soup.find(string=lambda text, t=tier: text and f"{t} tier" in text)
            if element:
                return f"{tier} Partner"
            # Fallback: look for data attributes or class names containing the tier name
            badge = soup.find(attrs={"data-partner-tier": True})
            if badge:
                return badge.get("data-partner-tier", "X")
        return "X"

    async def url_handler(self, url: str, country: str):
        if url in self.scraped_urls:
            print(f"Skipping already-scraped: {url}")
            return None

        soup = await self.scraper.get_soup(url, headers=self.headers)
        if not soup:
            print(f"Error getting {url}")
            return None
        data  = {}

        LinkedIn = ""
        Instagram = ""
        Facebook = ""
        Twitter = ""
        Youtube = ""

        title = soup.select_one("h1.richtext.text-t4")
        description = soup.select_one("section[data-section-name='description']")
        phone =  soup.select_one("a[href*='tel:']")
        email =  soup.select_one("a[href*='mailto:']")
        website =  soup.select_one("div.flex.flex-wrap.gap-x-2.items-center a[rel='nofollow']")

        location = soup.select_one("div.flex.flex-col.gap-y-1:-soup-contains('Primary location') p:nth-child(2)")
        languages = soup.select_one("div.flex.flex-col.gap-y-1:-soup-contains('Languages') p:nth-child(2)")

        socials = soup.select("div.flex.flex-col.gap-y-1:-soup-contains('Social links') a")
        for social in socials:
            href = social.get('href', '')
            if "linkedin.com" in href:
                LinkedIn = href
            if "instagram" in href:
                Instagram = href
            if "facebook" in href:
                Facebook = href
            if "twitter" in href or "x.com" in href:
                Twitter = href
            if "youtube" in href:
                Youtube = href

        partner_tier = self._extract_partner_tier(soup)

        data["Name"] = title.get_text(strip=True) if title else "X"
        data["Country"] = country
        data["Partner Tier"] = partner_tier
        data["Description"] = description.get_text(strip=True) if description else "X"
        data["Phone Number"] = phone.get('href').replace('tel:', '') if phone else "X"
        data["Website"] = website.get('href') if website else "X"
        data["Email"] = email.get('href').replace('mailto:', '') if email else "X"
        data["LinkedIn"] = LinkedIn if LinkedIn else "X"
        data["Location"] = location.get_text(strip=True) if location else "X"
        data["Languages"] = languages.get_text(strip=True) if languages else "X"
        data["URL"] = url
        data["Instagram"] = Instagram if Instagram else "X"
        data["Facebook"] = Facebook if Facebook else "X"
        data["Twitter"] = Twitter if Twitter else "X"
        data["Youtube"] = Youtube if Youtube else "X"

        self.scraped_urls.add(url)
        await self.store_url(url)
        self.to_csv(data)
        return data

    def _build_page_url(self, country_code: str, page: int) -> str:
        params = f"country={country_code}&sort=DEFAULT"
        if page > 1:
            params += f"&page={page}"
        return f"{self.directory_url}?{params}"

    async def get_page_urls(self, country_code: str, page: int):
        url = self._build_page_url(country_code, page)
        soup = await self.scraper.get_soup(url, headers=self.headers)
        if not soup:
            print(f"Error getting {url}")
            return None, False
        urls = soup.select('[data-component-name="listing-profile-card"] a[href]')
        next_page = soup.find(attrs={'data-component-name': 'next-page'})
        has_next = next_page is not None and next_page.get('aria-disabled') != 'true'
        return [self.base_url + a.get('href') for a in urls], has_next

    async def scrape_country(self, country_name: str, country_code: str):
        print(f"\n{'='*60}")
        print(f"Scraping {country_name} (country={country_code})")
        print(f"{'='*60}")
        index = 1
        total = 0
        while True:
            urls, has_next = await self.get_page_urls(country_code, index)
            if not urls:
                print(f"No URLs found on page {index} for {country_name}, stopping.")
                break
            # Filter out already-scraped URLs
            new_urls = [u for u in urls if u not in self.scraped_urls]
            print(f"Page {index}: {len(urls)} found, {len(new_urls)} new")
            if new_urls:
                tasks = [self.url_handler(u, country_name) for u in new_urls]
                await run_in_batches(tasks, max_concurrent_tasks=10)
                total += len(new_urls)
            if not has_next:
                print(f"No more pages for {country_name}. Total scraped: {total}")
                break
            index += 1

    async def main(self):
        # Load already-scraped URLs (supports resume)
        self.scraped_urls = await self.load_scraped_urls()
        print(f"Loaded {len(self.scraped_urls)} previously scraped URLs")

        for country_name, country_code in COUNTRIES:
            await self.scrape_country(country_name, country_code)

        print(f"\nDone! All agencies saved to {OUPTUT_FILE}")

if __name__ == "__main__":
    shopify = Shopify()
    asyncio.run(shopify.main())
