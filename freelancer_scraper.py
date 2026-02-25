import os
import re
import json
import asyncio
import pandas as pd
from pathlib import Path

from scraper import Scraper

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "freelancers.csv"

BASE_SEARCH_URL = "https://www.upwork.com/search/profiles/"
BASE_PROFILE_URL = "https://www.upwork.com/freelancers/"

MAX_PAGES = 20  # maximum pages to scrape per category

CATEGORIES = [
    {"name": "Web Development",       "query": "web development"},
    {"name": "Performance Marketing", "query": "performance marketing"},
    {"name": "Full Stack Development","query": "full stack development"},
]


async def run_in_batches(tasks, max_concurrent_tasks: int = 5):
    results = []
    for i in range(0, len(tasks), max_concurrent_tasks):
        batch = tasks[i:i + max_concurrent_tasks]
        print(f"  Running batch {i // max_concurrent_tasks + 1}: {len(batch)} profiles")
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)
    return results


class UpworkScraper:

    def __init__(self):
        self.scraper = Scraper(requests_per_second=2, timeout=30)
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.upwork.com/",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
        }
        self.seen_urls: set = set()

    def to_csv(self, data: dict):
        df = pd.json_normalize(data)
        df.to_csv(
            OUTPUT_FILE,
            mode="a",
            header=not OUTPUT_FILE.exists(),
            index=False,
            encoding="utf-8-sig",
        )

    def _extract_json_from_script(self, soup, pattern: str):
        """Try to extract JSON data embedded in <script> tags matching a pattern."""
        for script in soup.find_all("script"):
            text = script.get_text()
            if pattern in text:
                try:
                    # Look for JSON object/array in the script text
                    match = re.search(r'\{.*\}', text, re.DOTALL)
                    if match:
                        return json.loads(match.group())
                except (json.JSONDecodeError, Exception):
                    pass
        return None

    def _extract_profile_urls_from_json(self, soup) -> list:
        """Extract profile UIDs from embedded JSON state in Upwork search pages."""
        urls = []
        for script in soup.find_all("script"):
            text = script.get_text()
            # Upwork embeds profile data with ciphertext (encrypted UID)
            # Pattern: ~<alphanumeric> in JSON keys or values
            matches = re.findall(r'["\']ciphertext["\']\s*:\s*["\']([^"\']+)["\']', text)
            for uid in matches:
                url = f"{BASE_PROFILE_URL}~{uid}"
                if url not in self.seen_urls:
                    self.seen_urls.add(url)
                    urls.append(url)
            if urls:
                break
        return urls

    async def search_page(self, query: str, page: int) -> tuple[list, bool]:
        """
        Fetch one page of Upwork search results for a given query (US only).
        Returns (list_of_profile_urls, has_next_page).
        """
        params = {
            "q": query,
            "country": "United States",
            "page": str(page),
        }
        soup = await self.scraper.get_soup(BASE_SEARCH_URL, params=params, headers=self.headers)
        if not soup:
            print(f"    Failed to fetch search page {page} for '{query}'")
            return [], False

        urls = []

        # Strategy A: direct HTML anchor tags pointing to freelancer profiles
        anchors = soup.select("a[href*='/freelancers/~']")
        for a in anchors:
            href = a.get("href", "")
            # Normalise to full URL, strip query params
            clean = re.sub(r'\?.*$', '', href)
            if not clean.startswith("http"):
                clean = "https://www.upwork.com" + clean
            if clean not in self.seen_urls:
                self.seen_urls.add(clean)
                urls.append(clean)

        # Strategy B: JSON ciphertext extraction if HTML yielded nothing
        if not urls:
            urls = self._extract_profile_urls_from_json(soup)

        # Detect next page: look for a pagination element or rely on result count
        has_next = bool(soup.select_one(
            "[data-test='pagination-next']:not([disabled]), "
            "a[aria-label='Next page'], "
            "li.up-pagination-item-next:not(.disabled)"
        ))
        # Fallback: if we got results and haven't explicitly seen "no next", assume there might be one
        if not has_next and urls:
            # Check if the page has a "no results" indicator
            no_results = soup.find(string=re.compile(r'no (freelancers|results)', re.I))
            has_next = no_results is None and page < MAX_PAGES

        return urls, has_next

    def _parse_profile_html(self, soup, url: str, category: str) -> dict:
        """Parse a freelancer profile page with HTML selectors."""
        data = {
            "Name": "N/A",
            "Title": "N/A",
            "Location": "N/A",
            "Bio": "N/A",
            "Skills": "N/A",
            "Category": category,
            "Profile_URL": url,
            "Hourly_Rate": "N/A",
            "Job_Success_Score": "N/A",
        }

        # Name — multiple selector attempts for robustness
        for sel in [
            "h1[itemprop='name']",
            "span[itemprop='name']",
            "h1.freelancer-name",
            "[data-test='freelancer-name']",
            "h1",
        ]:
            el = soup.select_one(sel)
            if el:
                data["Name"] = el.get_text(strip=True)
                break

        # Title / Professional headline
        for sel in [
            "p.freelancer-title",
            "[data-test='freelancer-title']",
            "h2.freelancer-title",
            "p[itemprop='jobTitle']",
            "div.title",
        ]:
            el = soup.select_one(sel)
            if el:
                data["Title"] = el.get_text(strip=True)
                break

        # Location
        for sel in [
            "[data-test='location']",
            "span[itemprop='addressLocality']",
            "li:-soup-contains('United States')",
            "div.location",
            "span.location",
        ]:
            el = soup.select_one(sel)
            if el:
                data["Location"] = el.get_text(strip=True)
                break

        # Bio / Overview
        for sel in [
            "[data-test='description']",
            "div.freelancer-overview",
            "div.overview",
            "section.air3-card-section div.text-body",
            "div[itemprop='description']",
            "p.description",
        ]:
            el = soup.select_one(sel)
            if el:
                data["Bio"] = el.get_text(separator=" ", strip=True)[:1000]
                break

        # Skills
        skill_els = soup.select(
            "[data-test='skill-badge'], "
            "span.skill-tag, "
            "a.skill-tag, "
            "li.skill, "
            "span[data-test='freetext-skill']"
        )
        if skill_els:
            data["Skills"] = ", ".join(s.get_text(strip=True) for s in skill_els)

        # Hourly Rate
        for sel in [
            "[data-test='hourly-rate']",
            "span.rate",
            "div.rate",
            "h3:-soup-contains('$')",
            "span:-soup-contains('/hr')",
        ]:
            el = soup.select_one(sel)
            if el:
                data["Hourly_Rate"] = el.get_text(strip=True)
                break

        # Job Success Score
        for sel in [
            "[data-test='job-success-score']",
            "span.job-success",
            "div.job-success",
            "span:-soup-contains('Job Success')",
        ]:
            el = soup.select_one(sel)
            if el:
                data["Job_Success_Score"] = el.get_text(strip=True)
                break

        return data

    def _parse_profile_from_json(self, soup, url: str, category: str) -> dict | None:
        """Try to extract profile data from embedded JSON in the page."""
        for script in soup.find_all("script", {"type": "application/json"}):
            try:
                state = json.loads(script.get_text())
                # Flatten nested dicts to find relevant keys
                text = json.dumps(state)
                if '"name"' not in text and '"title"' not in text:
                    continue

                # Try to navigate to profile object
                profile = None
                # Common Upwork SSR key paths
                for key in ["profile", "freelancerProfile", "user", "contractor"]:
                    if key in state:
                        profile = state[key]
                        break

                if not profile and "props" in state:
                    profile = state.get("props", {}).get("profile") or \
                               state.get("props", {}).get("pageProps", {}).get("profile")

                if not profile:
                    continue

                skills_raw = profile.get("skills") or profile.get("skillTags") or []
                if isinstance(skills_raw, list):
                    skills = ", ".join(
                        s.get("name", s) if isinstance(s, dict) else str(s)
                        for s in skills_raw
                    )
                else:
                    skills = str(skills_raw)

                return {
                    "Name": profile.get("name") or profile.get("fullName") or "N/A",
                    "Title": profile.get("title") or profile.get("professionalTitle") or "N/A",
                    "Location": (
                        profile.get("location", {}).get("city", "") + ", " +
                        profile.get("location", {}).get("state", "")
                    ).strip(", ") or "N/A",
                    "Bio": (profile.get("description") or profile.get("overview") or "N/A")[:1000],
                    "Skills": skills or "N/A",
                    "Category": category,
                    "Profile_URL": url,
                    "Hourly_Rate": str(profile.get("hourlyRate") or profile.get("rate") or "N/A"),
                    "Job_Success_Score": str(
                        profile.get("jobSuccessScore") or
                        profile.get("totalFeedback") or "N/A"
                    ),
                }
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
        return None

    async def scrape_profile(self, url: str, category: str) -> dict | None:
        """Scrape a single Upwork freelancer profile page."""
        soup = await self.scraper.get_soup(url, headers=self.headers)
        if not soup:
            print(f"    Failed to fetch profile: {url}")
            return None

        # Try JSON extraction first (richer, more reliable)
        data = self._parse_profile_from_json(soup, url, category)
        if not data:
            # Fall back to HTML parsing
            data = self._parse_profile_html(soup, url, category)

        # Skip clearly empty records
        if data["Name"] == "N/A" and data["Title"] == "N/A":
            return None

        print(f"    Scraped: {data['Name']} | {data['Title']} | {data['Location']}")
        self.to_csv(data)
        return data

    async def scrape_category(self, category: dict):
        """Scrape all pages for a single category and write results to CSV."""
        name = category["name"]
        query = category["query"]
        print(f"\n=== Category: {name} ===")

        all_urls = []
        for page in range(1, MAX_PAGES + 1):
            print(f"  Fetching search page {page}…")
            urls, has_next = await self.search_page(query, page)
            print(f"  Found {len(urls)} new profile URLs on page {page}")
            all_urls.extend(urls)
            if not has_next or not urls:
                print(f"  No more pages for '{name}'.")
                break

        print(f"  Total profiles to scrape for '{name}': {len(all_urls)}")
        tasks = [self.scrape_profile(url, name) for url in all_urls]
        results = await run_in_batches(tasks, max_concurrent_tasks=5)
        scraped = [r for r in results if r is not None]
        print(f"  Done. Scraped {len(scraped)} profiles for '{name}'.")
        return scraped

    async def main(self):
        print(f"Starting Upwork US Freelancer Scraper")
        print(f"Output: {OUTPUT_FILE}")
        print(f"Categories: {[c['name'] for c in CATEGORIES]}")

        all_results = []
        for category in CATEGORIES:
            results = await self.scrape_category(category)
            all_results.extend(results)

        await self.scraper.close()
        print(f"\n=== Finished. Total freelancers scraped: {len(all_results)} ===")
        print(f"Results saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(UpworkScraper().main())
