import asyncio
import re
import pandas as pd
from pathlib import Path
from scraper import Scraper

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "data.csv"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0"
}

TIER_MAP = {
    "tier_plus": "Plus",
    "tier_select": "Select",
    "tier_premier": "Premier",
    "tier_platinum": "Platinum",
    "tier_registered": "Registered",
}


async def fetch_tier(scraper: Scraper, url: str) -> str:
    for attempt in range(3):
        resp = await scraper.get(url, headers=HEADERS)
        if resp:
            m = re.search(r'partnerProgramTier\\\\\",\\\\\"([^\"\\\\]+)\\\\\"', resp.text)
            if m:
                return TIER_MAP.get(m.group(1), "")
            return ""
        await asyncio.sleep(2)
    return ""


async def run_in_batches(tasks, batch_size=10):
    results = []
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i + batch_size]
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)
        done = i + len(batch)
        print(f"Progress: {done}/{len(tasks)}", end="\r", flush=True)
    return results


async def main():
    df = pd.read_csv(OUTPUT_FILE, encoding="utf-8-sig")
    missing_mask = df["Shopify Tier"].isna() | (df["Shopify Tier"].str.strip() == "")
    missing_idx = df[missing_mask].index.tolist()
    print(f"Total agencies: {len(df)}")
    print(f"Missing tiers: {len(missing_idx)}")

    scraper = Scraper(requests_per_second=10)
    tasks = [fetch_tier(scraper, df.at[i, "Shopify Page"]) for i in missing_idx]
    tiers = await run_in_batches(tasks, batch_size=10)
    await scraper.session.aclose()

    for idx, tier in zip(missing_idx, tiers):
        df.at[idx, "Shopify Tier"] = tier

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    still_missing = df["Shopify Tier"].isna() | (df["Shopify Tier"].str.strip() == "")
    print(f"\nDone! Still missing after fill: {still_missing.sum()}")
    print(f"Tiers filled: {len(missing_idx) - still_missing.sum()}")


if __name__ == "__main__":
    asyncio.run(main())
