import os
import asyncio
import aiofiles
import pandas as pd
from pathlib import Path

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
        print(f"Running batch {i // max_concurrent_tasks + 1}: {len(batch)} tasks")
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)
        print(f"Finished batch {i // max_concurrent_tasks + 1}")
    
    return results

class Shopify:
    
    def __init__(self) -> None:
        self.scraper =  Scraper()
        self.base_url = "https://www.shopify.com"
        self.page_url = "https://www.shopify.com/partners/directory/services?minPrice=&maxPrice=&locationCodes=loc-us&sort=DEFAULT"
        self.headers  = {
                        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
                         "Accept-Language":"en-US,en;q=0.5",
                         "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0"
                         }
    
    async def store_urls(self , url : str):
        async with aiofiles.open(URLS_FILE,"a") as f:
            await f.write(url + "\n") 
    
    async def load_urls(self):
        async with aiofiles.open(URLS_FILE , "r") as f:
            return [line.strip() for line in f.readlines()]       
    
    def to_csv(self,data : dict):
        df =pd.json_normalize(data)
        df.to_csv(OUPTUT_FILE,mode="a",header=not os.path.exists(OUPTUT_FILE),index=False, encoding="utf-8-sig")
        
    
    async def  url_handler(self, url : str):
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
        Languages = soup.select_one("div.flex.flex-col.gap-y-1:-soup-contains('Languages') p:nth-child(2)")
        

        socials = soup.select("div.flex.flex-col.gap-y-1:-soup-contains('Social links') a")
        for social in socials:
            href = social.get('href')
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

        
        data["Name"] = title.get_text(strip=True) if title else "X"
        data["Description"] = description.get_text(strip=True) if description else "X"
        data["Phone Number"] = phone.get('href').replace('tel:','') if phone else "X"
        data["Website"] = website.get('href') if website else "X"
        data["Email"] = email.get('href').replace('mailto:','') if email else "X"
        data["LinkedIn"] = LinkedIn if LinkedIn else "X"
        data["Location"] = location.get_text(strip=True) if location else "X"
        data["Languages"] = Languages.get_text(strip=True) if Languages else "X"
        data["URL"] = url
        data["Instagram"] = Instagram if Instagram else "X"
        data["Facebook"] = Facebook if Facebook else "X"
        data["Twitter"] = Twitter if Twitter else "X"
        data["Youtube"] = Youtube if Youtube else "X"
       
        await self.store_urls(url)
       
        self.to_csv(data)
        return data
    
    async def get_page_urls(self, page : int):
        if page == 1:
            url = self.page_url
        else:
            url = self.page_url + f"&page={page}"
        soup = await self.scraper.get_soup(url, headers=self.headers)
        if not soup:
            print(f"Error getting {url}")
            return None, False
        urls = soup.select('[data-component-name="listing-profile-card"] a[href]')
        next_page = soup.find(attrs={'data-component-name': 'next-page'})
        has_next = next_page is not None and next_page.get('aria-disabled') != 'true'
        return [self.base_url + url.get('href') for url in urls], has_next
    
    async def main(self):
        index = 1
        while True:
            urls, has_next = await self.get_page_urls(index)
            if not urls:
                print(f"No urls found on page {index}, stopping.")
                break
            print(f"Getting urls for page {index} with {len(urls)} urls")
            tasks = [self.url_handler(url) for url in urls]
            results = await run_in_batches(tasks, max_concurrent_tasks=15)
            print(f"Finished getting urls for page {index}")
            if not has_next:
                print("No more pages, done.")
                break
            index += 1
    
if __name__ == "__main__":
    shopify = Shopify()
    ouput = asyncio.run(shopify.main())
