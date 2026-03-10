"""
Google Sites Scraper - Return Prime
Scrapes text content and file links (PDFs, Docs, Slides/Decks) from a Google Sites page.
Requires authentication cookies since the site is private.

HOW TO GET YOUR COOKIES:
1. Open Chrome/Firefox and log in to your Google account
2. Navigate to: https://sites.google.com/gokwik.co/salesportal/return-prime
3. Open DevTools (F12) → Application tab → Cookies → https://sites.google.com
4. Copy the values of: __Secure-3PSID, __Secure-3PAPISID, SSID, SID, HSID, APISID, SAPISID
5. Paste them in the COOKIES section below or pass via --cookie-file argument

OR: Export cookies from browser using an extension like "EditThisCookie" or "Cookie Quick Manager"
and save as cookies.json, then run: python3 google_sites_scraper.py --cookie-file cookies.json
"""

import asyncio
import json
import re
import os
import csv
import sys
import argparse
from pathlib import Path
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright
import httpx

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "return_prime_output"
OUTPUT_DIR.mkdir(exist_ok=True)
FILES_DIR = OUTPUT_DIR / "files"
FILES_DIR.mkdir(exist_ok=True)

TARGET_URL = "https://sites.google.com/gokwik.co/salesportal/return-prime"

# ============================================================
# PASTE YOUR GOOGLE COOKIES HERE (from browser DevTools)
# Format: {"cookie_name": "cookie_value", ...}
# Leave empty {} to use --cookie-file argument
# ============================================================
MANUAL_COOKIES = {}
# Example:
# MANUAL_COOKIES = {
#     "SID": "your_sid_value_here",
#     "__Secure-3PSID": "your_3psid_value_here",
#     "SSID": "your_ssid_value_here",
#     "APISID": "your_apisid_value_here",
# }

# File extension patterns to look for
FILE_EXTENSIONS = [".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".csv", ".txt"]

# Google Drive/Docs URL patterns
GOOGLE_FILE_PATTERNS = [
    r"docs\.google\.com/presentation",   # Google Slides (decks)
    r"docs\.google\.com/document",       # Google Docs
    r"docs\.google\.com/spreadsheets",   # Google Sheets
    r"drive\.google\.com/file",          # Google Drive files
    r"drive\.google\.com/open",          # Google Drive open
    r"drive\.google\.com/uc",            # Google Drive direct download
    r"drive\.google\.com/drive",         # Google Drive folder
]


def is_google_file_link(url: str) -> bool:
    for pattern in GOOGLE_FILE_PATTERNS:
        if re.search(pattern, url):
            return True
    return False


def is_direct_file_link(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    return any(path.endswith(ext) for ext in FILE_EXTENSIONS)


def is_video_link(url: str) -> bool:
    return any(x in url for x in ["youtube.com", "youtu.be", "vimeo.com", "wistia.com"])


def get_file_type(url: str) -> str:
    url_lower = url.lower()
    if "presentation" in url_lower or ".ppt" in url_lower:
        return "Presentation/Deck"
    elif "document" in url_lower or ".doc" in url_lower:
        return "Document"
    elif "spreadsheets" in url_lower or ".xls" in url_lower:
        return "Spreadsheet"
    elif ".pdf" in url_lower:
        return "PDF"
    elif "drive.google.com" in url_lower:
        return "Google Drive File"
    else:
        parsed = urlparse(url)
        ext = Path(parsed.path).suffix.lower()
        return ext.lstrip(".").upper() if ext else "Unknown"


def make_export_url(url: str) -> tuple:
    """Convert Google Docs/Slides/Sheets URL to a direct export URL."""
    if "docs.google.com/presentation" in url:
        m = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
        if m:
            return f"https://docs.google.com/presentation/d/{m.group(1)}/export/pptx", ".pptx"
    elif "docs.google.com/document" in url:
        m = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
        if m:
            return f"https://docs.google.com/document/d/{m.group(1)}/export?format=docx", ".docx"
    elif "docs.google.com/spreadsheets" in url:
        m = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
        if m:
            return f"https://docs.google.com/spreadsheets/d/{m.group(1)}/export?format=xlsx", ".xlsx"
    elif "drive.google.com/file/d/" in url:
        m = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
        if m:
            return f"https://drive.google.com/uc?export=download&id={m.group(1)}", ""
    elif "drive.google.com/open?id=" in url:
        m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
        if m:
            return f"https://drive.google.com/uc?export=download&id={m.group(1)}", ""
    return url, ""


async def download_file(url: str, filename: str, client: httpx.AsyncClient) -> dict:
    result = {"downloaded": False, "local_path": "", "size": 0, "error": ""}
    try:
        resp = await client.get(url, follow_redirects=True, timeout=30)
        if resp.status_code == 200:
            content_type = resp.headers.get("content-type", "")
            if not Path(filename).suffix:
                if "pdf" in content_type:
                    filename += ".pdf"
                elif "presentation" in content_type or "powerpoint" in content_type:
                    filename += ".pptx"
                elif "document" in content_type or "word" in content_type:
                    filename += ".docx"
                elif "spreadsheet" in content_type or "excel" in content_type:
                    filename += ".xlsx"
            filepath = FILES_DIR / filename
            filepath.write_bytes(resp.content)
            result["downloaded"] = True
            result["local_path"] = str(filepath)
            result["size"] = len(resp.content)
        else:
            result["error"] = f"HTTP {resp.status_code}"
    except Exception as e:
        result["error"] = str(e)
    return result


def load_cookies(cookie_file: str = None) -> list:
    """Load cookies from file or MANUAL_COOKIES dict."""
    cookies = []

    if cookie_file and Path(cookie_file).exists():
        print(f"Loading cookies from: {cookie_file}")
        with open(cookie_file) as f:
            data = json.load(f)
        # Support various formats: list of dicts, or flat dict
        if isinstance(data, list):
            for c in data:
                if "name" in c and "value" in c:
                    cookies.append({
                        "name": c["name"],
                        "value": c["value"],
                        "domain": c.get("domain", ".google.com"),
                        "path": c.get("path", "/"),
                    })
        elif isinstance(data, dict):
            for name, value in data.items():
                cookies.append({
                    "name": name,
                    "value": value,
                    "domain": ".google.com",
                    "path": "/",
                })
    elif MANUAL_COOKIES:
        print("Using manually configured cookies")
        for name, value in MANUAL_COOKIES.items():
            cookies.append({
                "name": name,
                "value": value,
                "domain": ".google.com",
                "path": "/",
            })

    return cookies


async def scrape_google_site(url: str, cookies: list):
    print(f"\nScraping: {url}")
    print(f"Auth cookies loaded: {len(cookies)}")

    all_links = []
    page_texts = []
    subpages = set()
    visited = set()

    async with async_playwright() as p:
        # Configure proxy bypass for google.com
        http_proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy") or ""
        launch_proxy = None
        if http_proxy:
            launch_proxy = {
                "server": http_proxy,
                "bypass": "*.google.com,*.googleapis.com,*.gstatic.com,localhost,127.0.0.1"
            }

        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            proxy=launch_proxy,
            ignore_https_errors=True,
        )

        # Inject authentication cookies
        if cookies:
            await context.add_cookies(cookies)
            print("Cookies injected into browser context")

        async def scrape_page(page_url: str, depth: int = 0):
            if page_url in visited or depth > 2:
                return
            visited.add(page_url)

            print(f"  {'  ' * depth}Loading: {page_url}")
            page = await context.new_page()

            try:
                await page.goto(page_url, wait_until="networkidle", timeout=45000)
                await page.wait_for_timeout(3000)

                # Check if redirected to login page
                current_url = page.url
                if "accounts.google.com" in current_url or "ServiceLogin" in current_url:
                    print(f"  *** AUTHENTICATION REQUIRED - redirected to login page ***")
                    print(f"  Please provide valid cookies. See instructions at top of script.")
                    await page.close()
                    return

                # Scroll to load lazy content
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1500)
                await page.evaluate("window.scrollTo(0, 0)")
                await page.wait_for_timeout(500)

                title = await page.title()
                print(f"    Title: {title}")

                # Extract body text
                body_text = await page.inner_text("body")

                page_texts.append({
                    "url": page_url,
                    "title": title,
                    "text": body_text.strip()
                })

                # Extract all links
                links = await page.evaluate("""
                    () => {
                        const links = [];
                        document.querySelectorAll('a[href]').forEach(a => {
                            links.push({
                                href: a.href,
                                text: (a.innerText || a.title || a.getAttribute('aria-label') || '').trim()
                            });
                        });
                        return links;
                    }
                """)

                for link in links:
                    href = link.get("href", "")
                    link_text = link.get("text", "")

                    if not href or href.startswith("javascript:") or href.startswith("mailto:"):
                        continue

                    if is_video_link(href):
                        continue

                    if is_google_file_link(href) or is_direct_file_link(href):
                        file_type = get_file_type(href)
                        entry = {
                            "page_url": page_url,
                            "link_text": link_text,
                            "original_url": href,
                            "file_type": file_type,
                        }
                        # Deduplicate by URL
                        if not any(l["original_url"] == href for l in all_links):
                            all_links.append(entry)
                            print(f"    Found [{file_type}]: {link_text[:60]}")

                    elif "sites.google.com/gokwik.co/salesportal" in href and href not in visited:
                        subpages.add(href)

            except Exception as e:
                print(f"  Error on {page_url}: {e}")
            finally:
                await page.close()

        await scrape_page(url)

        for subpage in list(subpages):
            await scrape_page(subpage, depth=1)

        await browser.close()

    return all_links, page_texts


async def main():
    parser = argparse.ArgumentParser(description="Scrape Google Sites - Return Prime")
    parser.add_argument("--cookie-file", help="Path to cookies JSON file", default="cookies.json")
    args = parser.parse_args()

    print("=" * 60)
    print("Google Sites Scraper - Return Prime")
    print("=" * 60)

    cookies = load_cookies(args.cookie_file)

    if not cookies:
        print("\nWARNING: No authentication cookies provided!")
        print("The site requires Google login. Trying without auth anyway...\n")

    all_links, page_texts = await scrape_google_site(TARGET_URL, cookies)

    print(f"\nFound {len(all_links)} file links across {len(page_texts)} pages")

    # Download files
    if all_links:
        print("\nAttempting to download files...")
        cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in cookies) if cookies else ""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
        }
        if cookie_header:
            headers["Cookie"] = cookie_header

        async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30) as client:
            for i, link in enumerate(all_links):
                original_url = link["original_url"]
                link_text = link["link_text"] or f"file_{i+1}"

                safe_name = re.sub(r'[^\w\s-]', '', link_text)[:50].strip()
                safe_name = re.sub(r'\s+', '_', safe_name) or f"file_{i+1}"

                export_url, ext = make_export_url(original_url)
                filename = safe_name + ext

                print(f"  Downloading [{link['file_type']}]: {link_text[:50]}...")
                dl_result = await download_file(export_url, filename, client)
                link.update(dl_result)

                if dl_result["downloaded"]:
                    size_kb = dl_result["size"] / 1024
                    print(f"    Saved: {filename} ({size_kb:.1f} KB)")
                else:
                    print(f"    Failed: {dl_result['error']}")

    # Save text content
    text_output_path = OUTPUT_DIR / "page_content.txt"
    with open(text_output_path, "w", encoding="utf-8") as f:
        f.write(f"Google Sites Scraper - Return Prime\n")
        f.write(f"URL: {TARGET_URL}\n")
        f.write(f"Pages scraped: {len(page_texts)}\n")
        f.write("=" * 60 + "\n\n")
        for pt in page_texts:
            f.write(f"{'=' * 60}\n")
            f.write(f"PAGE: {pt['title']}\n")
            f.write(f"URL: {pt['url']}\n")
            f.write(f"{'=' * 60}\n\n")
            f.write(pt["text"])
            f.write("\n\n")

    # Save files summary CSV
    csv_path = OUTPUT_DIR / "files_summary.csv"
    csv_fields = ["file_type", "link_text", "original_url", "downloaded", "local_path", "size", "error", "page_url"]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        for link in all_links:
            writer.writerow({k: link.get(k, "") for k in csv_fields})

    # Save JSON summary
    json_path = OUTPUT_DIR / "files_summary.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "target_url": TARGET_URL,
            "pages_scraped": len(page_texts),
            "files_found": len(all_links),
            "files_downloaded": sum(1 for l in all_links if l.get("downloaded")),
            "files": all_links,
            "pages": [{"url": p["url"], "title": p["title"]} for p in page_texts]
        }, f, indent=2, ensure_ascii=False)

    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Pages scraped:      {len(page_texts)}")
    print(f"Files found:        {len(all_links)}")
    print(f"Files downloaded:   {sum(1 for l in all_links if l.get('downloaded'))}")
    print(f"\nOutputs saved to: {OUTPUT_DIR}/")
    print(f"  - page_content.txt   (all extracted text)")
    print(f"  - files_summary.csv  (all file links + download status)")
    print(f"  - files_summary.json (structured data)")
    print(f"  - files/             (downloaded files)")

    if all_links:
        print("\nFiles found:")
        for link in all_links:
            status = "DOWNLOADED" if link.get("downloaded") else f"FAILED ({link.get('error', 'N/A')})"
            print(f"  [{link['file_type']}] {link['link_text'][:50]} - {status}")


if __name__ == "__main__":
    asyncio.run(main())
