"""
Google Sites Scraper - Return Prime
Scrapes text, PDFs, Docs, and Decks from the GoKwik Sales Portal.

AUTHENTICATION (required - site is private):
  Option A: Set GOOGLE_COOKIES env variable with your browser cookie string
    export GOOGLE_COOKIES="SID=xxx; SSID=yyy; APISID=zzz; __Secure-3PSID=aaa"

  Option B: Provide a cookies.json file
    python3 google_sites_scraper.py --cookie-file cookies.json

  HOW TO GET COOKIES:
    1. Open Chrome, log in to Google with your GoKwik/gokwik.co account
    2. Navigate to: https://sites.google.com/gokwik.co/salesportal/return-prime
    3. Open DevTools (F12) → Network tab → click any request to sites.google.com
    4. Copy the full "Cookie:" header value
    5. Export as env var: export GOOGLE_COOKIES="<paste here>"

    OR use Chrome extension "EditThisCookie" → Export → save as cookies.json
"""

import asyncio
import json
import re
import os
import csv
import argparse
import zipfile
from pathlib import Path
from urllib.parse import urlparse
from playwright.async_api import async_playwright
import httpx

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "return_prime_output"
OUTPUT_DIR.mkdir(exist_ok=True)
FILES_DIR = OUTPUT_DIR / "files"
FILES_DIR.mkdir(exist_ok=True)

TARGET_URL = "https://sites.google.com/gokwik.co/salesportal/return-prime"
SITE_BASE = "sites.google.com/gokwik.co/salesportal"

GOOGLE_FILE_PATTERNS = [
    r"docs\.google\.com/presentation",
    r"docs\.google\.com/document",
    r"docs\.google\.com/spreadsheets",
    r"drive\.google\.com/file",
    r"drive\.google\.com/open",
    r"drive\.google\.com/uc",
    r"drive\.google\.com/drive",
]
FILE_EXTENSIONS = [".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx"]


def get_proxy_config():
    proxy_url = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or ""
    if not proxy_url:
        return None
    m = re.match(r"http://([^:]+):([^@]+)@(.+)", proxy_url)
    if m:
        return {"server": f"http://{m.group(3)}", "username": m.group(1), "password": m.group(2)}
    return {"server": proxy_url}


def is_file_link(url: str) -> bool:
    if any(re.search(p, url) for p in GOOGLE_FILE_PATTERNS):
        return True
    return any(urlparse(url).path.lower().endswith(ext) for ext in FILE_EXTENSIONS)


def is_video(url: str) -> bool:
    return any(x in url for x in ["youtube.com", "youtu.be", "vimeo.com", "wistia.com", "loom.com"])


def get_file_type(url: str) -> str:
    u = url.lower()
    if "presentation" in u or ".ppt" in u:
        return "Deck/Presentation"
    if "document" in u or ".doc" in u:
        return "Document"
    if "spreadsheets" in u or ".xls" in u:
        return "Spreadsheet"
    if ".pdf" in u:
        return "PDF"
    if "drive.google.com" in u:
        return "Drive File"
    ext = Path(urlparse(url).path).suffix.lower().lstrip(".")
    return ext.upper() if ext else "File"


def make_export_url(url: str):
    """Return (download_url, extension) for Google file links."""
    def fid(u):
        m = re.search(r"/d/([a-zA-Z0-9_-]+)", u)
        return m.group(1) if m else None

    if "docs.google.com/presentation" in url:
        i = fid(url)
        return (f"https://docs.google.com/presentation/d/{i}/export/pptx", ".pptx") if i else (url, "")
    if "docs.google.com/document" in url:
        i = fid(url)
        return (f"https://docs.google.com/document/d/{i}/export?format=docx", ".docx") if i else (url, "")
    if "docs.google.com/spreadsheets" in url:
        i = fid(url)
        return (f"https://docs.google.com/spreadsheets/d/{i}/export?format=xlsx", ".xlsx") if i else (url, "")
    if "drive.google.com/file/d/" in url:
        i = fid(url)
        return (f"https://drive.google.com/uc?export=download&id={i}", "") if i else (url, "")
    if "drive.google.com/open" in url:
        m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
        i = m.group(1) if m else None
        return (f"https://drive.google.com/uc?export=download&id={i}", "") if i else (url, "")
    return url, ""


def load_cookies(cookie_file: str = None) -> list:
    """Load cookies from file, env var, or return empty."""
    # Option A: env variable cookie string
    cookie_str = os.environ.get("GOOGLE_COOKIES", "")
    if cookie_str:
        print("Using cookies from GOOGLE_COOKIES environment variable")
        cookies = []
        for part in cookie_str.split(";"):
            part = part.strip()
            if "=" in part:
                name, _, value = part.partition("=")
                cookies.append({"name": name.strip(), "value": value.strip(),
                                 "domain": ".google.com", "path": "/"})
        return cookies

    # Option B: cookie file
    path = Path(cookie_file) if cookie_file else Path(BASE_DIR / "cookies.json")
    if path.exists():
        print(f"Using cookies from: {path}")
        data = json.loads(path.read_text())
        if isinstance(data, list):
            return [{"name": c["name"], "value": c["value"],
                     "domain": c.get("domain", ".google.com"),
                     "path": c.get("path", "/")} for c in data if "name" in c and "value" in c]
        if isinstance(data, dict):
            return [{"name": k, "value": v, "domain": ".google.com", "path": "/"}
                    for k, v in data.items()]

    return []


async def download_file(url: str, filename: str, client: httpx.AsyncClient) -> dict:
    result = {"downloaded": False, "local_path": "", "size_kb": 0, "error": ""}
    try:
        r = await client.get(url, follow_redirects=True, timeout=40)
        if r.status_code == 200:
            ct = r.headers.get("content-type", "")
            if not Path(filename).suffix:
                if "pdf" in ct:               filename += ".pdf"
                elif "powerpoint" in ct:      filename += ".pptx"
                elif "word" in ct:            filename += ".docx"
                elif "excel" in ct:           filename += ".xlsx"
                elif "presentation" in ct:    filename += ".pptx"
                elif "document" in ct:        filename += ".docx"
                elif "spreadsheet" in ct:     filename += ".xlsx"
            filepath = FILES_DIR / filename
            filepath.write_bytes(r.content)
            result.update(downloaded=True, local_path=str(filepath), size_kb=round(len(r.content)/1024, 1))
        elif r.status_code == 401:
            result["error"] = "Auth required - provide valid cookies"
        elif r.status_code == 403:
            result["error"] = "Access denied (403) - file may need login"
        else:
            result["error"] = f"HTTP {r.status_code}"
    except Exception as e:
        result["error"] = str(e)[:100]
    return result


async def scrape_all(target_url: str, cookies: list):
    all_files = []
    all_pages = []
    visited = set()
    to_visit = {target_url}

    proxy_config = get_proxy_config()

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True, proxy=proxy_config)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            ignore_https_errors=True,
        )
        if cookies:
            await context.add_cookies(cookies)
            print(f"Injected {len(cookies)} auth cookies")

        async def visit(url: str):
            if url in visited:
                return
            visited.add(url)
            print(f"  Scraping: {url}")
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=45000)
                await page.wait_for_timeout(2000)

                cur = page.url
                if "accounts.google.com" in cur or "ServiceLogin" in cur:
                    print("  !! Login page - cookies missing or expired")
                    return

                # Scroll to trigger lazy loads
                for _ in range(3):
                    await page.evaluate("window.scrollBy(0, window.innerHeight)")
                    await page.wait_for_timeout(600)
                await page.evaluate("window.scrollTo(0, 0)")

                title = await page.title()
                text = await page.inner_text("body")

                all_pages.append({"url": url, "title": title, "text": text.strip()})
                print(f"    Title: {title} | Text: {len(text)} chars")

                # Extract links
                links = await page.evaluate("""() => {
                    return Array.from(document.querySelectorAll('a[href]')).map(a => ({
                        href: a.href,
                        text: (a.innerText || a.title || a.getAttribute('aria-label') || '').trim()
                    }));
                }""")

                for lnk in links:
                    href = lnk.get("href", "")
                    ltext = lnk.get("text", "")
                    if not href or href.startswith("javascript:") or href.startswith("mailto:"):
                        continue
                    if is_video(href):
                        continue
                    if is_file_link(href):
                        if not any(f["original_url"] == href for f in all_files):
                            all_files.append({
                                "page_url": url, "link_text": ltext,
                                "original_url": href, "file_type": get_file_type(href),
                            })
                            print(f"    [FILE] {get_file_type(href)}: {ltext[:60]}")
                    elif SITE_BASE in href and href not in visited:
                        to_visit.add(href)

            except Exception as e:
                print(f"  Error: {e}")
            finally:
                await page.close()

        # Crawl all discovered pages
        while to_visit:
            url = to_visit.pop()
            await visit(url)

        await browser.close()

    return all_files, all_pages


async def download_all_files(all_files: list, cookies: list):
    if not all_files:
        return

    cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Cookie": cookie_header,
    }

    async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=40,
                                  verify=False) as client:
        for i, f in enumerate(all_files):
            safe = re.sub(r'[^\w\s-]', '', f["link_text"] or f"file_{i+1}")[:50].strip()
            safe = re.sub(r'\s+', '_', safe) or f"file_{i+1}"
            export_url, ext = make_export_url(f["original_url"])
            filename = safe + ext

            print(f"  Downloading: {f['link_text'][:50]} [{f['file_type']}]")
            result = await download_file(export_url, filename, client)
            f.update(result)

            if result["downloaded"]:
                print(f"    Saved: {filename} ({result['size_kb']} KB)")
            else:
                print(f"    Failed: {result['error']}")


def save_outputs(all_files: list, all_pages: list):
    # 1. page_content.txt
    txt_path = OUTPUT_DIR / "page_content.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"Return Prime - GoKwik Sales Portal\n")
        f.write(f"Source: {TARGET_URL}\n")
        f.write(f"Pages scraped: {len(all_pages)}\n")
        f.write("=" * 70 + "\n\n")
        for p in all_pages:
            f.write(f"{'=' * 70}\n")
            f.write(f"PAGE: {p['title']}\n")
            f.write(f"URL:  {p['url']}\n")
            f.write(f"{'=' * 70}\n\n")
            f.write(p["text"])
            f.write("\n\n")

    # 2. files_summary.csv
    csv_path = OUTPUT_DIR / "files_summary.csv"
    fields = ["file_type", "link_text", "original_url", "downloaded",
              "local_path", "size_kb", "error", "page_url"]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for item in all_files:
            w.writerow({k: item.get(k, "") for k in fields})

    # 3. files_summary.json
    json_path = OUTPUT_DIR / "files_summary.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "source": TARGET_URL,
            "pages_scraped": len(all_pages),
            "files_found": len(all_files),
            "files_downloaded": sum(1 for x in all_files if x.get("downloaded")),
            "pages": [{"url": p["url"], "title": p["title"]} for p in all_pages],
            "files": all_files,
        }, f, indent=2, ensure_ascii=False)

    return txt_path, csv_path, json_path


def create_zip(all_files: list, txt_path, csv_path, json_path) -> Path:
    zip_path = BASE_DIR / "return_prime_complete.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # Add summary files
        zf.write(txt_path, "page_content.txt")
        zf.write(csv_path, "files_summary.csv")
        zf.write(json_path, "files_summary.json")

        # Add downloaded files
        for item in all_files:
            if item.get("downloaded") and item.get("local_path"):
                p = Path(item["local_path"])
                if p.exists():
                    zf.write(p, f"files/{p.name}")

    return zip_path


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cookie-file", default="cookies.json")
    args = parser.parse_args()

    print("=" * 70)
    print("Return Prime - GoKwik Sales Portal Scraper")
    print("=" * 70)

    cookies = load_cookies(args.cookie_file)
    if not cookies:
        print("\nNO AUTH COOKIES FOUND.")
        print("The site requires Google login. Please provide cookies:")
        print("  Option A: export GOOGLE_COOKIES='SID=xxx; SSID=yyy; ...'")
        print("  Option B: place cookies.json in this directory")
        print("\nSee script header for detailed instructions.")
        print("\nContinuing anyway (will likely hit login redirect)...\n")

    # --- Scrape ---
    print("\n[1/4] Crawling pages...")
    all_files, all_pages = await scrape_all(TARGET_URL, cookies)

    print(f"\n  Done: {len(all_pages)} pages, {len(all_files)} files found")

    # --- Download files ---
    print("\n[2/4] Downloading files...")
    await download_all_files(all_files, cookies)

    # --- Save outputs ---
    print("\n[3/4] Saving output files...")
    txt_path, csv_path, json_path = save_outputs(all_files, all_pages)

    # --- Create ZIP ---
    print("\n[4/4] Creating ZIP bundle...")
    zip_path = create_zip(all_files, txt_path, csv_path, json_path)

    # --- Summary ---
    print("\n" + "=" * 70)
    print("COMPLETE")
    print("=" * 70)
    print(f"Pages scraped   : {len(all_pages)}")
    print(f"Files found     : {len(all_files)}")
    print(f"Files downloaded: {sum(1 for x in all_files if x.get('downloaded'))}")
    print(f"\nDownloadable ZIP: {zip_path}")
    print(f"ZIP size        : {zip_path.stat().st_size / 1024:.1f} KB")
    print()

    if all_files:
        print("Files found:")
        for f in all_files:
            status = f"downloaded ({f.get('size_kb')} KB)" if f.get("downloaded") else f"failed: {f.get('error','')}"
            print(f"  [{f['file_type']:20s}] {f['link_text'][:45]:45s} | {status}")
    else:
        print("No files found. Check page_content.txt for text content.")

    print(f"\n=> Your single downloadable file: {zip_path.name}")


if __name__ == "__main__":
    asyncio.run(main())
