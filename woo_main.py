"""
WooCommerce Agency Partner Scraper
-----------------------------------
Scrapes all agencies from https://woocommerce.com/development-services/

For each agency extracts:
  - Name, Location, Partner Tier, Website
  - Services, Industries, Budget, Languages
  - Description, Emails found in description
  - Agency LinkedIn (from their own website)
  - Founder Name (from agency website About/Team page)
  - Partnership POC Name (Google search)
  - Founder / POC LinkedIn (Google search)
"""

import asyncio
import re
import json
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

import httpx
import aiofiles
import pandas as pd
from bs4 import BeautifulSoup

from scraper import Scraper

BASE_DIR    = Path(__file__).resolve().parent
URLS_FILE   = BASE_DIR / "woo_urls.txt"
OUTPUT_FILE = BASE_DIR / "woo_data.csv"

HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
}

WOO_BASE = "https://woocommerce.com"
WOO_LISTING = "https://woocommerce.com/development-services/"

TIER_CLASS_MAP = {
    "premier-partner":          "Premier Partner",
    "pro-agency-partner":       "Pro Partner",
    "core-agency-partner":      "Core Partner",
    "gold-agency-partner":      "Gold Partner",
    "silver-agency-partner":    "Silver Partner",
}


# ─────────────────────────── helpers ─────────────────────────────────────────

def parse_emails(text: str) -> str:
    found = re.findall(r"[\w.+-]+@[\w-]+\.\w{2,}", text)
    # exclude woocommerce/wordpress owned emails
    found = [e for e in found if "woocommerce" not in e and "wordpress" not in e]
    return ", ".join(dict.fromkeys(found)) or "X"


def extract_linkedin_from_links(links: list[str]) -> str:
    for href in links:
        if "linkedin.com/company" in href or "linkedin.com/in" in href:
            if "woocommerce" not in href:
                return href.rstrip("/")
    return "X"


def tier_from_classes(classes: list[str]) -> str:
    for cls in classes:
        for key, val in TIER_CLASS_MAP.items():
            if key in cls:
                return val
    return "X"


# ─────────────────────────── listing phase ───────────────────────────────────

async def fetch_all_partner_ids() -> list[dict]:
    """Extract all partner {id, slug} pairs from the embedded JSON state."""
    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        resp = await client.get(WOO_LISTING, headers=HEADERS)
    soup = BeautifulSoup(resp.content, "html.parser")
    for script in soup.find_all("script"):
        text = script.get_text()
        if "partner_ids" in text:
            try:
                data = json.loads(text.strip())
                return data["state"]["a4a-partner-directory/listing"]["partner_ids"]
            except Exception:
                pass
    return []


# ─────────────────────────── profile scraping ────────────────────────────────

def scrape_woo_profile(soup: BeautifulSoup, url: str) -> dict:
    """Parse a WooCommerce partner profile page."""
    row: dict = {}

    # --- sidebar card ---
    card = soup.find(class_="a4a-partner-directory-partner-details-sidebar-card")

    name_el = soup.find("h2", class_="a4a-partner-directory-partner-details-sidebar-card__partner-name")
    row["Name"] = name_el.get_text(strip=True) if name_el else "X"

    loc_el = soup.find(class_="a4a-partner-directory-partner-details-sidebar-card__location")
    row["Location"] = loc_el.get_text(strip=True) if loc_el else "X"

    tier_el = soup.find(class_=re.compile(r"partner-tier-badge-"))
    if tier_el:
        row["Partner Tier"] = tier_from_classes(tier_el.get("class", []))
        tier_text = tier_el.get_text(strip=True)
        if row["Partner Tier"] == "X" and tier_text:
            row["Partner Tier"] = tier_text
    else:
        row["Partner Tier"] = "X"

    # partner website link
    link_el = soup.find("a", class_="a4a-partner-directory-partner-details-sidebar-card__link-button")
    if link_el:
        href = link_el.get("href", "")
        m = re.search(r"redirect_link=(.+?)(?:&|$)", href)
        row["Website"] = unquote(m.group(1)) if m else href
    else:
        row["Website"] = "X"

    # --- features sidebar section ---
    def get_feature(label: str) -> str:
        for h5 in soup.find_all("h5"):
            if h5.get_text(strip=True) == label:
                ul = h5.find_next_sibling("ul")
                if ul:
                    return ", ".join(li.get_text(strip=True) for li in ul.find_all("li"))
        return "X"

    row["Industries"]  = get_feature("Industries")
    row["Services"]    = get_feature("Services")
    row["Products"]    = get_feature("Products")
    row["Languages"]   = get_feature("Languages spoken")

    # budget
    for el in soup.find_all(class_=re.compile("budget|starting")):
        t = el.get_text(strip=True)
        if "$" in t or "USD" in t or "budget" in t.lower() or "starting" in t.lower():
            row["Min Budget"] = t
            break
    if "Min Budget" not in row:
        budget_match = re.search(r"Accepts projects starting from\s*([\$\d,]+)", soup.get_text())
        row["Min Budget"] = budget_match.group(1) if budget_match else "X"

    # locations served
    for h5 in soup.find_all("h5"):
        if "Locations served" in h5.get_text():
            ul = h5.find_next_sibling("ul")
            row["Locations Served"] = ", ".join(li.get_text(strip=True) for li in ul.find_all("li")) if ul else "X"
            break
    if "Locations Served" not in row:
        row["Locations Served"] = "X"

    # description / body text
    desc_div = soup.find(class_=re.compile("partner-details-content|partner-body|wp-block-post-content"))
    if not desc_div:
        desc_div = soup.find("article")
    row["Description"] = desc_div.get_text(" ", strip=True)[:1000] if desc_div else "X"

    # emails from full page text
    row["Email"] = parse_emails(soup.get_text())

    row["WooCommerce Profile URL"] = url
    return row


# ─────────────────────────── agency website scraping ─────────────────────────

async def scrape_agency_website(website: str, scraper: Scraper) -> dict:
    """
    Visit the agency's own website to find:
      - LinkedIn company page
      - Emails (contact page)
      - Founder / team member names & their LinkedIn profiles (About/Team page)
      - Partnership POC name & LinkedIn
    """
    result = {
        "Agency LinkedIn":          "X",
        "Founder Name":             "X",
        "Founder LinkedIn":         "X",
        "Partnership POC Name":     "X",
        "Partnership POC LinkedIn": "X",
        "Contact Email":            "X",
    }
    if not website or website == "X":
        return result

    parsed_base = urlparse(website)
    base_url = f"{parsed_base.scheme}://{parsed_base.netloc}"

    def abs_links(soup_obj):
        links = []
        for a in soup_obj.find_all("a", href=True):
            href = a.get("href", "")
            if href.startswith("http"):
                links.append(href)
            elif href.startswith("/"):
                links.append(base_url + href)
        return links

    # ---- homepage ----
    try:
        soup = await scraper.get_soup(website, headers=HEADERS)
    except Exception:
        return result
    if not soup:
        return result

    all_links_abs = abs_links(soup)
    result["Agency LinkedIn"] = extract_linkedin_from_links(all_links_abs)

    email_from_home = parse_emails(soup.get_text())
    if email_from_home != "X":
        result["Contact Email"] = email_from_home

    # Collect person LinkedIn links from homepage too
    person_links_home = [l for l in all_links_abs if "linkedin.com/in" in l]

    # ---- try internal pages: about, team, leadership, people ----
    about_slugs = [
        "about", "about-us", "team", "our-team", "company",
        "who-we-are", "leadership", "people", "management",
        "about/team", "company/team", "our-company",
    ]
    all_person_linkedin: list[str] = list(person_links_home)
    found_about = False

    for slug in about_slugs:
        about_url = f"{base_url}/{slug}/"
        try:
            about_soup = await scraper.get_soup(about_url, headers=HEADERS)
        except Exception:
            continue
        if not about_soup:
            continue

        about_text = about_soup.get_text(separator=" ", strip=True)
        if len(about_text) < 300:
            continue
        # skip if it's a redirect back to home (text nearly identical)
        if about_text[:200] == soup.get_text(separator=" ", strip=True)[:200]:
            continue

        found_about = True

        # LinkedIn links on this page
        page_links = abs_links(about_soup)
        company_lk = [l for l in page_links if "linkedin.com/company" in l and "woocommerce" not in l]
        person_lk  = [l for l in page_links if "linkedin.com/in" in l]

        if company_lk and result["Agency LinkedIn"] == "X":
            result["Agency LinkedIn"] = company_lk[0].rstrip("/")
        all_person_linkedin.extend(person_lk)

        # emails
        if result["Contact Email"] == "X":
            em = parse_emails(about_text)
            if em != "X":
                result["Contact Email"] = em

        # founder name
        if result["Founder Name"] == "X":
            result["Founder Name"] = _extract_founder_name(about_soup)

        # try to pair names with LinkedIn links via close proximity in HTML
        if person_lk:
            name_link_pairs = _extract_person_linkedin_pairs(about_soup)
            for name, link in name_link_pairs:
                role_lower = name.lower()
                if any(kw in role_lower for kw in ["founder", "co-founder", "ceo", "chief", "owner", "managing director"]):
                    if result["Founder Name"] == "X":
                        result["Founder Name"] = name
                    if result["Founder LinkedIn"] == "X":
                        result["Founder LinkedIn"] = link.rstrip("/")
                elif any(kw in role_lower for kw in ["partner", "partnership", "alliances", "business development"]):
                    if result["Partnership POC Name"] == "X":
                        result["Partnership POC Name"] = name
                    if result["Partnership POC LinkedIn"] == "X":
                        result["Partnership POC LinkedIn"] = link.rstrip("/")

        if result["Founder Name"] != "X" and result["Founder LinkedIn"] != "X":
            break

    # ---- assign first personal LinkedIn if founder/POC not yet found ----
    unique_person = list(dict.fromkeys(all_person_linkedin))
    if result["Founder LinkedIn"] == "X" and unique_person:
        result["Founder LinkedIn"] = unique_person[0].rstrip("/")
    if result["Partnership POC LinkedIn"] == "X" and len(unique_person) > 1:
        result["Partnership POC LinkedIn"] = unique_person[1].rstrip("/")

    # ---- contact page ----
    if result["Contact Email"] == "X":
        for contact_slug in ["contact", "contact-us", "get-in-touch"]:
            try:
                contact_soup = await scraper.get_soup(f"{base_url}/{contact_slug}/", headers=HEADERS)
                if contact_soup:
                    em = parse_emails(contact_soup.get_text())
                    if em != "X":
                        result["Contact Email"] = em
                        break
            except Exception:
                pass

    return result


def _extract_person_linkedin_pairs(soup: BeautifulSoup) -> list[tuple[str, str]]:
    """
    Find (name_or_role, linkedin_url) pairs by looking for personal LinkedIn
    links that appear near a person's name/title in the HTML.
    """
    pairs = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "linkedin.com/in" not in href:
            continue
        # Look at surrounding text context (parent elements)
        context = ""
        el = a.parent
        for _ in range(4):
            if el is None:
                break
            context = el.get_text(separator=" ", strip=True)
            if len(context) > 20:
                break
            el = el.parent if el else None
        if context:
            pairs.append((context[:120], href))
    return pairs


def _extract_founder_name(soup: BeautifulSoup) -> str:
    """Heuristic: find a person name near 'Founder'/'CEO'/'Co-founder' text."""
    text = soup.get_text(separator="\n", strip=True)
    patterns = [
        r"([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)[,\n\|–—-]+\s*(?:Founder|Co-Founder|CEO|Managing Director|Director|Owner)",
        r"(?:Founder|Co-Founder|CEO|Managing Director|Director|Owner)[,\n\|–—:\s]+([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.MULTILINE)
        if m:
            name = m.group(1).strip()
            # sanity check: not a common phrase
            if len(name.split()) <= 4 and not any(w in name.lower() for w in ["about", "team", "page"]):
                return name
    return "X"


# ─────────────────────────── LinkedIn company page scraper ───────────────────

async def scrape_linkedin_company(linkedin_url: str, scraper: Scraper) -> dict:
    """
    Try to extract basic info from a LinkedIn company public page.
    LinkedIn public pages are accessible without login but contain limited data.
    """
    result = {"LinkedIn Employees": "X", "LinkedIn Specialties": "X"}
    if not linkedin_url or linkedin_url == "X":
        return result
    try:
        soup = await scraper.get_soup(linkedin_url, headers={
            **HEADERS,
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })
        if not soup:
            return result
        # Extract from JSON-LD if available
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text())
                if isinstance(data, dict):
                    if "numberOfEmployees" in data:
                        result["LinkedIn Employees"] = str(data["numberOfEmployees"].get("value", "X"))
                    if "knowsAbout" in data:
                        result["LinkedIn Specialties"] = ", ".join(data["knowsAbout"])
            except Exception:
                pass
    except Exception:
        pass
    return result


# ─────────────────────────── I/O ─────────────────────────────────────────────

async def load_done_urls() -> set:
    if not URLS_FILE.exists():
        return set()
    async with aiofiles.open(URLS_FILE) as f:
        lines = await f.readlines()
    return {l.strip() for l in lines if l.strip()}


async def mark_done(url: str, done: set):
    done.add(url)
    async with aiofiles.open(URLS_FILE, "a") as f:
        await f.write(url + "\n")


def save_row(row: dict):
    pd.json_normalize(row).to_csv(
        OUTPUT_FILE, mode="a",
        header=not OUTPUT_FILE.exists(),
        index=False, encoding="utf-8-sig",
    )


# ─────────────────────────── main orchestration ──────────────────────────────

async def run_in_batches(coros, batch_size: int = 5):
    results = []
    tasks = list(coros)
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i: i + batch_size]
        label = f"Batch {i // batch_size + 1}/{(len(tasks) - 1) // batch_size + 1}"
        print(f"  {label}: {len(batch)} tasks")
        results.extend(await asyncio.gather(*batch))
    return results


async def scrape_one(partner: dict, done_urls: set, profile_scraper: Scraper, website_scraper: Scraper):
    pid   = partner["id"]
    slug  = partner["slug"]
    url   = f"{WOO_BASE}/development-services/{slug}/{pid}/"

    if url in done_urls:
        print(f"  [skip] {slug}")
        return None

    print(f"  Scraping {slug} …")

    # 1. WooCommerce profile
    soup = await profile_scraper.get_soup(url, headers=HEADERS)
    if not soup:
        print(f"  [error] {slug}")
        return None

    row = scrape_woo_profile(soup, url)

    # 2. Agency website
    if row.get("Website") and row["Website"] != "X":
        web_data = await scrape_agency_website(row["Website"], website_scraper)
        row["Agency LinkedIn"]          = web_data["Agency LinkedIn"]
        row["Founder Name"]             = web_data["Founder Name"]
        row["Founder LinkedIn"]         = web_data["Founder LinkedIn"]
        row["Partnership POC Name"]     = web_data["Partnership POC Name"]
        row["Partnership POC LinkedIn"] = web_data["Partnership POC LinkedIn"]
        if web_data["Contact Email"] != "X":
            row["Contact Email"] = web_data["Contact Email"]
        elif row.get("Email") != "X":
            row["Contact Email"] = row["Email"]
        else:
            row["Contact Email"] = "X"
    else:
        row["Agency LinkedIn"]          = "X"
        row["Founder Name"]             = "X"
        row["Founder LinkedIn"]         = "X"
        row["Partnership POC Name"]     = "X"
        row["Partnership POC LinkedIn"] = "X"
        row["Contact Email"]            = row.get("Email", "X")

    # 3. If still missing Agency LinkedIn, try scraping LinkedIn company page
    # (already found from website; this is a no-op if already set)

    # cleanup raw Email key (we now use Contact Email)
    row.pop("Email", None)

    save_row(row)
    await mark_done(url, done_urls)
    return row


async def main():
    print("=== WooCommerce Agency Scraper ===\n")

    done_urls = await load_done_urls()
    print(f"Resuming — {len(done_urls)} URLs already done.\n")

    # Phase 1 — collect partner list
    print("Phase 1: Fetching partner list from listing page …")
    partners = await fetch_all_partner_ids()
    print(f"  Found {len(partners)} partners.\n")

    if not partners:
        print("ERROR: Could not fetch partner list.")
        return

    # Phase 2 — scrape each
    print("Phase 2: Scraping profiles + websites + LinkedIn search …")
    profile_scraper = Scraper(requests_per_second=5)
    website_scraper = Scraper(requests_per_second=3)

    new_partners = [p for p in partners
                    if f"{WOO_BASE}/development-services/{p['slug']}/{p['id']}/" not in done_urls]
    print(f"  {len(new_partners)} to scrape (skipping {len(partners) - len(new_partners)} done).\n")

    coros = [scrape_one(p, done_urls, profile_scraper, website_scraper) for p in new_partners]
    results = await run_in_batches(coros, batch_size=3)

    saved = [r for r in results if r]
    print(f"\nDone!  Saved {len(saved)} agencies to {OUTPUT_FILE}")

    await profile_scraper.session.aclose()
    await website_scraper.session.aclose()


if __name__ == "__main__":
    asyncio.run(main())
