"""
Top 100 Credible US Freelancers — Contact Enrichment
======================================================
Selects the best 25 freelancers from each of 4 categories:
  • Shopify Development   (scraped fresh from Guru.com)
  • Web Development       (from existing freelancers.csv)
  • Full Stack Development
  • Performance Marketing

For each freelancer it visits their Guru profile to collect:
  • LinkedIn URL  (directly on Guru page — high success rate)
  • Website domain

Then uses Explorium API to find professional email:
  domain → business_id → senior prospect → email

Output: top100_freelancers.csv
Columns: Rank, Name, Category, Title, Location, Feedback, Earnings_Per_Yr,
         Profile_URL, LinkedIn, Email, Email_Source, Website
"""

import re
import asyncio
import logging
import httpx
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

from scraper import Scraper

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("top100")

BASE_DIR    = Path(__file__).resolve().parent
EXISTING    = BASE_DIR / "freelancers.csv"
OUTPUT_FILE = BASE_DIR / "top100_freelancers.csv"

SHOPIFY_URL  = "https://www.guru.com/d/freelancers/skill/shopify/"
GURU_BASE    = "https://www.guru.com"
TOP_N        = 25   # per category
PROFILE_RATE = 4    # Guru profile page requests / sec
PROFILE_BATCH= 10

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":     "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Explorium ────────────────────────────────────────────────────────────────
EXPLORIUM_KEY  = "40e74b6a-d27c-44f3-b222-7ae0842dc829"
EXPLORIUM_BASE = "https://api.explorium.ai"
EXP_HEADERS    = {"api_key": EXPLORIUM_KEY, "Content-Type": "application/json"}
SENIOR_LEVELS  = ["owner", "founder", "president", "cxo", "partner",
                  "director", "manager", "senior"]

SKIP_DOMAINS = {
    "guru.com", "linkedin.com", "facebook.com", "twitter.com", "x.com",
    "instagram.com", "youtube.com", "github.com", "t.co", "behance.net",
    "dribbble.com", "clutch.co", "apple.com", "play.google.com", "google.com",
    "yelp.com", "crunchbase.com", "upwork.com", "fiverr.com", "wa.me",
    "whatsapp.com", "telegram.me", "medium.com",
}
SOCIAL_CLASSES = {"socialIcon", "profile-web__social__icon", "c-footer__socials__social"}
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


# ── Ranking helpers ───────────────────────────────────────────────────────────

def parse_feedback(s: str) -> float:
    try:
        return float(str(s).replace("%", "").replace("N/A", "0").strip())
    except Exception:
        return 0.0


def parse_earnings(s: str) -> int:
    try:
        return int(re.sub(r"[^\d]", "", str(s)))
    except Exception:
        return 0


def top_n_from_df(df: pd.DataFrame, category: str, n: int) -> pd.DataFrame:
    sub = df[df["Category"] == category].copy()
    sub["_fb"]  = sub["Feedback"].apply(parse_feedback)
    sub["_earn"] = sub["Earnings_Per_Yr"].apply(parse_earnings)
    sub = sub.sort_values(["_fb", "_earn"], ascending=False)
    return sub.head(n).drop(columns=["_fb", "_earn"])


# ── Guru card parser (reused from freelancer_scraper.py) ─────────────────────

def _text(el, selector: str) -> str:
    found = el.select_one(selector)
    return found.get_text(strip=True) if found else "N/A"


def parse_card(card, category: str) -> dict | None:
    name_link = card.select_one("h3.freelancerAvatar__screenName a")
    if not name_link:
        return None
    profile_url = GURU_BASE + name_link.get("href", "")
    name = name_link.get_text(strip=True)

    city    = _text(card, ".freelancerAvatar__location--city").rstrip(",").strip()
    state   = _text(card, ".freelancerAvatar__location--state").rstrip(",").strip()
    country = _text(card, ".freelancerAvatar__location--country")
    if country != "United States":
        return None

    location = ", ".join(x for x in [city, state, country] if x and x != "N/A")
    title    = _text(card, ".serviceListing__title a")
    bio      = _text(card, ".serviceListing__desc")
    skills   = " | ".join(t.get_text(strip=True) for t in card.select(".skillsList__skill a")) or "N/A"

    rate_raw = _text(card, ".serviceListing__rates")
    m = re.search(r'\$[\d,]+/hr', rate_raw)
    hourly_rate = m.group(0) if m else rate_raw

    feedback = _text(card, ".freelancerAvatar__feedback")
    earnings_raw = _text(card, ".earnings__amount")
    earnings = f"${earnings_raw}/yr" if earnings_raw != "N/A" else "N/A"

    return {
        "Name": name, "Title": title, "Location": location,
        "Bio": bio[:800], "Skills": skills, "Hourly_Rate": hourly_rate,
        "Feedback": feedback, "Earnings_Per_Yr": earnings,
        "Category": category, "Profile_URL": profile_url,
    }


# ── Shopify freelancer scraper ────────────────────────────────────────────────

async def scrape_shopify(scraper: Scraper, target: int = 60) -> list[dict]:
    """Scrape US Shopify freelancers from Guru.com, return top `target` by credibility."""
    results: list[dict] = []
    seen: set[str] = set()

    for page in range(1, 30):
        url = SHOPIFY_URL if page == 1 else f"{SHOPIFY_URL}pg/{page}/"
        hdrs = {**HEADERS, "Referer": SHOPIFY_URL if page > 1 else GURU_BASE + "/"}
        resp = await scraper.get(url, headers=hdrs)
        if resp is None or resp.status_code != 200:
            break
        soup = BeautifulSoup(resp.content, "html.parser")
        cards = soup.select("div.record.findGuruRecord")
        if not cards:
            break
        for card in cards:
            data = parse_card(card, "Shopify Development")
            if data and data["Profile_URL"] not in seen:
                seen.add(data["Profile_URL"])
                results.append(data)
        print(f"  Shopify page {page}: {len(results)} US freelancers so far")
        if len(results) >= target:
            break

    # Sort by credibility
    results.sort(key=lambda r: (parse_feedback(r["Feedback"]), parse_earnings(r["Earnings_Per_Yr"])),
                 reverse=True)
    return results


# ── Profile page enrichment ───────────────────────────────────────────────────

GURU_LINKEDIN_PAGES = {
    "https://www.linkedin.com/company/gurufreelancing/",
    "https://www.linkedin.com/company/gurufreelancing",
}

def extract_linkedin(soup: BeautifulSoup) -> str:
    """Find freelancer's own LinkedIn URL from their Guru profile page."""
    for a in soup.select("a[href*='linkedin.com']"):
        href = a.get("href", "")
        if not href or href in GURU_LINKEDIN_PAGES:
            continue
        classes = set(a.get("class") or [])
        # Skip Guru footer links
        if "c-footer__socials__social" in classes:
            continue
        return href
    return ""


def extract_website_domain(soup: BeautifulSoup) -> str:
    """Extract freelancer's personal website domain (not social)."""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href.startswith("http"):
            continue
        classes = set(a.get("class") or [])
        if classes & SOCIAL_CLASSES:
            continue
        try:
            host = href.split("/")[2].lstrip("www.").split(":")[0]
        except IndexError:
            continue
        if not host or "." not in host:
            continue
        if any(host == d or host.endswith("." + d) for d in SKIP_DOMAINS):
            continue
        return host
    return ""


def extract_email_from_page(soup: BeautifulSoup) -> str:
    for a in soup.select("a[href^='mailto:']"):
        raw = a["href"].replace("mailto:", "").strip().split("?")[0]
        if EMAIL_RE.match(raw):
            return raw
    m = EMAIL_RE.search(soup.get_text(" "))
    return m.group(0) if m else ""


async def enrich_profile(row: dict, scraper: Scraper, idx: int, total: int) -> dict:
    """
    Visit Guru profile page: extract LinkedIn, website domain, direct email.
    If a website domain is found, also visit the homepage looking for email + LinkedIn.
    """
    soup = await scraper.get_soup(row["Profile_URL"], headers=HEADERS)
    linkedin     = ""
    domain       = ""
    direct_email = ""

    if soup:
        linkedin     = extract_linkedin(soup)
        domain       = extract_website_domain(soup)
        direct_email = extract_email_from_page(soup)

    # Also scrape their website homepage for email + LinkedIn
    if domain and (not direct_email or not linkedin):
        site_url = f"https://{domain}"
        site_soup = await scraper.get_soup(site_url, headers=HEADERS)
        if site_soup:
            if not direct_email:
                direct_email = extract_email_from_page(site_soup)
            if not linkedin:
                linkedin = extract_linkedin(site_soup)

    result = {**row, "LinkedIn": linkedin, "Website": domain,
              "Email": direct_email, "Email_Source": "direct" if direct_email else ""}
    status = []
    if linkedin:     status.append("LI")
    if direct_email: status.append(f"email:{direct_email[:30]}")
    elif domain:     status.append(f"dom:{domain}")
    print(f"  [{idx:3}/{total}]  {row['Name'][:28]:28}  {', '.join(status) or '—'}")
    return result


# ── Explorium email enrichment ────────────────────────────────────────────────

def exp_match_businesses(client: httpx.Client, domains: list[str]) -> dict[str, str]:
    payload = {"businesses_to_match": [{"domain": d} for d in domains]}
    r = client.post(f"{EXPLORIUM_BASE}/v1/businesses/match", json=payload, timeout=30)
    r.raise_for_status()
    result = {}
    for m in r.json().get("matched_businesses", []):
        bid = m.get("business_id")
        dom = (m.get("input") or {}).get("domain", "")
        if bid and dom:
            result[dom] = bid
    return result


def exp_prospects(client: httpx.Client, biz_id: str) -> list[str]:
    payload = {
        "mode": "full", "size": 3, "page_size": 3,
        "filters": {
            "business_id": {"values": [biz_id]},
            "has_email":   {"value": True},
            "job_level":   {"values": SENIOR_LEVELS},
        },
    }
    r = client.post(f"{EXPLORIUM_BASE}/v1/prospects", json=payload, timeout=30)
    r.raise_for_status()
    return [p["prospect_id"] for p in r.json().get("data", []) if p.get("prospect_id")]


def exp_bulk_enrich(client: httpx.Client, pids: list[str]) -> dict[str, str]:
    if not pids:
        return {}
    r = client.post(f"{EXPLORIUM_BASE}/v1/prospects/contacts_information/bulk_enrich",
                    json={"prospect_ids": pids}, timeout=30)
    r.raise_for_status()
    result = {}
    for item in r.json().get("data", []):
        pid = item.get("prospect_id", "")
        d   = item.get("data") or {}
        email = d.get("professions_email", "")
        if not email:
            for e in d.get("emails", []):
                email = e.get("address", "")
                if email:
                    break
        if pid:
            result[pid] = email
    return result


def explorium_enrich(rows: list[dict]) -> list[dict]:
    """For rows with a website domain and no email yet, call Explorium."""
    need_enrich = [(i, r) for i, r in enumerate(rows)
                   if r.get("Website") and not r.get("Email")]
    if not need_enrich:
        return rows

    print(f"\n=== Explorium enrichment for {len(need_enrich)} rows with domains ===")
    domains = list({r["Website"] for _, r in need_enrich})
    rows_copy = [dict(r) for r in rows]

    with httpx.Client(headers=EXP_HEADERS) as client:
        # Batch business match
        dom_to_bid: dict[str, str] = {}
        for batch_start in range(0, len(domains), 50):
            chunk = domains[batch_start: batch_start + 50]
            try:
                dom_to_bid.update(exp_match_businesses(client, chunk))
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 403:
                    print("  Explorium: out of credits — skipping email enrichment")
                    return rows
                logger.warning(f"Business match error: {e}")
            except Exception as e:
                logger.warning(f"Business match error: {e}")

        matched_count = len(dom_to_bid)
        print(f"  Matched {matched_count}/{len(domains)} domains → fetching prospects…")

        # Fetch prospects for each matched business
        dom_to_pids: dict[str, list[str]] = {}
        all_pids: list[str] = []
        for dom, bid in dom_to_bid.items():
            try:
                pids = exp_prospects(client, bid)
                if pids:
                    dom_to_pids[dom] = pids
                    all_pids.extend(pids)
            except Exception as e:
                logger.warning(f"Prospects error for {dom}: {e}")

        # Bulk enrich (retry once on failure)
        pid_email: dict[str, str] = {}
        for batch_start in range(0, len(all_pids), 50):
            chunk = all_pids[batch_start: batch_start + 50]
            for attempt in range(2):
                try:
                    pid_email.update(exp_bulk_enrich(client, chunk))
                    break
                except Exception as e:
                    if attempt == 0:
                        import time; time.sleep(3)
                    else:
                        logger.warning(f"Bulk enrich error (gave up): {e}")

        # Map emails back to rows
        found = 0
        for orig_idx, row in need_enrich:
            dom  = row.get("Website", "")
            pids = dom_to_pids.get(dom, [])
            email = ""
            for pid in pids:
                email = pid_email.get(pid, "")
                if email:
                    break
            if email:
                rows_copy[orig_idx]["Email"]        = email
                rows_copy[orig_idx]["Email_Source"] = "explorium"
                found += 1

        print(f"  Found {found} emails via Explorium\n")

    return rows_copy


# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    scraper = Scraper(requests_per_second=PROFILE_RATE, timeout=20)

    # ── 1. Select top 25 from existing 3 categories ─────────────────────────
    print("=== Loading existing freelancer data ===")
    df = pd.read_csv(EXISTING, encoding="utf-8-sig")
    selected_frames = []
    for cat in ["Web Development", "Full Stack Development", "Performance Marketing"]:
        top = top_n_from_df(df, cat, TOP_N)
        print(f"  {cat}: {len(top)} selected")
        selected_frames.append(top)

    # ── 2. Scrape Shopify category ───────────────────────────────────────────
    print("\n=== Scraping Shopify Development from Guru.com ===")
    shopify_all = await scrape_shopify(scraper, target=80)
    shopify_top = shopify_all[:TOP_N]
    print(f"  Shopify Development: {len(shopify_top)} selected (from {len(shopify_all)} scraped)\n")
    selected_frames.append(pd.DataFrame(shopify_top))

    # Combine — deduplicate by Profile_URL (keep first occurrence / highest category)
    all_rows = pd.concat(selected_frames, ignore_index=True)
    all_rows = all_rows.drop_duplicates(subset="Profile_URL", keep="first")

    # If dedup left us short of 100, backfill from the largest pool
    if len(all_rows) < 100:
        existing_urls = set(all_rows["Profile_URL"])
        for cat in ["Web Development", "Full Stack Development"]:
            pool = top_n_from_df(df, cat, 40)  # wider window
            extras = pool[~pool["Profile_URL"].isin(existing_urls)]
            need = 100 - len(all_rows)
            if need <= 0:
                break
            all_rows = pd.concat([all_rows, extras.head(need)], ignore_index=True)
            existing_urls.update(extras.head(need)["Profile_URL"].tolist())
            print(f"  Backfill from {cat}: added {min(need, len(extras))}")

    all_rows = all_rows.head(100).reset_index(drop=True)
    all_rows["Rank"] = range(1, len(all_rows) + 1)
    rows = all_rows.to_dict("records")
    total = len(rows)
    print(f"Total selected (after dedup + backfill): {total} freelancers\n")

    # ── 3. Visit each Guru profile: LinkedIn + domain + direct email ─────────
    print("=== Visiting Guru profiles for LinkedIn + contact info ===")
    try:
        enriched = []
        for batch_start in range(0, total, PROFILE_BATCH):
            batch = rows[batch_start: batch_start + PROFILE_BATCH]
            tasks = [enrich_profile(row, scraper, batch_start + i + 1, total)
                     for i, row in enumerate(batch)]
            results = await asyncio.gather(*tasks)
            enriched.extend(results)
    finally:
        await scraper.session.aclose()

    # ── 4. Explorium email enrichment for rows with website domains ──────────
    enriched = explorium_enrich(enriched)

    # ── 5. Merge any previously scraped emails ────────────────────────────────
    scraped_path = BASE_DIR / "freelancers_emails.csv"
    if scraped_path.exists():
        scraped_df = pd.read_csv(scraped_path, encoding="utf-8-sig")
        scraped_df = scraped_df[scraped_df["Email"].notna() & (scraped_df["Email"].str.strip() != "")]
        email_map  = dict(zip(scraped_df["Profile_URL"], scraped_df["Email"]))
        for r in enriched:
            if not r.get("Email") and r.get("Profile_URL") in email_map:
                r["Email"]        = email_map[r["Profile_URL"]]
                r["Email_Source"] = "scraped"

    # ── 6. Write output ───────────────────────────────────────────────────────
    out_cols = ["Rank", "Name", "Category", "Title", "Location",
                "Feedback", "Earnings_Per_Yr", "Hourly_Rate",
                "Profile_URL", "LinkedIn", "Email", "Email_Source", "Website"]
    out_df = pd.DataFrame(enriched)
    # Ensure all columns exist
    for col in out_cols:
        if col not in out_df.columns:
            out_df[col] = ""
    out_df = out_df[out_cols]
    out_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\nSaved → {OUTPUT_FILE}")

    # Summary
    with_li    = (out_df["LinkedIn"].fillna("") != "").sum()
    with_email = (out_df["Email"].fillna("") != "").sum()
    with_any   = ((out_df["LinkedIn"].fillna("") != "") |
                  (out_df["Email"].fillna("") != "")).sum()

    print(f"\n{'='*60}")
    print(f"TOP 100 FREELANCERS  —  {OUTPUT_FILE.name}")
    print(f"  With LinkedIn URL   : {with_li}")
    print(f"  With Email          : {with_email}")
    print(f"    ↳ direct (scraped): {(out_df['Email_Source']=='direct').sum()}")
    print(f"    ↳ Explorium        : {(out_df['Email_Source']=='explorium').sum()}")
    print(f"  With ANY contact    : {with_any}")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
