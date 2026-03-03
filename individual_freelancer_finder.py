"""
Individual Freelancer Finder
============================
Scrapes the Shopify Partners Directory for US-based INDIVIDUAL freelancers
(not agencies/companies) in web development, performance marketing,
full-stack, and Shopify development.

Individual detection uses:
  1. Name pattern — 2-3 capitalized words, no company suffix
  2. Description language — uses "I/my/he/she" not "we/our team/our agency"
  3. Explicit keyword — "freelancer", "independent", "solo"

Target: 100 individual freelancers with email OR LinkedIn.
"""

import asyncio
import csv
import re
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

BASE_DIR    = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "individual_freelancers.csv"

BASE_URL      = "https://www.shopify.com"
DIRECTORY_URL = f"{BASE_URL}/partners/directory/services"

TARGET     = 100
MAX_PAGES  = 600
BATCH_SIZE = 16

HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.5",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) "
        "Gecko/20100101 Firefox/131.0"
    ),
}

CSV_FIELDS = [
    "Name", "Email", "LinkedIn", "Phone", "Website",
    "Twitter", "Facebook", "Instagram", "Youtube",
    "Location", "Shopify_Profile_URL",
]

# ---------------------------------------------------------------------------
# Company / agency name suffixes  (if found → skip)
# ---------------------------------------------------------------------------
_COMPANY_RE = re.compile(
    r"\b(LLC|L\.L\.C|Inc|Inc\.|Corp|Ltd|Co\b|Agency|Studio|Studios|Labs|Lab|"
    r"Technologies|Technology|Tech|Solutions|Digital|Media|Group|Team|"
    r"Designs?|Services|Systems|Software|Consulting|Consultancy|Creative|"
    r"Commerce|Ventures|Partners|Associates|Experts|Works|Workshop|"
    r"Collective|Hub|House|Factory|Network|Global|Enterprise|Enterprises|"
    r"Innovations?|Holding|Holdings|Company|Firm|Bureau|Sphere|Forge|"
    r"Pixels?|Devs?|Codes?|Geek|Geeks|Wizards?|Monks?|Queens?|Panel|"
    r"Medi[a]|Zone|Point|Force|Empire|Nation|Works|Village|Valley)\b",
    re.I,
)

# ---------------------------------------------------------------------------
# Individual-freelancer signals
# ---------------------------------------------------------------------------

# Words that indicate a SINGLE PERSON is writing/being described
_INDIVIDUAL_RE = re.compile(
    r"\b(I am|I'm|I have|I build|I help|I create|I develop|I work|I offer|"
    r"I specialize|I provide|I design|I focus|I partner|I've|my portfolio|"
    r"my name|as a freelancer|freelancer|independent developer|"
    r"solo developer|self.employed|one.person|one-man|working independently)\b",
    re.I,
)

# Words that clearly indicate an AGENCY (→ skip)
_AGENCY_RE = re.compile(
    r"\b(our team|our agency|our company|our developers|our experts|"
    r"our designers|we are a team|we have a team|team of|"
    r"[0-9]+\+ (developers|designers|experts|members|professionals)|"
    r"dedicated team|full.service agency|award.winning agency)\b",
    re.I,
)

# ---------------------------------------------------------------------------
# Location helpers
# ---------------------------------------------------------------------------

_US_RE = re.compile(
    r"\b(United States|USA|"
    r"Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|"
    r"Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|"
    r"Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|"
    r"Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|"
    r"New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|"
    r"Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|"
    r"Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming|"
    r"District of Columbia|D\.C\.)\b"
)


def extract_location(soup) -> str:
    for sel in [
        "div.flex.flex-col.gap-y-1:-soup-contains('Primary location') p:nth-child(2)",
        "div.flex.flex-col.gap-y-1:-soup-contains('Primary Location') p:nth-child(2)",
        "div:-soup-contains('Primary location') p",
    ]:
        try:
            tag = soup.select_one(sel)
            if tag:
                return tag.get_text(strip=True)
        except Exception:
            pass
    for tag in soup.find_all(["p", "span"], limit=600):
        if tag.find_all(recursive=False):
            continue
        text = tag.get_text(strip=True)
        if 2 < len(text) < 80 and _US_RE.search(text):
            return text
    return ""


def is_us(location: str) -> bool:
    return bool(location and _US_RE.search(location))


# ---------------------------------------------------------------------------
# Individual-freelancer classifier
# ---------------------------------------------------------------------------

def is_individual_freelancer(name: str, description: str) -> bool:
    """
    Return True when the profile is most likely a solo freelancer,
    not an agency or multi-person studio.
    """
    # Strip taglines after |, –, —, :
    base_name = re.split(r"[|–—:]", name)[0].strip()

    # If the name has an obvious company suffix → agency
    if _COMPANY_RE.search(base_name):
        return False

    # Count words in base name (strip numbers, symbols)
    words = [w for w in base_name.split() if re.match(r"[A-Za-z]{2,}", w)]
    looks_like_person = (
        2 <= len(words) <= 3
        and all(w[0].isupper() for w in words)
    )

    desc = description or ""

    # Strong agency signal → reject even if name looks like a person
    if _AGENCY_RE.search(desc):
        return False

    # Explicit individual signal in description → accept
    if _INDIVIDUAL_RE.search(desc):
        return True

    # Name alone suggests a real person → accept cautiously
    if looks_like_person:
        return True

    # Check for 3rd-person singular with the person's first name as subject
    first = words[0] if words else ""
    if first and re.search(
        rf"\b{re.escape(first)}\s+(is|has|builds|helps|creates|develops|"
        rf"specializes|provides|designs|works|focuses)\b",
        desc,
        re.I,
    ):
        return True

    return False


# ---------------------------------------------------------------------------
# Profile parser
# ---------------------------------------------------------------------------

def parse_profile(soup, url: str) -> dict:
    data = {f: "" for f in CSV_FIELDS}
    data["Shopify_Profile_URL"] = url

    title = soup.select_one("h1.richtext.text-t4") or soup.select_one("h1")
    data["Name"] = title.get_text(strip=True) if title else ""

    phone_tag = soup.select_one("a[href*='tel:']")
    data["Phone"] = phone_tag["href"].replace("tel:", "").strip() if phone_tag else ""

    email_tag = soup.select_one("a[href*='mailto:']")
    data["Email"] = email_tag["href"].replace("mailto:", "").strip() if email_tag else ""

    website_tag = soup.select_one(
        "div.flex.flex-wrap.gap-x-2.items-center a[rel='nofollow']"
    )
    data["Website"] = website_tag["href"].strip() if website_tag else ""

    data["Location"] = extract_location(soup)

    desc_tag = soup.select_one("section[data-section-name='description']")
    data["_description"] = desc_tag.get_text(strip=True) if desc_tag else ""

    # Socials — partner's own "Social links" section only
    social_section = None
    for sel in [
        "div.flex.flex-col.gap-y-1:-soup-contains('Social links')",
        "div:-soup-contains('Social links')",
    ]:
        try:
            s = soup.select_one(sel)
            if s:
                social_section = s
                break
        except Exception:
            pass

    for a in (social_section.select("a[href]") if social_section else []):
        href = a.get("href", "")
        if not href:
            continue
        if "linkedin.com" in href and not data["LinkedIn"]:
            data["LinkedIn"] = href
        elif ("twitter.com" in href or "/x.com/" in href) and not data["Twitter"]:
            data["Twitter"] = href
        elif "instagram.com" in href and not data["Instagram"]:
            data["Instagram"] = href
        elif "facebook.com" in href and not data["Facebook"]:
            data["Facebook"] = href
        elif "youtube.com" in href and not data["Youtube"]:
            data["Youtube"] = href

    return data


def has_contact(d: dict) -> bool:
    return bool(d["Email"]) or bool(d["LinkedIn"])


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

class CSVWriter:
    def __init__(self, path: Path):
        self._path  = path
        self._seen  = set()
        self._count = 0
        self._fh    = None
        self._w     = None

    def open(self):
        self._fh = open(self._path, "w", newline="", encoding="utf-8-sig")
        self._w  = csv.DictWriter(self._fh, fieldnames=CSV_FIELDS)
        self._w.writeheader()

    def close(self):
        if self._fh:
            self._fh.close()

    def write(self, row: dict) -> bool:
        key = row.get("Shopify_Profile_URL", "")
        if key in self._seen:
            return False
        self._seen.add(key)
        self._w.writerow({f: row.get(f, "") for f in CSV_FIELDS})
        self._fh.flush()
        self._count += 1
        return True

    @property
    def count(self) -> int:
        return self._count


# ---------------------------------------------------------------------------
# Network helpers
# ---------------------------------------------------------------------------

async def fetch_html(client: httpx.AsyncClient, url: str):
    try:
        r = await client.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return BeautifulSoup(r.content, "html.parser")
    except Exception:
        pass
    return None


async def get_listing_page(client: httpx.AsyncClient, page: int):
    url  = f"{DIRECTORY_URL}?page={page}"
    soup = await fetch_html(client, url)
    if not soup:
        return [], False
    cards    = soup.select('[data-component-name="listing-profile-card"] a[href]')
    urls     = [BASE_URL + a["href"] for a in cards if a.get("href")]
    nxt      = soup.find(attrs={"data-component-name": "next-page"})
    has_next = nxt is not None and nxt.get("aria-disabled") != "true"
    return urls, has_next


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    writer    = CSVWriter(OUTPUT_FILE)
    writer.open()
    visited   = set()
    scanned   = 0
    sem       = asyncio.Semaphore(BATCH_SIZE)

    async def fetch_profile(client, url):
        async with sem:
            return await fetch_html(client, url)

    print(f"Target : {TARGET} US-based INDIVIDUAL freelancers with contact info")
    print(f"Output : {OUTPUT_FILE}\n")

    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
            for page in range(1, MAX_PAGES + 1):
                if writer.count >= TARGET:
                    break

                listing_urls, has_next = await get_listing_page(client, page)
                new_urls = [u for u in listing_urls if u not in visited]
                visited.update(listing_urls)

                print(
                    f"Page {page:>3}: {len(listing_urls):>2} listings, "
                    f"{len(new_urls):>2} new  [saved {writer.count}/{TARGET}]"
                )

                if not new_urls and page > 1:
                    print("No new listings — end of directory.")
                    break

                soups = await asyncio.gather(
                    *[fetch_profile(client, u) for u in new_urls]
                )

                for url, soup in zip(new_urls, soups):
                    if soup is None:
                        continue
                    data = parse_profile(soup, url)
                    scanned += 1

                    if not is_us(data["Location"]):
                        continue
                    if not has_contact(data):
                        continue
                    if not is_individual_freelancer(data["Name"], data["_description"]):
                        continue
                    if writer.write(data):
                        print(
                            f"  ✔ [{writer.count:>3}] "
                            f"{data['Name'][:45]:<45} "
                            f"{'✉ ' if data['Email'] else '  '}"
                            f"{'🔗' if data['LinkedIn'] else '  '} "
                            f"{data['Location']}"
                        )
                    if writer.count >= TARGET:
                        break

                if not has_next:
                    print("Last directory page reached.")
                    break

    finally:
        writer.close()

    print(f"\n{'='*60}")
    print(f"Scanned : {scanned} profiles")
    print(f"Saved   : {writer.count} individual freelancers → {OUTPUT_FILE}")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
