"""
Email Enricher — Explorium API (3-step flow)
============================================
Step A  (freelancers WITH a website domain)
  1. POST /v1/businesses/match        (batch 50) → business_id per domain
  2. POST /v1/prospects               (per biz)  → prospect_ids with has_email=True
  3. POST /v1/prospects/contacts_information/bulk_enrich (batch 50) → emails

Step B  (freelancers WITHOUT a domain — name-only match)
  1. POST /v1/prospects/match         (batch 50) → prospect_id per name
  2. POST /v1/prospects/contacts_information/bulk_enrich (batch 50) → emails

Output: freelancers_emails_enriched.csv
"""

import re
import logging
import httpx
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("email_enricher")

# ── Config ─────────────────────────────────────────────────────────────────
API_KEY      = "40e74b6a-d27c-44f3-b222-7ae0842dc829"
BASE_URL     = "https://api.explorium.ai"
BATCH        = 50   # max items per API call

BASE_DIR     = Path(__file__).resolve().parent
INPUT_FILE   = BASE_DIR / "freelancers_emails_final.csv"
OUTPUT_FILE  = BASE_DIR / "freelancers_emails_enriched.csv"

HEADERS = {"api_key": API_KEY, "Content-Type": "application/json"}

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Job seniority levels to prefer (ordered best → ok)
SENIOR_LEVELS = ["owner", "founder", "president", "cxo", "partner",
                 "director", "manager", "senior", "non-managerial"]


# ── Helpers ─────────────────────────────────────────────────────────────────

def domain_from_candidate(email: str) -> str | None:
    """Extract domain from a candidate email like 'info@synoviq.com'."""
    if email and "@" in email:
        return email.split("@")[1].strip().lower()
    return None


def parse_name(full_name: str) -> tuple[str, str]:
    parts = str(full_name).strip().split()
    if len(parts) == 0:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def best_email(enrich_data: dict) -> str:
    """Pick the best email from a contacts_information enrich response."""
    prof = enrich_data.get("professions_email", "")
    if prof and EMAIL_RE.match(prof):
        return prof
    for item in enrich_data.get("emails", []):
        addr = item.get("address", "")
        if addr and EMAIL_RE.match(addr):
            return addr
    return ""


def chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


# ── API calls ────────────────────────────────────────────────────────────────

def api_post(client: httpx.Client, path: str, body: dict) -> dict:
    r = client.post(f"{BASE_URL}{path}", json=body, timeout=30)
    r.raise_for_status()
    return r.json()


def match_businesses(client: httpx.Client, domains: list[str]) -> dict[str, str]:
    """domains → {domain: business_id}  (only matched ones included)"""
    payload = {"businesses_to_match": [{"domain": d} for d in domains]}
    data = api_post(client, "/v1/businesses/match", payload)
    result = {}
    for m in data.get("matched_businesses", []):
        bid = m.get("business_id")
        dom = (m.get("input") or {}).get("domain")
        if bid and dom:
            result[dom] = bid
    return result


def prospects_for_business(client: httpx.Client, biz_id: str,
                            max_results: int = 3) -> list[str]:
    """Return up to max_results prospect_ids at this business that have email."""
    payload = {
        "mode": "full",
        "size": max_results,
        "page_size": max_results,
        "filters": {
            "business_id": {"values": [biz_id]},
            "has_email":   {"value": True},
            "job_level":   {"values": SENIOR_LEVELS},
        },
    }
    data = api_post(client, "/v1/prospects", payload)
    return [p["prospect_id"] for p in data.get("data", []) if p.get("prospect_id")]


def bulk_enrich(client: httpx.Client, prospect_ids: list[str]) -> dict[str, str]:
    """prospect_id → best email string"""
    result: dict[str, str] = {}
    for sub in chunks(prospect_ids, BATCH):
        data = api_post(client, "/v1/prospects/contacts_information/bulk_enrich",
                        {"prospect_ids": sub})
        for item in data.get("data", []):
            pid   = item.get("prospect_id", "")
            email = best_email(item.get("data") or {})
            if pid:
                result[pid] = email
    return result


def match_prospects_by_name(client: httpx.Client,
                             items: list[dict]) -> dict[int, str]:
    """items = [{idx, full_name, company_name}]  →  {idx: prospect_id}"""
    payload = {
        "prospects_to_match": [
            {"full_name": it["full_name"], "company_name": it.get("company_name")}
            for it in items
        ]
    }
    data = api_post(client, "/v1/prospects/match", payload)
    result: dict[int, str] = {}
    for i, m in enumerate(data.get("matched_prospects", [])):
        pid = m.get("prospect_id")
        if pid and i < len(items):
            result[items[i]["idx"]] = pid
    return result


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
    rows = df.to_dict("records")
    total = len(rows)
    print(f"Loaded {total} rows from {INPUT_FILE.name}\n")

    # Resume support
    done_urls: set[str] = set()
    if OUTPUT_FILE.exists():
        try:
            done_df = pd.read_csv(OUTPUT_FILE, usecols=["Profile_URL"], encoding="utf-8-sig")
            done_urls = set(done_df["Profile_URL"].dropna())
            print(f"Resuming — {len(done_urls)} already in output.\n")
        except Exception as e:
            logger.warning(f"Could not read output: {e}")

    pending = [r for r in rows if r["Profile_URL"] not in done_urls]
    if not pending:
        print("Nothing to process.")
        _print_summary()
        return

    # Categorise rows
    scraped_rows = [r for r in pending if str(r.get("Email_Source","")) == "scraped"]
    domain_rows  = []   # have a candidate email → have domain
    nodom_rows   = []   # no domain at all

    for r in pending:
        if str(r.get("Email_Source","")) == "scraped":
            continue
        cand = str(r.get("Email","") or "")
        dom  = domain_from_candidate(cand)
        if dom:
            domain_rows.append({"row": r, "domain": dom})
        else:
            nodom_rows.append(r)

    print(f"  Already scraped (keep)  : {len(scraped_rows)}")
    print(f"  Have domain (biz match) : {len(domain_rows)}")
    print(f"  Name-only (prospect match): {len(nodom_rows)}")
    print()

    output_rows: list[dict] = []

    # ── Keep scraped rows ────────────────────────────────────────────────────
    for r in scraped_rows:
        output_rows.append({**r, "Email_Source": "scraped"})

    with httpx.Client(headers=HEADERS) as client:

        # ── STEP A: domain → business → prospects → emails ───────────────────
        if domain_rows:
            print(f"=== STEP A: Business domain enrichment ({len(domain_rows)} rows) ===")
            total_a_found = 0

            for i, chunk in enumerate(chunks(domain_rows, BATCH)):
                domains = [item["domain"] for item in chunk]
                print(f"  [{i*BATCH+1:4}–{i*BATCH+len(chunk):4}]  matching {len(domains)} domains… ", end="", flush=True)

                try:
                    biz_map = match_businesses(client, domains)   # domain → biz_id
                    print(f"{len(biz_map)} matched → fetching prospects… ", end="", flush=True)

                    # Gather prospect_ids from all matched businesses
                    biz_to_pids: dict[str, list[str]] = {}
                    all_pids: list[str] = []
                    for dom, biz_id in biz_map.items():
                        try:
                            pids = prospects_for_business(client, biz_id, max_results=3)
                            if pids:
                                biz_to_pids[dom] = pids
                                all_pids.extend(pids)
                        except Exception as e:
                            logger.warning(f"prospect fetch failed for {dom}: {e}")

                    # Enrich contacts
                    pid_email: dict[str, str] = {}
                    if all_pids:
                        try:
                            pid_email = bulk_enrich(client, all_pids)
                        except Exception as e:
                            logger.warning(f"bulk enrich failed: {e}")

                    found_this = 0
                    for item in chunk:
                        row  = item["row"].copy()
                        dom  = item["domain"]
                        pids = biz_to_pids.get(dom, [])
                        email = ""
                        for pid in pids:
                            e = pid_email.get(pid, "")
                            if e:
                                email = e
                                break
                        row["Email"]        = email
                        row["Email_Source"] = "explorium-biz" if email else ""
                        output_rows.append(row)
                        if email:
                            found_this += 1

                    total_a_found += found_this
                    print(f"{found_this} emails")

                except Exception as e:
                    logger.error(f"Step A batch error: {e}")
                    for item in chunk:
                        r2 = item["row"].copy()
                        r2["Email"] = ""
                        r2["Email_Source"] = ""
                        output_rows.append(r2)
                    print("ERROR")

            print(f"\nStep A total: {total_a_found} emails from {len(domain_rows)} rows\n")

        # ── STEP B: name-only prospect match ─────────────────────────────────
        if nodom_rows:
            print(f"=== STEP B: Prospect name match ({len(nodom_rows)} rows) ===")
            total_b_found = 0

            for i, chunk in enumerate(chunks(nodom_rows, BATCH)):
                items = []
                for j, row in enumerate(chunk):
                    first, last = parse_name(str(row.get("Name", "")))
                    full = f"{first} {last}".strip()
                    items.append({"idx": j, "full_name": full, "company_name": None})

                print(f"  [{i*BATCH+1:4}–{i*BATCH+len(chunk):4}]  matching names… ", end="", flush=True)

                try:
                    idx_to_pid = match_prospects_by_name(client, items)
                    print(f"{len(idx_to_pid)} matched → enriching… ", end="", flush=True)

                    pid_email: dict[str, str] = {}
                    if idx_to_pid:
                        try:
                            pid_email = bulk_enrich(client, list(idx_to_pid.values()))
                        except Exception as e:
                            logger.warning(f"bulk enrich failed: {e}")

                    found_this = 0
                    for j, row in enumerate(chunk):
                        pid   = idx_to_pid.get(j)
                        email = pid_email.get(pid, "") if pid else ""
                        r2    = row.copy()
                        r2["Email"]        = email
                        r2["Email_Source"] = "explorium-name" if email else ""
                        output_rows.append(r2)
                        if email:
                            found_this += 1

                    total_b_found += found_this
                    print(f"{found_this} emails")

                except Exception as e:
                    logger.error(f"Step B batch error: {e}")
                    for row in chunk:
                        r2 = row.copy()
                        r2["Email"] = ""
                        r2["Email_Source"] = ""
                        output_rows.append(r2)
                    print("ERROR")

            print(f"\nStep B total: {total_b_found} emails from {len(nodom_rows)} rows\n")

    # Write output
    out_df = pd.DataFrame(output_rows)
    write_header = not OUTPUT_FILE.exists()
    out_df.to_csv(OUTPUT_FILE, mode="a", header=write_header, index=False, encoding="utf-8-sig")

    _print_summary()


def _print_summary():
    if not OUTPUT_FILE.exists():
        return
    df = pd.read_csv(OUTPUT_FILE, encoding="utf-8-sig")
    counts = df.groupby(df["Email_Source"].fillna(""))["Email_Source"].count()
    scraped    = (df["Email_Source"].fillna("") == "scraped").sum()
    exp_biz    = (df["Email_Source"].fillna("") == "explorium-biz").sum()
    exp_name   = (df["Email_Source"].fillna("") == "explorium-name").sum()
    empty      = (df["Email"].isna() | (df["Email"].fillna("") == "")).sum()

    print(f"\n{'='*60}")
    print(f"OUTPUT: {OUTPUT_FILE.name}   ({len(df)} rows)")
    print(f"  Scraped (real)             : {scraped}")
    print(f"  Explorium — biz+domain     : {exp_biz}")
    print(f"  Explorium — name match     : {exp_name}")
    print(f"  Not found                  : {empty}")
    print(f"  ─────────────────────────────")
    print(f"  TOTAL WITH EMAIL           : {scraped + exp_biz + exp_name}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
