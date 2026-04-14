# Mutual Fund Dashboard

A self-hosted Streamlit dashboard for analysing your Indian mutual fund
portfolio with **live NAVs** from AMFI (via [mfapi.in](https://api.mfapi.in/)).

> **This is not investment advice.** The dashboard shows descriptive analytics
> on publicly available NAV data. Consult a SEBI-registered investment adviser
> before making investment decisions.

## Why not connect directly to Groww?

Groww does **not** expose mutual fund holdings through any public API:

- The official `growwapi` Python SDK's `get_holdings_for_user()` only returns
  equity delivery (stock) holdings.
- Groww's own MCP server explicitly states "Mutual funds, IPOs, bonds, etc.
  will be added" — they are not supported yet.

So instead, the dashboard offers **three import paths** that all funnel into
the same unified holdings schema, and then refreshes NAVs live from AMFI.

| Importer | What it does | When to use |
|---|---|---|
| **Manual CSV** | You supply `scheme_code, units, buy_nav, buy_date` rows. | Quickest; works for any broker. |
| **CAS PDF** | Parses your monthly CAMS / Karvy Consolidated Account Statement using [`casparser`](https://github.com/codereverser/casparser). | Most accurate — captures every fund across every AMC. |
| **Groww CSV** | Parses a CSV exported from `Profile → Reports → Holdings` in the Groww app. | Best-effort; share a sample if your file isn't recognised. |

NAVs themselves refresh live (cached for 15 minutes — AMFI publishes once a
day so polling more often is wasted bandwidth).

## Quick start

```bash
# from the repo root
pip install -r mutual_fund_dashboard/requirements.txt
streamlit run mutual_fund_dashboard/app.py
```

Then in the sidebar, pick **Manual CSV** and upload `mutual_fund_dashboard/sample_holdings.csv`
to see the dashboard with demo data.

## Manual CSV format

```csv
scheme_code,scheme_name,units,buy_nav,buy_date
120503,Parag Parikh Flexi Cap Fund - Direct - Growth,250.5,55.20,2022-04-15
118989,Mirae Asset Large Cap Fund - Direct - Growth,180.0,68.50,2021-09-10
```

`scheme_code` is the AMFI scheme code. Find it via the **Screener** tab inside
the dashboard, or browse <https://api.mfapi.in/mf>.

## What's in each tab

1. **Overview** — KPI cards (Invested, Current Value, P&L, Day Change) and
   an allocation pie by fund.
2. **Holdings** — full table with current value, P&L, and XIRR per fund.
3. **Performance** — interactive multi-line chart of NAV history rebased to 100.
4. **Categories** — asset class (Equity / Debt / Hybrid / Commodity / Other)
   and SEBI category breakdown.
5. **Screener** — filter the AMFI scheme universe by name / fund house. This
   is a *browser*, not a recommender.

## Architecture

```
mutual_fund_dashboard/
├── app.py                # Streamlit entry point
├── loaders/              # Holdings importers (manual / CAS / Groww)
├── services/
│   ├── nav_service.py    # mfapi.in client + Streamlit cache
│   ├── analytics.py      # XIRR, returns, day change
│   ├── overlap.py        # Category & asset class breakdown
│   └── screener.py       # AMFI scheme browser
└── tests/                # Unit tests (no network)
```

Reused open-source projects:

- [NayakwadiS/mftool](https://github.com/NayakwadiS/mftool) — fallback NAV source
- [codereverser/casparser](https://github.com/codereverser/casparser) — CAS PDF parsing
- [pyxirr](https://pypi.org/project/pyxirr/) — fast XIRR
- [Shuvayan007/mutual-fund-portfolio-rebalancer](https://github.com/Shuvayan007/mutual-fund-portfolio-rebalancer) — reference for Streamlit + mfapi.in layout

## Running tests

```bash
pip install pytest
pytest mutual_fund_dashboard/tests/
```

Tests mock the NAV service so they don't hit the network.

## Roadmap (out of scope for v1)

- Stock-level fund overlap (needs AMFI monthly portfolio disclosures)
- SIP planner / goal projection
- Tax/capital-gains reporting
- Direct Groww account sync (waiting on Groww to publish a public MF API)
