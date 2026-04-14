"""Streamlit entry point for the mutual fund dashboard.

Run with::

    streamlit run mutual_fund_dashboard/app.py

This is a *descriptive* analytics tool. It does not provide investment advice.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st

from loaders import load_cas_pdf, load_groww_csv, load_manual_csv
from loaders.cas_loader import CasLoadError
from loaders.groww_csv_loader import GrowwCsvError
from loaders.manual_loader import ManualCsvError
from services import analytics, nav_service, overlap, screener

# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Mutual Fund Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
)

if "holdings_df" not in st.session_state:
    st.session_state["holdings_df"] = None
if "holdings_source" not in st.session_state:
    st.session_state["holdings_source"] = None


# ---------------------------------------------------------------------------
# Sidebar: importers + controls
# ---------------------------------------------------------------------------
st.sidebar.title("Mutual Fund Dashboard")
st.sidebar.caption("Live NAVs from AMFI via mfapi.in")

importer = st.sidebar.radio(
    "Load holdings from:",
    options=["Manual CSV", "CAS PDF", "Groww CSV"],
    index=0,
)

if importer == "Manual CSV":
    st.sidebar.markdown(
        "Upload a CSV with columns: `scheme_code, scheme_name, units, buy_nav, buy_date`. "
        "See `sample_holdings.csv` for a template."
    )
    uploaded = st.sidebar.file_uploader("Manual holdings CSV", type=["csv"], key="manual_upload")
    if uploaded is not None:
        try:
            st.session_state["holdings_df"] = load_manual_csv(uploaded)
            st.session_state["holdings_source"] = "manual_csv"
            st.sidebar.success(f"Loaded {len(st.session_state['holdings_df'])} holdings.")
        except ManualCsvError as exc:
            st.sidebar.error(str(exc))

elif importer == "CAS PDF":
    st.sidebar.markdown(
        "Upload your CAMS / Karvy Consolidated Account Statement (CAS) PDF. "
        "The password is usually your PAN in upper case."
    )
    cas_file = st.sidebar.file_uploader("CAS PDF", type=["pdf"], key="cas_upload")
    cas_password = st.sidebar.text_input("CAS PDF password", type="password")
    if cas_file is not None and cas_password and st.sidebar.button("Parse CAS"):
        try:
            st.session_state["holdings_df"] = load_cas_pdf(cas_file, cas_password)
            st.session_state["holdings_source"] = "cas_pdf"
            st.sidebar.success(f"Loaded {len(st.session_state['holdings_df'])} holdings from CAS.")
        except CasLoadError as exc:
            st.sidebar.error(str(exc))

elif importer == "Groww CSV":
    st.sidebar.markdown(
        "Upload a CSV exported from the Groww app (Profile → Reports → Holdings)."
    )
    groww_file = st.sidebar.file_uploader("Groww holdings CSV", type=["csv"], key="groww_upload")
    if groww_file is not None:
        try:
            st.session_state["holdings_df"] = load_groww_csv(groww_file)
            st.session_state["holdings_source"] = "groww_csv"
            st.sidebar.success(f"Loaded {len(st.session_state['holdings_df'])} holdings.")
        except GrowwCsvError as exc:
            st.sidebar.error(str(exc))

st.sidebar.divider()
if st.sidebar.button("Refresh live NAVs", use_container_width=True):
    nav_service.get_latest_nav.clear()  # type: ignore[attr-defined]
    st.sidebar.success("NAV cache cleared. Re-fetching on next render.")

st.sidebar.divider()
st.sidebar.warning(
    "**Disclaimer:** This dashboard shows descriptive analytics on publicly "
    "available NAV data. It is **not** investment advice. Consult a SEBI-"
    "registered investment adviser before making investment decisions."
)


# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------
st.title("Mutual Fund Portfolio Dashboard")

holdings = st.session_state["holdings_df"]

if holdings is None or holdings.empty:
    st.info(
        "Upload your holdings from the sidebar to get started. "
        "Use **Manual CSV** with `sample_holdings.csv` for a quick demo."
    )
    st.stop()

with st.spinner("Fetching live NAVs..."):
    enriched = analytics.enrich_holdings(holdings)

summary = analytics.portfolio_summary(enriched)

tab_overview, tab_holdings, tab_performance, tab_categories, tab_screener = st.tabs(
    ["Overview", "Holdings", "Performance", "Categories", "Screener"]
)

# ---------------------------------------------------------------------------
# Tab 1: Overview
# ---------------------------------------------------------------------------
with tab_overview:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Invested", f"\u20b9 {summary['invested']:,.0f}")
    c2.metric(
        "Current value",
        f"\u20b9 {summary['current_value']:,.0f}",
        delta=f"\u20b9 {summary['day_change']:,.0f} today",
    )
    c3.metric(
        "Total P&L",
        f"\u20b9 {summary['pnl_abs']:,.0f}",
        delta=f"{summary['pnl_pct'] * 100:.2f}%",
    )
    avg_xirr = enriched["xirr"].dropna().mean() if "xirr" in enriched else None
    c4.metric(
        "Avg XIRR (per fund)",
        f"{avg_xirr * 100:.2f}%" if avg_xirr is not None and not pd.isna(avg_xirr) else "n/a",
    )

    st.subheader("Allocation by fund")
    if not enriched["current_value"].dropna().empty:
        pie = px.pie(
            enriched.dropna(subset=["current_value"]),
            names="scheme_name",
            values="current_value",
            hole=0.45,
        )
        pie.update_traces(textposition="inside", textinfo="percent")
        st.plotly_chart(pie, use_container_width=True)

# ---------------------------------------------------------------------------
# Tab 2: Holdings table
# ---------------------------------------------------------------------------
with tab_holdings:
    st.subheader("All holdings")
    show_cols = [
        "scheme_name", "fund_house", "scheme_category", "units",
        "avg_buy_nav", "latest_nav", "invested_amount", "current_value",
        "pnl_abs", "pnl_pct", "xirr", "nav_date",
    ]
    available = [c for c in show_cols if c in enriched.columns]
    display_df = enriched[available].copy()
    if "pnl_pct" in display_df:
        display_df["pnl_pct"] = display_df["pnl_pct"].map(
            lambda v: f"{v * 100:.2f}%" if pd.notna(v) else "n/a"
        )
    if "xirr" in display_df:
        display_df["xirr"] = display_df["xirr"].map(
            lambda v: f"{v * 100:.2f}%" if pd.notna(v) else "n/a"
        )
    st.dataframe(display_df, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Tab 3: Performance charts
# ---------------------------------------------------------------------------
with tab_performance:
    st.subheader("NAV history (rebased to 100)")
    lookback = st.selectbox(
        "Lookback window",
        options=["3M", "6M", "1Y", "3Y", "5Y", "Max"],
        index=2,
    )
    days_map = {"3M": 90, "6M": 180, "1Y": 365, "3Y": 365 * 3, "5Y": 365 * 5, "Max": 365 * 30}
    cutoff = pd.Timestamp(date.today()) - pd.Timedelta(days=days_map[lookback])

    history_frames = []
    for code, name in zip(enriched["scheme_code"], enriched["scheme_name"]):
        try:
            h = nav_service.get_historical_nav(code)
        except nav_service.NAVNotFoundError:
            continue
        h = h[h["date"] >= cutoff]
        if h.empty:
            continue
        base = h.iloc[0]["nav"]
        h = h.assign(rebased=h["nav"] / base * 100, scheme=name)
        history_frames.append(h)

    if history_frames:
        all_hist = pd.concat(history_frames, ignore_index=True)
        line = px.line(all_hist, x="date", y="rebased", color="scheme",
                       labels={"rebased": "Indexed value (start = 100)"})
        st.plotly_chart(line, use_container_width=True)
    else:
        st.info("No history available for the selected window.")

# ---------------------------------------------------------------------------
# Tab 4: Category breakdown
# ---------------------------------------------------------------------------
with tab_categories:
    st.subheader("Asset class allocation")
    asset_df = overlap.asset_class_breakdown(enriched)
    if not asset_df.empty:
        bar1 = px.bar(asset_df, x="asset_class", y="current_value",
                      text=asset_df["share_pct"].map(lambda v: f"{v * 100:.1f}%"))
        st.plotly_chart(bar1, use_container_width=True)

    st.subheader("Category allocation")
    cat_df = overlap.category_breakdown(enriched)
    if not cat_df.empty:
        bar2 = px.bar(cat_df, x="scheme_category", y="current_value",
                      text=cat_df["share_pct"].map(lambda v: f"{v * 100:.1f}%"))
        bar2.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(bar2, use_container_width=True)
        st.dataframe(cat_df, use_container_width=True, hide_index=True)

    st.caption(
        "Stock-level fund overlap (e.g. how many of your funds hold the same "
        "underlying stock) is on the roadmap. It needs AMFI's monthly portfolio "
        "disclosures."
    )

# ---------------------------------------------------------------------------
# Tab 5: Screener (descriptive only)
# ---------------------------------------------------------------------------
with tab_screener:
    st.subheader("Browse the AMFI scheme universe")
    st.caption(
        "Educational browser only. No fund here is being recommended to you - "
        "use this to look up scheme codes for your manual CSV."
    )
    name_query = st.text_input("Search by name", "")
    fund_house_query = st.text_input("Filter by fund house", "")

    try:
        all_schemes = screener.list_all_schemes()
    except nav_service.NAVNotFoundError as exc:
        st.error(str(exc))
        all_schemes = pd.DataFrame()

    filtered = screener.filter_schemes(
        all_schemes, name_query=name_query, fund_house=fund_house_query,
    )
    st.write(f"{len(filtered):,} schemes")
    st.dataframe(filtered.head(500), use_container_width=True, hide_index=True)
