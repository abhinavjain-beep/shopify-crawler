"""Unit tests for the analytics + loader pipeline.

These tests run without network access by monkey-patching ``nav_service``.
They do *not* exercise Streamlit, casparser, or pyxirr at the integration
level - those are smoke-tested by running ``streamlit run app.py``.
"""

from __future__ import annotations

import io
from datetime import date

import pandas as pd
import pytest

from mutual_fund_dashboard.loaders.manual_loader import (
    ManualCsvError,
    load_manual_csv,
)
from mutual_fund_dashboard.services import analytics, nav_service, overlap


SAMPLE_CSV = """scheme_code,scheme_name,units,buy_nav,buy_date
120503,Parag Parikh Flexi Cap,100,50,2022-01-01
118989,Mirae Asset Large Cap,200,60,2021-06-01
"""


def test_manual_loader_normalises_to_unified_schema():
    df = load_manual_csv(io.StringIO(SAMPLE_CSV))
    assert list(df.columns) == [
        "scheme_code", "scheme_name", "units", "invested_amount",
        "avg_buy_nav", "first_buy_date", "folio", "source",
    ]
    assert df.loc[0, "invested_amount"] == pytest.approx(5000)
    assert df.loc[1, "invested_amount"] == pytest.approx(12000)
    assert df.loc[0, "scheme_code"] == "120503"
    assert df.loc[0, "source"] == "manual_csv"


def test_manual_loader_rejects_missing_columns():
    bad = "scheme_code,units\n120503,10\n"
    with pytest.raises(ManualCsvError):
        load_manual_csv(io.StringIO(bad))


def test_compute_xirr_returns_none_for_single_cashflow():
    assert analytics.compute_xirr([date(2024, 1, 1)], [-1000]) is None


def test_enrich_holdings_with_mocked_nav(monkeypatch):
    # Build a minimal holdings frame.
    holdings = load_manual_csv(io.StringIO(SAMPLE_CSV))

    fake_navs = {
        "120503": {"scheme_code": "120503", "scheme_name": "PPFC", "nav": 75.0,
                   "date": "01-04-2026", "prev_nav": 74.5},
        "118989": {"scheme_code": "118989", "scheme_name": "Mirae LC", "nav": 90.0,
                   "date": "01-04-2026", "prev_nav": 89.0},
    }
    fake_meta = {
        "120503": {"scheme_code": "120503", "fund_house": "PPFAS", "scheme_type": "Open",
                   "scheme_category": "Equity Scheme - Flexi Cap Fund", "scheme_name": "PPFC"},
        "118989": {"scheme_code": "118989", "fund_house": "Mirae", "scheme_type": "Open",
                   "scheme_category": "Equity Scheme - Large Cap Fund", "scheme_name": "Mirae LC"},
    }

    monkeypatch.setattr(nav_service, "get_latest_nav",
                        lambda code: fake_navs[code])
    monkeypatch.setattr(nav_service, "get_scheme_meta",
                        lambda code: fake_meta[code])

    enriched = analytics.enrich_holdings(holdings)

    # Row 0: 100 units * 75 = 7500 current vs 5000 invested = +2500
    row0 = enriched.iloc[0]
    assert row0["current_value"] == pytest.approx(7500)
    assert row0["pnl_abs"] == pytest.approx(2500)
    assert row0["pnl_pct"] == pytest.approx(0.5)
    assert row0["day_change"] == pytest.approx((75.0 - 74.5) * 100)

    # Row 1: 200 * 90 = 18000 vs 12000 invested = +6000
    row1 = enriched.iloc[1]
    assert row1["current_value"] == pytest.approx(18000)
    assert row1["pnl_pct"] == pytest.approx(0.5)

    summary = analytics.portfolio_summary(enriched)
    assert summary["invested"] == pytest.approx(17000)
    assert summary["current_value"] == pytest.approx(25500)
    assert summary["pnl_abs"] == pytest.approx(8500)
    assert summary["pnl_pct"] == pytest.approx(0.5)


def test_category_and_asset_class_breakdown():
    enriched = pd.DataFrame([
        {"scheme_category": "Equity Scheme - Flexi Cap Fund", "current_value": 7500},
        {"scheme_category": "Equity Scheme - Large Cap Fund", "current_value": 18000},
        {"scheme_category": "Debt Scheme - Liquid Fund", "current_value": 5000},
    ])
    cats = overlap.category_breakdown(enriched)
    assert len(cats) == 3
    assert cats["share_pct"].sum() == pytest.approx(1.0)

    classes = overlap.asset_class_breakdown(enriched)
    classes_map = dict(zip(classes["asset_class"], classes["current_value"]))
    assert classes_map["Equity"] == pytest.approx(25500)
    assert classes_map["Debt"] == pytest.approx(5000)
