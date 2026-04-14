"""Portfolio analytics: returns, XIRR, day change, allocation.

These are *descriptive* metrics computed from holdings + live NAVs. Nothing
in this module produces buy/sell recommendations.
"""

from __future__ import annotations

from datetime import date
from typing import Iterable

import pandas as pd

from . import nav_service

# pyxirr is optional at import time so unit tests can run without it. The
# enrichment helper below falls back to a two-point CAGR approximation if
# pyxirr isn't installed.
try:
    from pyxirr import xirr as _xirr
except Exception:  # pragma: no cover
    _xirr = None


def compute_xirr(dates: Iterable[date], amounts: Iterable[float]) -> float | None:
    """Compute XIRR for a stream of cashflows.

    Convention: invested money is *negative*, current value is *positive*.
    Returns the annualised rate as a decimal (e.g. 0.123 == 12.3%) or None
    if the calculation fails or pyxirr is not available.
    """
    if _xirr is None:
        return None
    dates_list = list(dates)
    amounts_list = list(amounts)
    if len(dates_list) < 2:
        return None
    try:
        return _xirr(dates_list, amounts_list)
    except Exception:
        return None


def _two_point_cagr(invested: float, current: float, buy_date: date, today: date) -> float | None:
    """Fallback annualised return when only one buy date is known."""
    if invested <= 0 or current <= 0:
        return None
    years = max((today - buy_date).days / 365.25, 1 / 365.25)
    return (current / invested) ** (1 / years) - 1


def enrich_holdings(holdings: pd.DataFrame) -> pd.DataFrame:
    """Add live NAV-derived columns to a holdings DataFrame.

    Input columns (from any loader):
        scheme_code, scheme_name, units, invested_amount,
        avg_buy_nav, first_buy_date, folio, source

    Output adds:
        latest_nav, nav_date, prev_nav, current_value,
        pnl_abs, pnl_pct, day_change, xirr, fund_house, scheme_category
    """
    if holdings.empty:
        return holdings.assign(
            latest_nav=[], nav_date=[], prev_nav=[], current_value=[],
            pnl_abs=[], pnl_pct=[], day_change=[], xirr=[],
            fund_house=[], scheme_category=[],
        )

    enriched_rows = []
    today = date.today()
    for row in holdings.to_dict(orient="records"):
        code = str(row["scheme_code"])
        try:
            nav = nav_service.get_latest_nav(code)
            meta = nav_service.get_scheme_meta(code)
        except nav_service.NAVNotFoundError:
            enriched_rows.append({
                **row,
                "latest_nav": None, "nav_date": None, "prev_nav": None,
                "current_value": None, "pnl_abs": None, "pnl_pct": None,
                "day_change": None, "xirr": None,
                "fund_house": "", "scheme_category": "",
            })
            continue

        latest_nav = nav["nav"]
        prev_nav = nav["prev_nav"]
        units = float(row["units"])
        invested = float(row["invested_amount"])
        current_value = units * latest_nav
        pnl_abs = current_value - invested
        pnl_pct = pnl_abs / invested if invested else None
        day_change = (latest_nav - prev_nav) * units if prev_nav is not None else None

        # XIRR with two cashflows: buy (negative) and current value (positive).
        buy_date = pd.to_datetime(row["first_buy_date"]).date()
        xirr_val = compute_xirr([buy_date, today], [-invested, current_value])
        if xirr_val is None:
            xirr_val = _two_point_cagr(invested, current_value, buy_date, today)

        enriched_rows.append({
            **row,
            "latest_nav": latest_nav,
            "nav_date": nav["date"],
            "prev_nav": prev_nav,
            "current_value": current_value,
            "pnl_abs": pnl_abs,
            "pnl_pct": pnl_pct,
            "day_change": day_change,
            "xirr": xirr_val,
            "fund_house": meta["fund_house"],
            "scheme_category": meta["scheme_category"],
        })

    return pd.DataFrame(enriched_rows)


def portfolio_summary(enriched: pd.DataFrame) -> dict[str, float]:
    """Aggregate KPIs across the whole portfolio."""
    if enriched.empty:
        return {
            "invested": 0.0, "current_value": 0.0,
            "pnl_abs": 0.0, "pnl_pct": 0.0, "day_change": 0.0,
        }

    invested = float(enriched["invested_amount"].sum())
    current_value = float(enriched["current_value"].fillna(0).sum())
    pnl_abs = current_value - invested
    pnl_pct = pnl_abs / invested if invested else 0.0
    day_change = float(enriched["day_change"].fillna(0).sum())
    return {
        "invested": invested,
        "current_value": current_value,
        "pnl_abs": pnl_abs,
        "pnl_pct": pnl_pct,
        "day_change": day_change,
    }
