"""Fund screener / discovery tool.

Browses the full AMFI scheme universe (~16k schemes) with simple text +
category + return filters. This is a *browse* tool - it does not rank or
recommend funds.
"""

from __future__ import annotations

import pandas as pd

from . import nav_service


def list_all_schemes() -> pd.DataFrame:
    """Cached full list of AMFI schemes from mfapi.in."""
    return nav_service.get_all_schemes()


def filter_schemes(
    schemes: pd.DataFrame,
    name_query: str = "",
    fund_house: str = "",
) -> pd.DataFrame:
    """Apply text filters to the scheme universe.

    Both filters are case-insensitive substring matches. ``schemes`` must
    have ``scheme_code`` and ``scheme_name`` columns (as returned by
    :func:`list_all_schemes`).
    """
    if schemes.empty:
        return schemes
    out = schemes
    if name_query:
        out = out[out["scheme_name"].str.contains(name_query, case=False, na=False)]
    if fund_house:
        out = out[out["scheme_name"].str.contains(fund_house, case=False, na=False)]
    return out.reset_index(drop=True)


def compute_period_return(scheme_code: str, days: int) -> float | None:
    """Compute return over the trailing ``days`` window for a scheme.

    Returns a decimal (e.g. 0.123 == 12.3%) or None if data is missing.
    Result is *not* annualised.
    """
    try:
        history = nav_service.get_historical_nav(scheme_code)
    except nav_service.NAVNotFoundError:
        return None
    if history.empty or len(history) < 2:
        return None

    end_row = history.iloc[-1]
    cutoff = end_row["date"] - pd.Timedelta(days=days)
    earlier = history[history["date"] <= cutoff]
    if earlier.empty:
        return None
    start_row = earlier.iloc[-1]
    if start_row["nav"] == 0:
        return None
    return float(end_row["nav"] / start_row["nav"] - 1)
