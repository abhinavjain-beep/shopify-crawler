"""Category-level allocation breakdown.

V1 scope: only category buckets exposed by mfapi.in's scheme metadata
(``scheme_category`` and ``scheme_type``). Stock-level fund overlap requires
parsing AMFI scheme portfolio disclosures (quarterly PDFs) and is out of
scope for v1 - see TODO below.
"""

from __future__ import annotations

import pandas as pd


def category_breakdown(enriched: pd.DataFrame) -> pd.DataFrame:
    """Group holdings by ``scheme_category`` and sum current values.

    Returns columns: ``scheme_category``, ``current_value``, ``share_pct``.
    """
    if enriched.empty or "current_value" not in enriched.columns:
        return pd.DataFrame(columns=["scheme_category", "current_value", "share_pct"])

    df = enriched.copy()
    df["scheme_category"] = df["scheme_category"].fillna("Unknown").replace("", "Unknown")
    grouped = (
        df.groupby("scheme_category", as_index=False)["current_value"]
        .sum()
        .sort_values("current_value", ascending=False)
        .reset_index(drop=True)
    )
    total = grouped["current_value"].sum()
    grouped["share_pct"] = grouped["current_value"] / total if total else 0.0
    return grouped


def asset_class_breakdown(enriched: pd.DataFrame) -> pd.DataFrame:
    """Coarse equity/debt/hybrid/other split derived from scheme_category.

    AMFI category strings are inconsistent across fund houses; we use a
    keyword heuristic that covers the common SEBI categories.
    """
    if enriched.empty:
        return pd.DataFrame(columns=["asset_class", "current_value", "share_pct"])

    def classify(category: str) -> str:
        c = (category or "").lower()
        if any(k in c for k in ("equity", "elss", "index", "etf")):
            return "Equity"
        if any(k in c for k in ("debt", "liquid", "gilt", "bond", "corporate", "credit", "duration")):
            return "Debt"
        if any(k in c for k in ("hybrid", "balanced", "arbitrage", "multi asset", "multi-asset")):
            return "Hybrid"
        if any(k in c for k in ("gold", "silver", "commodity")):
            return "Commodity"
        return "Other"

    df = enriched.copy()
    df["asset_class"] = df["scheme_category"].fillna("").map(classify)
    grouped = (
        df.groupby("asset_class", as_index=False)["current_value"]
        .sum()
        .sort_values("current_value", ascending=False)
        .reset_index(drop=True)
    )
    total = grouped["current_value"].sum()
    grouped["share_pct"] = grouped["current_value"] / total if total else 0.0
    return grouped


# TODO: stock-level overlap analysis. Requires parsing AMFI scheme portfolio
# disclosures (https://www.amfiindia.com/research-information/other-data/monthly-portfolio-disclosure)
# which are PDFs per fund house. Not in v1.
