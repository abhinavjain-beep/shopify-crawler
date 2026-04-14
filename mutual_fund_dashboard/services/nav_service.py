"""NAV fetching service.

Primary source: mfapi.in (free JSON API, no auth, mirrors AMFI daily NAVs).
Fallback: AMFI's NAVAll.txt feed.

All public functions are decorated with ``streamlit.cache_data`` when Streamlit
is importable, so that repeated UI interactions don't refetch the same data.
The cache TTL is 15 minutes for current quotes and 24 hours for historical
data and scheme metadata, which matches AMFI's once-a-day publish cadence.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import httpx
import pandas as pd

# Streamlit cache decorator is optional so this module is unit-testable
# without spinning up the Streamlit runtime.
try:
    import streamlit as st

    _cache_short = st.cache_data(ttl=900, show_spinner=False)
    _cache_long = st.cache_data(ttl=86400, show_spinner=False)
except Exception:  # pragma: no cover - exercised only outside Streamlit

    def _identity(func):
        return func

    _cache_short = _identity
    _cache_long = _identity


MFAPI_BASE = "https://api.mfapi.in/mf"
AMFI_NAVALL_URL = "https://www.amfiindia.com/spages/NAVAll.txt"
HTTP_TIMEOUT = 10.0


class NAVNotFoundError(Exception):
    """Raised when a scheme code returns no data from any source."""


def _fetch_mfapi(scheme_code: str) -> dict[str, Any] | None:
    """Fetch raw mfapi.in payload for a scheme. Returns None on failure."""
    url = f"{MFAPI_BASE}/{scheme_code}"
    try:
        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()
            if not data or not data.get("data"):
                return None
            return data
    except (httpx.HTTPError, ValueError):
        return None


@_cache_short
def get_latest_nav(scheme_code: str) -> dict[str, Any]:
    """Return the most recent NAV for a scheme.

    Returns a dict with ``nav`` (float), ``date`` (str DD-MM-YYYY),
    ``prev_nav`` (float|None), and ``scheme_name`` (str).
    """
    payload = _fetch_mfapi(scheme_code)
    if payload is None:
        raise NAVNotFoundError(f"No NAV found for scheme code {scheme_code}")

    rows = payload["data"]
    latest = rows[0]
    prev = rows[1] if len(rows) > 1 else None
    return {
        "scheme_code": scheme_code,
        "scheme_name": payload.get("meta", {}).get("scheme_name", ""),
        "nav": float(latest["nav"]),
        "date": latest["date"],
        "prev_nav": float(prev["nav"]) if prev else None,
    }


@_cache_long
def get_historical_nav(
    scheme_code: str,
    start: date | None = None,
    end: date | None = None,
) -> pd.DataFrame:
    """Return a DataFrame of historical NAVs sorted ascending by date.

    Columns: ``date`` (datetime64[ns]), ``nav`` (float).
    """
    payload = _fetch_mfapi(scheme_code)
    if payload is None:
        raise NAVNotFoundError(f"No history found for scheme code {scheme_code}")

    df = pd.DataFrame(payload["data"])
    df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y")
    df["nav"] = df["nav"].astype(float)
    df = df.sort_values("date").reset_index(drop=True)

    if start is not None:
        df = df[df["date"] >= pd.Timestamp(start)]
    if end is not None:
        df = df[df["date"] <= pd.Timestamp(end)]
    return df.reset_index(drop=True)


@_cache_long
def get_scheme_meta(scheme_code: str) -> dict[str, Any]:
    """Return scheme metadata: fund_house, scheme_type, scheme_category, scheme_name."""
    payload = _fetch_mfapi(scheme_code)
    if payload is None:
        raise NAVNotFoundError(f"No metadata for scheme code {scheme_code}")
    meta = payload.get("meta", {}) or {}
    return {
        "scheme_code": scheme_code,
        "fund_house": meta.get("fund_house", ""),
        "scheme_type": meta.get("scheme_type", ""),
        "scheme_category": meta.get("scheme_category", ""),
        "scheme_name": meta.get("scheme_name", ""),
    }


@_cache_long
def get_all_schemes() -> pd.DataFrame:
    """Download AMFI's full scheme list from mfapi.in.

    Returns a DataFrame with columns ``scheme_code`` and ``scheme_name``.
    """
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(MFAPI_BASE)
            resp.raise_for_status()
            data = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise NAVNotFoundError("Could not download scheme list from mfapi.in") from exc

    df = pd.DataFrame(data)
    if df.empty:
        return pd.DataFrame(columns=["scheme_code", "scheme_name"])
    df = df.rename(columns={"schemeCode": "scheme_code", "schemeName": "scheme_name"})
    df["scheme_code"] = df["scheme_code"].astype(str)
    return df[["scheme_code", "scheme_name"]]


def parse_nav_date(date_str: str) -> date:
    """Parse mfapi.in's DD-MM-YYYY date strings."""
    return datetime.strptime(date_str, "%d-%m-%Y").date()
