"""Loader for Groww's mutual fund holdings CSV export.

Groww lets users export holdings from Profile → Reports → Holdings as a
CSV. The exact column names have changed over time and Groww has not
published a stable schema. This loader is best-effort: it accepts a small
set of common header variants and falls back gracefully when columns are
missing.

If the parser doesn't recognise your file, share a sample (with PII
redacted) and update :data:`COLUMN_ALIASES` below.
"""

from __future__ import annotations

from io import StringIO
from typing import IO

import pandas as pd

# Map normalised name -> list of header variants seen in real Groww exports.
COLUMN_ALIASES: dict[str, list[str]] = {
    "scheme_name": ["Scheme Name", "scheme_name", "Fund Name", "Name"],
    "scheme_code": ["Scheme Code", "scheme_code", "AMFI Code", "ISIN"],
    "units": ["Units", "units", "Quantity"],
    "invested_amount": ["Invested Amount", "invested", "Invested", "Investment"],
    "buy_date": ["First Investment Date", "Investment Date", "Date", "first_buy_date"],
    "avg_buy_nav": ["Average NAV", "Avg NAV", "Buy NAV", "avg_buy_nav"],
}


class GrowwCsvError(ValueError):
    """Raised when the Groww export cannot be parsed."""


def _resolve(df: pd.DataFrame, key: str) -> str | None:
    for candidate in COLUMN_ALIASES[key]:
        if candidate in df.columns:
            return candidate
    return None


def load_groww_csv(source: IO[bytes] | IO[str] | str) -> pd.DataFrame:
    """Parse a Groww holdings CSV export and normalise to the unified schema."""
    if isinstance(source, (bytes, bytearray)):
        df = pd.read_csv(StringIO(source.decode("utf-8")))
    elif hasattr(source, "read"):
        df = pd.read_csv(source)
    else:
        df = pd.read_csv(source)

    name_col = _resolve(df, "scheme_name")
    code_col = _resolve(df, "scheme_code")
    units_col = _resolve(df, "units")
    invested_col = _resolve(df, "invested_amount")
    date_col = _resolve(df, "buy_date")
    nav_col = _resolve(df, "avg_buy_nav")

    if not (units_col and (invested_col or nav_col)):
        raise GrowwCsvError(
            "Couldn't find Units and Invested/NAV columns in this CSV. "
            f"Headers seen: {list(df.columns)}. "
            "Please share a sample so we can extend the parser."
        )

    if not code_col:
        raise GrowwCsvError(
            "This Groww export doesn't contain an AMFI scheme code. "
            "Add one manually or use the CAS PDF importer instead."
        )

    units = df[units_col].astype(float)
    if invested_col:
        invested = df[invested_col].astype(float)
        avg_nav = invested / units.replace(0, pd.NA)
    else:
        avg_nav = df[nav_col].astype(float)
        invested = units * avg_nav

    out = pd.DataFrame({
        "scheme_code": df[code_col].astype(str).str.strip(),
        "scheme_name": df[name_col].astype(str) if name_col else "",
        "units": units,
        "invested_amount": invested,
        "avg_buy_nav": avg_nav,
        "first_buy_date": (
            pd.to_datetime(df[date_col]).dt.date if date_col else pd.NaT
        ),
        "folio": None,
        "source": "groww_csv",
    })
    return out
