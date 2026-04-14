"""Loader for the manual holdings CSV format.

Expected columns in the CSV:
    scheme_code, scheme_name, units, buy_nav, buy_date

``scheme_code`` is the AMFI code (look up at https://api.mfapi.in/mf or via
the Screener tab inside the dashboard). ``buy_date`` is YYYY-MM-DD.
"""

from __future__ import annotations

from io import StringIO
from typing import IO

import pandas as pd

REQUIRED_COLUMNS = {"scheme_code", "units", "buy_nav", "buy_date"}


class ManualCsvError(ValueError):
    """Raised when the manual holdings CSV is missing columns or malformed."""


def load_manual_csv(source: IO[bytes] | IO[str] | str) -> pd.DataFrame:
    """Load a manual holdings CSV and normalise to the unified schema.

    ``source`` accepts a file-like object (as Streamlit's uploader provides),
    a string of raw CSV content, or a path-like.
    """
    if isinstance(source, (bytes, bytearray)):
        df = pd.read_csv(StringIO(source.decode("utf-8")))
    elif hasattr(source, "read"):
        df = pd.read_csv(source)
    else:
        df = pd.read_csv(source)

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ManualCsvError(f"Missing required columns: {sorted(missing)}")

    df["scheme_code"] = df["scheme_code"].astype(str).str.strip()
    df["units"] = df["units"].astype(float)
    df["buy_nav"] = df["buy_nav"].astype(float)
    df["buy_date"] = pd.to_datetime(df["buy_date"]).dt.date

    if "scheme_name" not in df.columns:
        df["scheme_name"] = ""

    normalised = pd.DataFrame({
        "scheme_code": df["scheme_code"],
        "scheme_name": df["scheme_name"].fillna("").astype(str),
        "units": df["units"],
        "invested_amount": df["units"] * df["buy_nav"],
        "avg_buy_nav": df["buy_nav"],
        "first_buy_date": df["buy_date"],
        "folio": None,
        "source": "manual_csv",
    })
    return normalised
