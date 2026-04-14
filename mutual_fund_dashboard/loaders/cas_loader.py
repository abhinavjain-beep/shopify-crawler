"""Loader for CAMS / Karvy Consolidated Account Statement (CAS) PDFs.

We delegate the heavy lifting to the open-source ``casparser`` library
(https://github.com/codereverser/casparser, MIT licence) and then aggregate
the per-folio transaction stream into one row per AMFI scheme code.

CAS PDFs are password protected. The password is typically the user's PAN
in upper case, sometimes combined with date of birth - the dashboard UI
collects it via ``st.text_input(type='password')``.
"""

from __future__ import annotations

from typing import IO, Any

import pandas as pd


class CasLoadError(RuntimeError):
    """Raised when the CAS file cannot be parsed."""


def _import_casparser():
    try:
        import casparser  # noqa: WPS433 - optional dep
    except ImportError as exc:  # pragma: no cover
        raise CasLoadError(
            "casparser is not installed. Install with `pip install casparser`."
        ) from exc
    return casparser


def load_cas_pdf(file: IO[bytes] | str, password: str) -> pd.DataFrame:
    """Parse a CAS PDF and return a normalised holdings DataFrame.

    Aggregates buy/sell/switch transactions to compute current units and
    weighted average buy NAV per scheme. SIPs and lump sums are both folded
    into ``invested_amount``.
    """
    casparser = _import_casparser()

    try:
        parsed: dict[str, Any] = casparser.read_cas_pdf(file, password, output="dict")
    except Exception as exc:
        raise CasLoadError(f"Failed to parse CAS PDF: {exc}") from exc

    rows: list[dict[str, Any]] = []
    for folio in parsed.get("folios", []):
        folio_id = folio.get("folio")
        for scheme in folio.get("schemes", []):
            scheme_code = str(scheme.get("amfi") or "").strip()
            if not scheme_code:
                continue

            current_units = 0.0
            invested_amount = 0.0
            first_buy_date = None
            for txn in scheme.get("transactions", []):
                units = float(txn.get("units") or 0)
                amount = float(txn.get("amount") or 0)
                txn_type = (txn.get("type") or "").upper()

                # casparser tags purchases as PURCHASE / PURCHASE_SIP and
                # redemptions as REDEMPTION / SWITCH_OUT (with negative units).
                if "PURCHASE" in txn_type or "STT" in txn_type or "SWITCH_IN" in txn_type:
                    current_units += units
                    if amount > 0:
                        invested_amount += amount
                        date = pd.to_datetime(txn.get("date")).date()
                        if first_buy_date is None or date < first_buy_date:
                            first_buy_date = date
                elif "REDEMPTION" in txn_type or "SWITCH_OUT" in txn_type:
                    current_units += units  # units are already negative here

            if current_units <= 0 or invested_amount <= 0:
                continue

            rows.append({
                "scheme_code": scheme_code,
                "scheme_name": scheme.get("scheme", ""),
                "units": current_units,
                "invested_amount": invested_amount,
                "avg_buy_nav": invested_amount / current_units,
                "first_buy_date": first_buy_date,
                "folio": folio_id,
                "source": "cas_pdf",
            })

    if not rows:
        raise CasLoadError("CAS parsed successfully but no holdings were found.")

    # Aggregate across multiple folios that hold the same scheme.
    df = pd.DataFrame(rows)
    aggregated = (
        df.groupby("scheme_code", as_index=False)
        .agg(
            scheme_name=("scheme_name", "first"),
            units=("units", "sum"),
            invested_amount=("invested_amount", "sum"),
            first_buy_date=("first_buy_date", "min"),
            folio=("folio", lambda v: ", ".join(sorted({str(x) for x in v if x}))),
        )
    )
    aggregated["avg_buy_nav"] = aggregated["invested_amount"] / aggregated["units"]
    aggregated["source"] = "cas_pdf"
    return aggregated[[
        "scheme_code", "scheme_name", "units", "invested_amount",
        "avg_buy_nav", "first_buy_date", "folio", "source",
    ]]
