"""
Freelancer Data Explorer — Flask web app
Serves the freelancers.csv data with live filtering, search, and CSV export.
"""

import io
import re
import pandas as pd
from pathlib import Path
from flask import Flask, render_template, request, jsonify, Response

app = Flask(__name__)

DATA_FILE = Path(__file__).resolve().parent / "freelancers.csv"


def load_data() -> pd.DataFrame:
    df = pd.read_csv(DATA_FILE, encoding="utf-8-sig")

    # Extract US state from location string  e.g. "Austin, Texas, United States" → "Texas"
    df["State"] = df["Location"].str.extract(r",\s*([A-Za-z ]+),\s*United States")[0].str.strip()

    # Numeric hourly rate for range filtering
    df["Rate_Num"] = (
        df["Hourly_Rate"]
        .str.extract(r"\$(\d+)")[0]
        .astype(float, errors="ignore")
    )

    return df


# ── load once at startup ──────────────────────────────────────────────────────
DF = load_data()

CATEGORIES = sorted(DF["Category"].dropna().unique().tolist())
STATES      = sorted(DF["State"].dropna().unique().tolist())
RATE_MIN    = int(DF["Rate_Num"].dropna().min())
RATE_MAX    = int(DF["Rate_Num"].dropna().max())


# ── helpers ───────────────────────────────────────────────────────────────────

def apply_filters(df: pd.DataFrame, args: dict) -> pd.DataFrame:
    q         = args.get("q", "").strip().lower()
    category  = args.get("category", "")
    state     = args.get("state", "")
    rate_min  = args.get("rate_min", RATE_MIN, type=int)
    rate_max  = args.get("rate_max", RATE_MAX, type=int)
    feedback  = args.get("feedback", "")      # "yes" → only rated freelancers

    # Free-text search across name / title / skills / bio
    if q:
        mask = (
            df["Name"].str.lower().str.contains(q, na=False)
            | df["Title"].str.lower().str.contains(q, na=False)
            | df["Skills"].str.lower().str.contains(q, na=False)
            | df["Bio"].str.lower().str.contains(q, na=False)
        )
        df = df[mask]

    if category:
        df = df[df["Category"] == category]

    if state:
        df = df[df["State"] == state]

    # Hourly rate range
    rate_mask = (
        df["Rate_Num"].isna()
        | ((df["Rate_Num"] >= rate_min) & (df["Rate_Num"] <= rate_max))
    )
    df = df[rate_mask]

    if feedback == "yes":
        df = df[df["Feedback"].notna() & (df["Feedback"] != "N/A")]

    return df


# ── routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    stats = {
        "total":        len(DF),
        "web_dev":      int((DF["Category"] == "Web Development").sum()),
        "perf_mktg":    int((DF["Category"] == "Performance Marketing").sum()),
        "fullstack":    int((DF["Category"] == "Full Stack Development").sum()),
        "states":       len(STATES),
    }
    return render_template(
        "index.html",
        categories=CATEGORIES,
        states=STATES,
        rate_min=RATE_MIN,
        rate_max=RATE_MAX,
        stats=stats,
    )


@app.route("/api/freelancers")
def api_freelancers():
    page     = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 25, type=int)
    sort_by  = request.args.get("sort", "Name")
    sort_dir = request.args.get("dir", "asc")

    df = apply_filters(DF.copy(), request.args)

    # Sort
    valid_cols = ["Name", "Title", "Location", "Hourly_Rate", "Feedback",
                  "Earnings_Per_Yr", "Category", "Rate_Num"]
    if sort_by not in valid_cols:
        sort_by = "Name"

    if sort_by == "Rate_Num":
        df = df.sort_values("Rate_Num", ascending=(sort_dir == "asc"), na_position="last")
    else:
        df = df.sort_values(sort_by, ascending=(sort_dir == "asc"), na_position="last")

    total   = len(df)
    pages   = max(1, (total + per_page - 1) // per_page)
    page    = max(1, min(page, pages))
    start   = (page - 1) * per_page
    subset  = df.iloc[start:start + per_page]

    cols = ["Name", "Title", "Location", "State", "Hourly_Rate",
            "Feedback", "Earnings_Per_Yr", "Skills", "Category", "Profile_URL", "Bio"]
    records = subset[cols].fillna("N/A").to_dict(orient="records")

    return jsonify({
        "total":    total,
        "page":     page,
        "pages":    pages,
        "per_page": per_page,
        "data":     records,
    })


@app.route("/api/stats")
def api_stats():
    df    = apply_filters(DF.copy(), request.args)
    total = len(df)

    by_cat   = df["Category"].value_counts().to_dict()
    by_state = df["State"].value_counts().head(10).to_dict()

    rate_vals = df["Rate_Num"].dropna()
    rate_stats = {
        "min":    round(float(rate_vals.min()), 2) if len(rate_vals) else 0,
        "max":    round(float(rate_vals.max()), 2) if len(rate_vals) else 0,
        "median": round(float(rate_vals.median()), 2) if len(rate_vals) else 0,
        "avg":    round(float(rate_vals.mean()), 2) if len(rate_vals) else 0,
    }

    return jsonify({
        "total":      total,
        "by_category": by_cat,
        "by_state":    by_state,
        "rates":       rate_stats,
    })


@app.route("/api/download")
def api_download():
    df = apply_filters(DF.copy(), request.args)

    export_cols = ["Name", "Title", "Location", "Hourly_Rate", "Feedback",
                   "Earnings_Per_Yr", "Skills", "Bio", "Category", "Profile_URL"]
    df = df[export_cols].fillna("N/A")

    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    buf.seek(0)

    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=freelancers_export.csv"},
    )


if __name__ == "__main__":
    print(f"Loaded {len(DF)} freelancers from {DATA_FILE}")
    app.run(debug=True, host="0.0.0.0", port=5000)
