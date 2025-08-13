"""
ETL script for loading Waiting List Minimum Dataset (WLMDS) data into Postgres.

Features:
- Auto-discovers the latest NHS WLMDS Excel URL if WLMDS_URL is not set.
- Auto-selects a worksheet containing "provider" in its name if WLMDS_SHEET is not set.
- Accepts either an HTTP(S) URL or a local file path for WLMDS_URL.
- Parses provider-level wait times and upserts them into `wait_metrics`.
- Creates the table if it does not exist.

Usage:
    python -m etl.wlmds_loader

Environment variables:
    - DATABASE_URL (required): e.g. postgresql://user:pass@host:port/db
    - WLMDS_URL (optional): HTTP(S) URL or local path to a WLMDS Excel file.
    - WLMDS_SHEET (optional): Exact sheet name to read. If not set, auto-pick a
      sheet whose name contains "provider" (case-insensitive), else the first sheet.
"""

from __future__ import annotations

import os
import re
import logging
import tempfile
import shutil
from datetime import datetime
from typing import Iterable, Tuple, List, Dict, Optional
from urllib.parse import urljoin, urlparse

import requests  # type: ignore
import pandas as pd  # type: ignore
import psycopg2  # type: ignore
from psycopg2.extras import execute_values  # type: ignore

# ------------------------------------------------------------------------------
# Config / Logging
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

NHS_WLMDS_PAGE = (
    "https://www.england.nhs.uk/statistics/statistical-work-areas/"
    "rtt-waiting-times/wlmds/"
)

# ------------------------------------------------------------------------------
# Discovery & Download helpers
# ------------------------------------------------------------------------------

def discover_latest_wlmds_url(session: Optional[requests.Session] = None) -> str:
    """
    Fetch the NHS WLMDS page and pick the newest WLMDS-Summary-to-*.xlsx link.
    """
    s = session or requests.Session()
    logging.info("Discovering latest WLMDS URL from NHS page …")
    resp = s.get(NHS_WLMDS_PAGE, timeout=30)
    resp.raise_for_status()

    # Find all links that look like WLMDS-Summary-to-<date>.xlsx
    links = re.findall(
        r'href="([^"]*WLMDS-Summary-to-[^"]*\.xlsx)"',
        resp.text,
        flags=re.IGNORECASE,
    )
    if not links:
        raise RuntimeError("Could not find any WLMDS-Summary-to-*.xlsx links on the NHS page")

    abs_links = [urljoin(resp.url, href) for href in links]
    abs_links = sorted(set(abs_links))  # de-dup + sort
    chosen = abs_links[-1]  # last is typically the newest by filename
    logging.info("Discovered WLMDS: %s", chosen)
    return chosen


def download_to_temp(src: str) -> str:
    """
    If `src` is an HTTP(S) URL, download to a temp .xlsx.
    If `src` is a local path, copy it to a temp .xlsx (so we can always clean up).
    Returns the temp file path.
    """
    parsed = urlparse(src)
    if parsed.scheme in ("http", "https"):
        logging.info("Downloading WLMDS from %s", src)
        resp = requests.get(src, timeout=60)
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(resp.content)
            return tmp.name
    else:
        # Treat as local path
        if not os.path.exists(src):
            raise FileNotFoundError(f"Local file not found: {src}")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            shutil.copyfile(src, tmp.name)
            logging.info("Copied local WLMDS file to temp: %s", tmp.name)
            return tmp.name


def choose_provider_sheet(xls: pd.ExcelFile, explicit: Optional[str] = None) -> str:
    """
    Return the sheet to use. Prefer explicit; otherwise first one containing 'provider'.
    Fallback to first sheet if none match.
    """
    if explicit:
        if explicit in xls.sheet_names:
            return explicit
        raise ValueError(f"Worksheet '{explicit}' not found. Available: {xls.sheet_names}")

    for name in xls.sheet_names:
        if re.search(r"provider", name, re.IGNORECASE):
            return name

    # Fallback
    return xls.sheet_names[0]

# ------------------------------------------------------------------------------
# Parsing helpers
# ------------------------------------------------------------------------------

def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def _pick_first_present(df: pd.DataFrame, aliases: List[str]) -> str:
    for name in aliases:
        if name in df.columns:
            return name
    raise ValueError(f"None of the expected columns found. Tried: {aliases}. Actual: {list(df.columns)}")


def parse_wlmds_excel(path: str, sheet_name: str) -> Iterable[Tuple[str, str, datetime, float, float, float]]:
    """
    Parse a WLMDS Excel sheet into (ods_code, treatment_code, period_date, median_weeks, pct_over_18w, pct_over_52w).
    Tries to be robust to column header variations.
    """
    logging.info("Parsing Excel: %s (sheet: %s)", path, sheet_name)
    df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")  # type: ignore
    df = _normalise_columns(df)

    # Typical column header variants seen across releases:
    ods_aliases = [
        "organisation code", "organisation code (ods)", "provider code", "org code"
    ]
    tfn_aliases = [
        "treatment function code", "treatment function", "tfc code", "tfc"
    ]
    period_aliases = [
        "period ending", "period end", "period", "month", "reporting period"
    ]
    median_aliases = [
        "median wait", "median wait (weeks)", "median (weeks)", "median weeks"
    ]
    gt18_aliases = [
        "% waiting > 18 weeks", "% waiting over 18 weeks", "% > 18 weeks", "over 18 weeks (%)"
    ]
    gt52_aliases = [
        "% waiting > 52 weeks", "% waiting over 52 weeks", "% > 52 weeks", "over 52 weeks (%)"
    ]

    ods_col = _pick_first_present(df, ods_aliases)
    tfn_col = _pick_first_present(df, tfn_aliases)
    per_col = _pick_first_present(df, period_aliases)
    med_col = _pick_first_present(df, median_aliases)
    gt18_col = _pick_first_present(df, gt18_aliases)
    gt52_col = _pick_first_present(df, gt52_aliases)

    # Coerce/clean types
    df[per_col] = pd.to_datetime(df[per_col], errors="coerce")
    df = df.dropna(subset=[per_col])

    records: List[Tuple[str, str, datetime, float, float, float]] = []
    for _, row in df.iterrows():
        ods_code = str(row[ods_col]).strip()
        treatment_code = str(row[tfn_col]).strip()

        # If treatment_code looks like "101 - General Surgery", take the code part
        if "-" in treatment_code and treatment_code.split("-")[0].strip().isdigit():
            treatment_code = treatment_code.split("-")[0].strip()

        period_date = pd.to_datetime(row[per_col]).to_pydatetime()

        def _to_float(v) -> float:
            try:
                return float(v)
            except Exception:
                return float("nan")

        median_weeks = _to_float(row[med_col])
        pct_over_18w = _to_float(row[gt18_col])
        pct_over_52w = _to_float(row[gt52_col])

        if not ods_code or not treatment_code:
            continue

        records.append((ods_code, treatment_code, period_date, median_weeks, pct_over_18w, pct_over_52w))

    logging.info("Parsed %d rows from sheet '%s'", len(records), sheet_name)
    return records

# ------------------------------------------------------------------------------
# Database helpers
# ------------------------------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS wait_metrics (
    ods_code TEXT NOT NULL,
    treatment_code TEXT NOT NULL,
    period_date DATE NOT NULL,
    median_weeks DOUBLE PRECISION NOT NULL,
    pct_over_18w DOUBLE PRECISION NOT NULL,
    pct_over_52w DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (ods_code, treatment_code, period_date)
);
"""


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


def upsert_wait_metrics(records: Iterable[Tuple[str, str, datetime, float, float, float]], database_url: str) -> None:
    recs = list(records)  # materialize once; also allows len()
    logging.info("Upserting %d records into wait_metrics …", len(recs))
    if not recs:
        logging.warning("No records to upsert; skipping.")
        return

    conn = psycopg2.connect(database_url)
    try:
        ensure_table(conn)
        with conn.cursor() as cur:
            insert_sql = """
                INSERT INTO wait_metrics
                (ods_code, treatment_code, period_date, median_weeks, pct_over_18w, pct_over_52w)
                VALUES %s
                ON CONFLICT (ods_code, treatment_code, period_date) DO UPDATE
                SET median_weeks = EXCLUDED.median_weeks,
                    pct_over_18w = EXCLUDED.pct_over_18w,
                    pct_over_52w = EXCLUDED.pct_over_52w;
            """
            execute_values(cur, insert_sql, recs)
        conn.commit()
        logging.info("Upsert completed successfully.")
    finally:
        conn.close()

# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------

def main() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")

    wlmds_url = os.environ.get("WLMDS_URL")
    wlmds_sheet = os.environ.get("WLMDS_SHEET")

    if not wlmds_url or not wlmds_url.strip():
        wlmds_url = discover_latest_wlmds_url()

    logging.info("WLMDS source: %s", wlmds_url)

    tmp_path = download_to_temp(wlmds_url)
    try:
        xls = pd.ExcelFile(tmp_path)
        sheet_name = choose_provider_sheet(xls, wlmds_sheet)
        logging.info("Using sheet: %s (available: %s)", sheet_name, xls.sheet_names)

        records = list(parse_wlmds_excel(tmp_path, sheet_name=sheet_name))
        upsert_wait_metrics(records, database_url)
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


if __name__ == "__main__":
    main()
