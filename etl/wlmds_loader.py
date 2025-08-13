"""
ETL script for loading Waiting List Minimum Dataset (WLMDS) data into the
    database.  This script downloads the latest WLMDS summary file from NHS England,
    parses the provider‑level wait time data and upserts it into the `wait_metrics`
    table used by the Waitlist Radar service.

    Due to network restrictions, this script defaults to a placeholder URL for the
    WLMDS Excel file.  Before running this script in production, set the
    ``WLMDS_URL`` environment variable to point at the latest WLMDS release (for
    example, ``https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2025/04/WLMDS-Summary-to-30-Mar-2025.xlsx``).

    Usage:
        python -m etl.wlmds_loader

    Environment variables required:
        - DATABASE_URL: Postgres connection string (e.g. ``postgresql://user:pass@host:port/db``)
        - WLMDS_URL (optional): URL of the WLMDS Excel file.
        - WLMDS_SHEET (optional): Name of the worksheet containing provider‑level
          data.  Defaults to ``Provider breakdown``.
"""

import os
import logging
import tempfile
from datetime import datetime
from typing import Iterable, Tuple

import pandas as pd  # type: ignore[import]
import requests  # type: ignore[import]
import psycopg2  # type: ignore[import]
from psycopg2.extras import execute_values  # type: ignore[import]


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")



def download_wlmds(url: str) -> str:
    """Download the WLMDS Excel file from the given URL into a temporary file.

    Returns the path to the downloaded file.
    """
    logging.info("Downloading WLMDS data from %s", url)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    # Write to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
        tmp_file.write(response.content)
        tmp_path = tmp_file.name
    logging.info("Downloaded WLMDS file to %s", tmp_path)
    return tmp_path


def parse_wlmds_excel(path: str, sheet_name: str = "Provider breakdown") -> Iterable[Tuple[str, str, datetime, float, float, float]]:
    """Parse a WLMDS Excel file into a sequence of wait_metrics records.

    Each yielded tuple has the form::

        (ods_code, treatment_code, period_date, median_weeks, pct_over_18w, pct_over_52w)

    ``sheet_name`` should correspond to the worksheet containing provider‑level
    metrics.  If the sheet structure changes in future releases, adjust the
    column names accordingly.
    """
    logging.info("Parsing WLMDS Excel file %s (sheet: %s)", path, sheet_name)
    # Load the worksheet using pandas.  We let pandas infer the header row.
    df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")  # type: ignore
    # Normalise column names to lowercase and strip whitespace
    df.columns = [str(c).strip().lower() for c in df.columns]
    # Define expected column names.  These may need to be updated if NHS
    # changes the WLMDS format.
    # Example expected columns:
    #   organisation code, treatment function code, period ending (date),
    #   median wait (weeks), pct > 18 weeks, pct > 52 weeks
    required_cols = {
        "organisation code": "ods_code",
        "treatment function code": "treatment_code",
        "period ending": "period_date",
        "median wait": "median_weeks",
        "% waiting > 18 weeks": "pct_over_18w",
        "% waiting > 52 weeks": "pct_over_52w",
    }
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in WLMDS sheet: {missing}")
    # Rename columns to our schema
    df = df.rename(columns=required_cols)
    # Parse date column
    df["period_date"] = pd.to_datetime(df["period_date"])
    # Build records
    records: list[Tuple[str, str, datetime, float, float, float]] = []
    for _, row in df.iterrows():
        ods_code = str(row["ods_code"]).strip()
        treatment_code = str(row["treatment_code"]).strip()
        period_date = row["period_date"].to_pydatetime()
        median_weeks = float(row["median_weeks"])
        pct_over_18w = float(row["pct_over_18w"])
        pct_over_52w = float(row["pct_over_52w"])
        records.append((ods_code, treatment_code, period_date, median_weeks, pct_over_18w, pct_over_52w))
    logging.info("Parsed %d records from WLMDS file", len(records))
    return records


def upsert_wait_metrics(records: Iterable[Tuple[str, str, datetime, float, float, float]], database_url: str) -> None:
    """Upsert a sequence of wait_metrics records into the database.

    If a record with the same (ods_code, treatment_code, period_date) already
    exists, it will be updated with the new values.
    """
    logging.info("Inserting %d records into wait_metrics table", len(list(records)))
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            insert_sql = """
                INSERT INTO wait_metrics (ods_code, treatment_code, period_date, median_weeks, pct_over_18w, pct_over_52w)
                VALUES %s
                ON CONFLICT (ods_code, treatment_code, period_date) DO UPDATE
                SET median_weeks = EXCLUDED.median_weeks,
                    pct_over_18w = EXCLUDED.pct_over_18w,
                    pct_over_52w = EXCLUDED.pct_over_52w;
            """
            execute_values(cur, insert_sql, records)
        conn.commit()
        logging.info("Upsert completed successfully")
    finally:
        conn.close()


def main() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable must be set")
    wlmds_url = os.environ.get(
        "WLMDS_URL",
        # Default placeholder – replace with the latest WLMDS summary file URL
        "https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2025/04/WLMDS-Summary-to-30-Mar-2025.xlsx",
    )
    sheet_name = os.environ.get("WLMDS_SHEET", "Provider breakdown")
    # Download
    tmp_path = download_wlmds(wlmds_url)
    try:
        # Parse records
        records = list(parse_wlmds_excel(tmp_path, sheet_name=sheet_name))
        if not records:
            logging.warning("No records parsed from WLMDS file – nothing to insert")
            return
        # Upsert into database
        upsert_wait_metrics(records, database_url)
    finally:
        # Clean up temporary file
        try:
            os.remove(tmp_path)
        except OSError:
            pass


if __name__ == "__main__":
    main()
