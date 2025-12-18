import os
from pathlib import Path
from typing import List, Dict, Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client  # pip install supabase

load_dotenv()

# ---- Config ----
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = os.getenv("AQ_TABLE_NAME", "air_quality_data")

BATCH_SIZE = int(os.getenv("AQ_BATCH_SIZE", "200"))
MAX_RETRIES = int(os.getenv("AQ_MAX_RETRIES", "2"))

BASE_DIR = Path(__file__).resolve().parents[0]
STAGED_DIR = BASE_DIR / "data" / "staged"


def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL and SUPABASE_*KEY environment variables must be set")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _latest_staged_file() -> Path:
    """Pick latest transformed CSV/parquet from staged dir."""
    candidates = list(STAGED_DIR.glob("air_quality_transformed_*.csv")) + \
                 list(STAGED_DIR.glob("air_quality_transformed_*.parquet"))
    if not candidates:
        raise FileNotFoundError(f"No transformed files found in {STAGED_DIR}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _load_dataframe(path: Path) -> pd.DataFrame:
    if path.suffix == ".csv":
        return pd.read_csv(path)
    elif path.suffix == ".parquet":
        return pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")


def _prepare_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert DataFrame to list[dict] with ISO time and NaN→None."""
    # Column name mapping between transform and DB schema
    col_map = {
        "city": "city",
        "time": "time",
        "pm10": "pm10",
        "pm2_5": "pm2_5",
        "carbon_monoxide": "carbon_monoxide",
        "nitrogen_dioxide": "nitrogen_dioxide",
        "sulphur_dioxide": "sulphur_dioxide",
        "ozone": "ozone",
        "uv_index": "uv_index",
        "aqi_category": "aqi_category",
        "pollution_severity": "severity_score",
        "risk_classification": "risk_flag",
        "hour_of_day": "hour",
    }

    # Ensure datetime
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        rec: Dict[str, Any] = {}
        for src, dst in col_map.items():
            value = row.get(src)

            # Convert pandas Timestamp to ISO string
            if src == "time" and pd.notna(value):
                value = value.to_pydatetime().isoformat()

            # Convert NaN/NaT to None (will become NULL in DB)
            if isinstance(value, (float, np.floating)) and np.isnan(value):
                value = None
            if pd.isna(value):
                value = None

            rec[dst] = value
        records.append(rec)
    return records


def _insert_batch(client: Client, batch: List[Dict[str, Any]]) -> int:
    """Insert a single batch with retries; return number of rows inserted."""
    attempts = 0
    last_err = None

    while attempts <= MAX_RETRIES:
        try:
            resp = client.table(TABLE_NAME).insert(batch).execute()
            # When using 'minimal' preference there is no data returned; count by len(batch)
            return len(batch)
        except Exception as e:  # supabase-py raises generic exceptions
            last_err = e
            attempts += 1
            print(f"⚠️  Batch insert failed (attempt {attempts}/{MAX_RETRIES + 1}): {e}")
            if attempts > MAX_RETRIES:
                print("❌ Giving up on this batch.")
                break
    return 0


def load_to_supabase():
    client = get_supabase_client()

    path = _latest_staged_file()
    print(f"Using transformed file: {path}")

    df = _load_dataframe(path)
    print(f"Loaded {len(df)} rows from staged file")

    records = _prepare_records(df)
    total = len(records)
    print(f"Prepared {total} records for insertion (batch size = {BATCH_SIZE})")

    inserted = 0
    failed_batches = 0

    for i in range(0, total, BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        print(f"👉 Inserting batch {i // BATCH_SIZE + 1} ({len(batch)} rows)...")
        before = inserted
        added = _insert_batch(client, batch)
        inserted += added
        if added == 0:
            failed_batches += 1

    print("\n===== Load Summary =====")
    print(f"Total records prepared: {total}")
    print(f"Total records inserted: {inserted}")
    print(f"Failed batches: {failed_batches}")
    print("========================")


if __name__ == "__main__":
    print("🚀 Starting Supabase load for air_quality_data")
    load_to_supabase()
    print("✅ Load finished")