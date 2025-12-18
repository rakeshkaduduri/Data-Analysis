import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project root (folder above Scripts/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Path to transformed CSV
STAGED_CSV = os.path.join(BASE_DIR, "data", "staged", "churn_transformed.csv")

# Supabase credentials from .env
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL or SUPABASE_KEY not found in .env")

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "churn"

COLUMNS = [
    "tenure",
    "MonthlyCharges",
    "TotalCharges",
    "Churn",
    "InternetService",
    "Contract",
    "PaymentMethod",
    "tenure_group",
    "monthly_charge_segment",
    "has_internet_service",
    "is_multi_line_user",
    "contract_type_code",
]


def load_data():
    if not os.path.exists(STAGED_CSV):
        raise FileNotFoundError(f"Transformed CSV not found at: {STAGED_CSV}. Run transform.py first.")

    df = pd.read_csv(STAGED_CSV)

    # Only keep needed columns in correct order
    df = df[COLUMNS]

    # NaN -> None (so Supabase stores NULL)
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")

    print(f"📤 Uploading {len(records)} rows to Supabase table '{TABLE_NAME}'...")

    batch_size = 200
    for start in range(0, len(records), batch_size):
        batch = records[start:start + batch_size]

        # Call Supabase insert, ignore .error (new client doesn't expose it)
        response = supabase.table(TABLE_NAME).insert(batch).execute()

        # Optionally inspect response.data
        print(f"✅ Inserted {len(batch)} rows (up to row {start + len(batch)})")

    print("✅ Done inserting all rows!")


if __name__ == "__main__":
    load_data()

# SQL to create the 'churn' table in Supabase
"""
create table if not exists public.churn (
  id bigserial primary key,
  tenure integer,
  "MonthlyCharges" double precision,
  "TotalCharges" double precision,
  "Churn" text,
  "InternetService" text,
  "Contract" text,
  "PaymentMethod" text,
  tenure_group text,
  monthly_charge_segment text,
  has_internet_service integer,
  is_multi_line_user integer,
  contract_type_code integer
);

"""
