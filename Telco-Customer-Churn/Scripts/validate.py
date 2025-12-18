import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

TABLE_NAME = "churn"

EXPECTED_TENURE_GROUPS = {"New", "Regular", "Loyal", "Champion"}
EXPECTED_CHARGE_SEGMENTS = {"Low", "Medium", "High"}
EXPECTED_CONTRACT_CODES = {0, 1, 2}


def get_base_dir() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_supabase_client() -> Client:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL or SUPABASE_KEY missing in .env")
    return create_client(url, key)


def validate():
    base_dir = get_base_dir()

    raw_path = os.path.join(base_dir, "data", "raw", "Telco-Customer-Churn.csv") 
    staged_path = os.path.join(base_dir, "data", "staged", "churn_transformed.csv")

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw CSV not found at: {raw_path}")
    if not os.path.exists(staged_path):
        raise FileNotFoundError(f"Transformed CSV not found at: {staged_path}")

    print("🔍 Loading local datasets...")
    df_raw = pd.read_csv(raw_path)
    df = pd.read_csv(staged_path)

    # 1) Missing values
    print("\n✅ 1) Checking missing values in key numeric columns...")
    missing_results = {}
    for col in ["tenure", "MonthlyCharges", "TotalCharges"]:
        if col in df.columns:
            n_missing = int(df[col].isna().sum())
            missing_results[col] = n_missing
        else:
            missing_results[col] = "COLUMN NOT FOUND"

    for col, val in missing_results.items():
        print(f"   - {col}: {val} missing")

    no_missing_numeric = all((isinstance(v, int) and v == 0) for v in missing_results.values())

    # 2) Row counts (simplified: just compare number of rows)
    print("\n✅ 2) Checking row counts...")
    raw_rows = len(df_raw)
    staged_rows = len(df)
    print(f"   - Raw rows:    {raw_rows}")
    print(f"   - Staged rows: {staged_rows}")

    row_count_same = (raw_rows == staged_rows)

    # 3) Supabase row count using exact count
    print("\n✅ 3) Checking Supabase table row count (exact)...")
    supabase = get_supabase_client()
    response = supabase.table(TABLE_NAME).select("id", count="exact").execute()

    db_rows = response.count if hasattr(response, "count") else len(response.data)
    print(f"   - Supabase table '{TABLE_NAME}' rows: {db_rows}")

    row_count_matches_db = (db_rows == staged_rows)

    # 4) Segment coverage
    print("\n✅ 4) Checking segment coverage...")
    tenure_groups_in_data = set(df["tenure_group"].dropna().astype(str).unique()) if "tenure_group" in df.columns else set()
    charge_segments_in_data = set(df["monthly_charge_segment"].dropna().astype(str).unique()) if "monthly_charge_segment" in df.columns else set()

    print(f"   - tenure_group values:          {tenure_groups_in_data}")
    print(f"   - expected tenure_group values: {EXPECTED_TENURE_GROUPS}")
    print(f"   - monthly_charge_segment values:          {charge_segments_in_data}")
    print(f"   - expected monthly_charge_segment values: {EXPECTED_CHARGE_SEGMENTS}")

    all_tenure_groups_present = EXPECTED_TENURE_GROUPS.issubset(tenure_groups_in_data)
    all_charge_segments_present = EXPECTED_CHARGE_SEGMENTS.issubset(charge_segments_in_data)

    # 5) Contract code check
    print("\n✅ 5) Checking contract_type_code values...")
    if "contract_type_code" in df.columns:
        contract_codes = set(int(x) for x in df["contract_type_code"].dropna().unique())
    else:
        contract_codes = set()

    print(f"   - contract_type_code values: {contract_codes}")
    print(f"   - expected values only:      {EXPECTED_CONTRACT_CODES}")

    contract_codes_ok = contract_codes.issubset(EXPECTED_CONTRACT_CODES)

    # SUMMARY
    print("\n📋 VALIDATION SUMMARY")
    print("----------------------")
    print(f"1) No missing in tenure/MonthlyCharges/TotalCharges: {no_missing_numeric}")
    print(f"2) Row count (raw vs staged) same:                  {row_count_same}")
    print(f"3) Row count matches Supabase table:                {row_count_matches_db}")
    print(f"4) All tenure_group segments present:               {all_tenure_groups_present}")
    print(f"5) All monthly_charge_segment values present:       {all_charge_segments_present}")
    print(f"6) contract_type_code only in {EXPECTED_CONTRACT_CODES}: {contract_codes_ok}")

    all_ok = (
        no_missing_numeric
        and row_count_same
        and row_count_matches_db
        and all_tenure_groups_present
        and all_charge_segments_present
        and contract_codes_ok
    )
    
    return all_ok


if __name__ == "__main__":
    validate()