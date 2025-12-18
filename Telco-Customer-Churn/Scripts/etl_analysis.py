import os
import pandas as pd
import matplotlib.pyplot as plt
from supabase import create_client, Client
from dotenv import load_dotenv

# ---------------- CONFIG ---------------- #
TABLE_NAME = "churn"
# ---------------------------------------- #

def get_base_dir() -> str:
    """
    Get the project root directory.

    - If running as a .py script (etl_analysis.py in Scripts/), use __file__
      -> base_dir = folder above Scripts/
    - If running inside a Jupyter notebook, __file__ is not defined
      -> fall back to current working directory (os.getcwd())
    """
    try:
        # This works when the code is in a .py file
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        # This runs when __file__ is not defined (e.g., inside Jupyter Notebook)
        return os.getcwd()


def get_supabase_client() -> Client:
    """Create Supabase client from .env."""
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL or SUPABASE_KEY missing in .env")
    return create_client(url, key)


def fetch_all_churn_rows(supabase: Client) -> pd.DataFrame:
    """
    Fetch all rows from Supabase churn table in batches
    (Supabase API returns up to ~1000 rows at a time).
    """
    all_rows = []
    batch_size = 1000
    start = 0

    while True:
        end = start + batch_size - 1  # Supabase range is inclusive
        resp = supabase.table(TABLE_NAME).select("*").range(start, end).execute()

        data_batch = resp.data or []
        if not data_batch:
            break

        all_rows.extend(data_batch)

        if len(data_batch) < batch_size:
            break

        start += batch_size

    if not all_rows:
        raise RuntimeError("No data returned from Supabase table 'churn'.")

    return pd.DataFrame(all_rows)


def to_numeric_safe(series: pd.Series):
    """Convert a series to numeric safely."""
    return pd.to_numeric(series, errors="coerce")


def main():
    # Figure out project root and processed data folder
    base_dir = get_base_dir()
    processed_dir = os.path.join(base_dir, "data", "processed")
    os.makedirs(processed_dir, exist_ok=True)

    # Connect to Supabase
    supabase = get_supabase_client()

    print("📥 Fetching data from Supabase 'churn' table...")
    df = fetch_all_churn_rows(supabase)
    print(f"   Rows fetched: {len(df)}")
    print(f"   Columns: {df.columns.tolist()}")

    # Ensure numeric columns are numeric
    if "MonthlyCharges" in df.columns:
        df["MonthlyCharges"] = to_numeric_safe(df["MonthlyCharges"])
    if "TotalCharges" in df.columns:
        df["TotalCharges"] = to_numeric_safe(df["TotalCharges"])
    if "tenure" in df.columns:
        df["tenure"] = to_numeric_safe(df["tenure"])

    # -----------------------------
    # METRICS
    # -----------------------------
    summary_rows = []

    # 1) Churn percentage
    if "Churn" in df.columns:
        if df["Churn"].dtype == "object":
            churn_numeric = (df["Churn"].astype(str).str.lower() == "yes").astype(int)
        else:
            churn_numeric = to_numeric_safe(df["Churn"]).fillna(0)
        churn_pct = churn_numeric.mean() * 100.0
        summary_rows.append({
            "metric": "churn_percentage",
            "group": "overall",
            "value": round(float(churn_pct), 2)
        })

    # 2) Average monthly charges per contract
    if "Contract" in df.columns and "MonthlyCharges" in df.columns:
        avg_mc_by_contract = df.groupby("Contract")["MonthlyCharges"].mean()
        for contract_type, avg_val in avg_mc_by_contract.items():
            summary_rows.append({
                "metric": "avg_monthly_charges_per_contract",
                "group": str(contract_type),
                "value": round(float(avg_val), 2)
            })

    # 3) Count of New, Regular, Loyal, Champion customers
    if "tenure_group" in df.columns:
        tg_counts = df["tenure_group"].value_counts(dropna=False)
        for tg, cnt in tg_counts.items():
            summary_rows.append({
                "metric": "tenure_group_count",
                "group": str(tg),
                "value": int(cnt)
            })

    # 4) Internet service distribution
    if "InternetService" in df.columns:
        is_counts = df["InternetService"].value_counts(dropna=False)
        for svc, cnt in is_counts.items():
            summary_rows.append({
                "metric": "internet_service_distribution",
                "group": str(svc),
                "value": int(cnt)
            })

    # 5) Pivot: churn rate by tenure_group
    if "tenure_group" in df.columns and "Churn" in df.columns:
        if df["Churn"].dtype == "object":
            churn_numeric = (df["Churn"].astype(str).str.lower() == "yes").astype(int)
        else:
            churn_numeric = to_numeric_safe(df["Churn"]).fillna(0)

        df["_ChurnNumeric"] = churn_numeric
        pivot = df.groupby("tenure_group")["_ChurnNumeric"].mean() * 100.0

        for tg, rate in pivot.items():
            summary_rows.append({
                "metric": "churn_rate_by_tenure_group",
                "group": str(tg),
                "value": round(float(rate), 2)
            })

    # Convert summary rows to DataFrame and save
    summary_df = pd.DataFrame(summary_rows)
    summary_path = os.path.join(processed_dir, "analysis_summary.csv")
    summary_df.to_csv(summary_path, index=False)
    print(f"✅ Analysis summary saved to: {summary_path}")

    # -----------------------------
    # OPTIONAL VISUALIZATIONS
    # -----------------------------

    # 1) Churn rate by Monthly Charge Segment
    if "monthly_charge_segment" in df.columns and "Churn" in df.columns:
        if df["Churn"].dtype == "object":
            churn_numeric = (df["Churn"].astype(str).str.lower() == "yes").astype(int)
        else:
            churn_numeric = to_numeric_safe(df["Churn"]).fillna(0)

        df["_ChurnNumeric"] = churn_numeric
        churn_by_segment = df.groupby("monthly_charge_segment")["_ChurnNumeric"].mean() * 100.0

        plt.figure(figsize=(6, 4))
        churn_by_segment.sort_index().plot(kind="bar")
        plt.ylabel("Churn rate (%)")
        plt.title("Churn Rate by Monthly Charge Segment")
        plt.xticks(rotation=0)
        plt.tight_layout()
        plot_path = os.path.join(processed_dir, "churn_rate_by_monthly_segment.png")
        plt.savefig(plot_path)
        plt.close()
        print(f"📊 Saved plot: {plot_path}")

    # 2) Histogram of TotalCharges
    if "TotalCharges" in df.columns:
        plt.figure(figsize=(6, 4))
        df["TotalCharges"].dropna().plot(kind="hist", bins=30)
        plt.xlabel("Total Charges")
        plt.title("Histogram of Total Charges")
        plt.tight_layout()
        hist_path = os.path.join(processed_dir, "total_charges_histogram.png")
        plt.savefig(hist_path)
        plt.close()
        print(f"📊 Saved plot: {hist_path}")

    # 3) Bar plot of Contract types
    if "Contract" in df.columns:
        plt.figure(figsize=(6, 4))
        df["Contract"].value_counts().plot(kind="bar")
        plt.ylabel("Count")
        plt.title("Contract Type Counts")
        plt.xticks(rotation=15)
        plt.tight_layout()
        contract_plot_path = os.path.join(processed_dir, "contract_type_counts.png")
        plt.savefig(contract_plot_path)
        plt.close()
        print(f"📊 Saved plot: {contract_plot_path}")

    print("✅ ETL Analysis complete.")


if __name__ == "__main__":
    main()