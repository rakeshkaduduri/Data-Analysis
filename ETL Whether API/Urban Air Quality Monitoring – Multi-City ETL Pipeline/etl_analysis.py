import os
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from supabase import create_client, Client  

load_dotenv()

# ---------------- Config ----------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY")
TABLE_NAME = os.getenv("AQ_TABLE_NAME", "air_quality_data")

BASE_DIR = Path(__file__).resolve().parents[0]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PLOTS_DIR = BASE_DIR / "data" / "plots"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


# ------------- Supabase Helpers -------------
def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL and SUPABASE_*KEY environment variables must be set")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_all_data(client: Client) -> pd.DataFrame:
    """
    Read full air_quality_data table from Supabase into a DataFrame.
    Uses paginated select in chunks of 1000.
    """
    all_rows = []
    limit = 1000
    offset = 0

    print("📥 Fetching data from Supabase...")
    while True:
        resp = (
            client.table(TABLE_NAME)
            .select("*")
            .range(offset, offset + limit - 1)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            break
        all_rows.extend(rows)
        print(f"  Retrieved {len(rows)} rows (total so far: {len(all_rows)})")
        if len(rows) < limit:
            break
        offset += limit

    if not all_rows:
        raise ValueError("No data returned from Supabase")

    df = pd.DataFrame(all_rows)
    # Convert time to datetime
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return df


# ------------- KPI Metrics -------------
def compute_kpi_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute high-level KPIs:
      - City with highest avg PM2.5
      - City with highest avg severity_score
      - Percent of hours in each risk_flag
      - Hour of day with worst AQI (highest avg PM2.5)
    """
    metrics = {}

    # City with highest average PM2.5
    pm25_city = (
        df.groupby("city", dropna=True)["pm2_5"]
        .mean()
        .sort_values(ascending=False)
    )
    if not pm25_city.empty:
        metrics["city_highest_avg_pm25"] = pm25_city.index[0]
        metrics["highest_avg_pm25_value"] = pm25_city.iloc[0]

    # City with highest average severity_score
    if "severity_score" in df.columns:
        sev_city = (
            df.groupby("city", dropna=True)["severity_score"]
            .mean()
            .sort_values(ascending=False)
        )
        if not sev_city.empty:
            metrics["city_highest_avg_severity"] = sev_city.index[0]
            metrics["highest_avg_severity_value"] = sev_city.iloc[0]

    # Percentage of High/Moderate/Low risk hours
    if "risk_flag" in df.columns:
        risk_counts = df["risk_flag"].value_counts(dropna=True)
        total = risk_counts.sum()
        for risk, cnt in risk_counts.items():
            metrics[f"pct_{risk.replace(' ', '_').lower()}"] = (cnt / total) * 100 if total else 0

    # Hour of day with worst AQI (based on PM2.5)
    if "hour" in df.columns:
        hour_pm25 = (
            df.groupby("hour", dropna=True)["pm2_5"]
            .mean()
            .sort_values(ascending=False)
        )
        if not hour_pm25.empty:
            metrics["worst_aqi_hour"] = int(hour_pm25.index[0])
            metrics["worst_aqi_hour_avg_pm25"] = hour_pm25.iloc[0]

    summary_df = pd.DataFrame([metrics])
    return summary_df


def compute_city_risk_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Risk distribution (count and percentage) per city."""
    if "risk_flag" not in df.columns:
        raise ValueError("risk_flag column missing in dataframe")

    counts = df.groupby(["city", "risk_flag"]).size().reset_index(name="count")
    total_per_city = counts.groupby("city")["count"].transform("sum")
    counts["percentage"] = (counts["count"] / total_per_city) * 100
    return counts


def compute_pollution_trends(df: pd.DataFrame) -> pd.DataFrame:
    """
    City Pollution Trend Report:
    For each city and time, keep pm2_5, pm10, ozone.
    """
    cols = ["city", "time", "pm2_5", "pm10", "ozone"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for pollution trends: {missing}")
    trends = df[cols].dropna(subset=["time"])
    return trends.sort_values(["city", "time"]).reset_index(drop=True)


# ------------- Visualizations -------------
def plot_hist_pm25(df: pd.DataFrame, path: Path):
    plt.figure(figsize=(8, 5))
    plt.hist(df["pm2_5"].dropna(), bins=40, color="steelblue", edgecolor="black")
    plt.title("Histogram of PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    print(f"📊 Saved PM2.5 histogram: {path}")


def plot_risk_flags_per_city(df: pd.DataFrame, path: Path):
    if "risk_flag" not in df.columns:
        print("risk_flag column missing; skipping risk flags bar chart")
        return
    pivot = (
        df.pivot_table(
            index="city",
            columns="risk_flag",
            values="id",  # any column to count
            aggfunc="count",
            fill_value=0,
        )
    )
    pivot.plot(kind="bar", figsize=(10, 6))
    plt.title("Risk Flags per City")
    plt.xlabel("City")
    plt.ylabel("Number of Hours")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    print(f"📊 Saved risk flags per city bar chart: {path}")


def plot_hourly_pm25_trends(df: pd.DataFrame, path: Path):
    plt.figure(figsize=(10, 6))
    # Plot one line per city
    for city, grp in df.groupby("city"):
        # Sort by time
        grp_sorted = grp.sort_values("time")
        plt.plot(grp_sorted["time"], grp_sorted["pm2_5"], label=city)
    plt.title("Hourly PM2.5 Trends")
    plt.xlabel("Time")
    plt.ylabel("PM2.5")
    plt.legend()
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    print(f"📊 Saved hourly PM2.5 trends line chart: {path}")


def plot_severity_vs_pm25(df: pd.DataFrame, path: Path):
    if "severity_score" not in df.columns:
        print("severity_score column missing; skipping scatter plot")
        return
    plt.figure(figsize=(8, 5))
    plt.scatter(df["pm2_5"], df["severity_score"], alpha=0.5)
    plt.title("Severity Score vs PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Severity Score")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    print(f"📊 Saved scatter plot (severity vs PM2.5): {path}")


# ------------- Main ETL Analysis -------------
def run_analysis():
    client = get_supabase_client()
    df = fetch_all_data(client)
    print(f"✅ Loaded {len(df)} records from Supabase")

    # Ensure proper numeric dtypes
    num_cols = [
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
        "uv_index",
        "severity_score",
        "hour",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # A. KPI Metrics
    summary_metrics = compute_kpi_metrics(df)
    summary_path = PROCESSED_DIR / "summary_metrics.csv"
    summary_metrics.to_csv(summary_path, index=False)
    print(f"💾 Saved summary metrics to {summary_path}")

    # City risk distribution
    city_risk_dist = compute_city_risk_distribution(df)
    risk_path = PROCESSED_DIR / "city_risk_distribution.csv"
    city_risk_dist.to_csv(risk_path, index=False)
    print(f"💾 Saved city risk distribution to {risk_path}")

    # B. City Pollution Trend Report
    pollution_trends = compute_pollution_trends(df)
    trends_path = PROCESSED_DIR / "pollution_trends.csv"
    pollution_trends.to_csv(trends_path, index=False)
    print(f"💾 Saved pollution trends to {trends_path}")

    # D. Visualizations
    plot_hist_pm25(df, PLOTS_DIR / "hist_pm25.png")
    plot_risk_flags_per_city(df, PLOTS_DIR / "risk_flags_per_city.png")
    plot_hourly_pm25_trends(pollution_trends, PLOTS_DIR / "hourly_pm25_trends.png")
    plot_severity_vs_pm25(df, PLOTS_DIR / "severity_vs_pm25.png")

    print("\n✅ Analysis complete. Processed CSVs and plots are ready.")


if __name__ == "__main__":
    run_analysis()
