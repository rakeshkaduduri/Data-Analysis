import os
import pandas as pd
import numpy as np


def transform_data(raw_path: str) -> str:
    # Get project root: one level above Scripts/
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    # 1) Read raw CSV
    df = pd.read_csv(raw_path)

    # -------------------------
    # CLEANING
    # -------------------------

    # Convert TotalCharges to numeric (spaces / bad values -> NaN)
    if "TotalCharges" in df.columns:
        df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")
        totalcharges_median = df["TotalCharges"].median()
        df["TotalCharges"] = df["TotalCharges"].fillna(totalcharges_median)

    # Ensure tenure and MonthlyCharges are numeric and fill with median
    for col in ["tenure", "MonthlyCharges"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)

    # Replace missing categorical values with "Unknown"
    cat_cols = df.select_dtypes(include="object").columns
    for col in cat_cols:
        df[col] = df[col].replace("", np.nan)
        df[col] = df[col].fillna("Unknown")

    # -------------------------
    # FEATURE ENGINEERING
    # -------------------------

    # 1. tenure_group
    if "tenure" in df.columns:
        df["tenure_group"] = pd.cut(
            x=df["tenure"],
            bins=[0, 12, 36, 60, np.inf],
            labels=["New", "Regular", "Loyal", "Champion"],
            right=False,
        )

    # 2. monthly_charge_segment
    if "MonthlyCharges" in df.columns:
        df["monthly_charge_segment"] = pd.cut(
            x=df["MonthlyCharges"],
            bins=[0, 30, 70, np.inf],
            labels=["Low", "Medium", "High"],
            right=False,
        )

    # 3. has_internet_service: DSL/Fiber optic -> 1, No -> 0
    if "InternetService" in df.columns:
        df["has_internet_service"] = df["InternetService"].map(
            {
                "No": 0,
                "DSL": 1,
                "Fiber optic": 1,
            }
        )
        df["has_internet_service"] = df["has_internet_service"].fillna(0).astype(int)

    # 4. is_multi_line_user: 1 if MultipleLines == "Yes", else 0
    if "MultipleLines" in df.columns:
        df["is_multi_line_user"] = (df["MultipleLines"] == "Yes").astype(int)

    # 5. contract_type_code
    if "Contract" in df.columns:
        df["contract_type_code"] = df["Contract"].map(
            {
                "Month-to-month": 0,
                "One year": 1,
                "Two year": 2,
            }
        )
        df["contract_type_code"] = df["contract_type_code"].fillna(-1).astype(int)

    # -------------------------
    # DROP UNNECESSARY FIELDS
    # -------------------------
    df = df.drop(columns=["customerID", "gender"], errors="ignore")

    # -------------------------
    # SAVE OUTPUT
    # -------------------------
    staged_path = os.path.join(staged_dir, "churn_transformed.csv")
    df.to_csv(staged_path, index=False)
    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path


if __name__ == "__main__":
    from extract import extract_data

    raw_path = extract_data(r"C:\Users\rakes\Downloads\Telco-Customer-Churn.csv")
    transform_data(raw_path)