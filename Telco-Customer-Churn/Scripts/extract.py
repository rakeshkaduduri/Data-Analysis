# extract.py
import os
import pandas as pd

def extract_data(filepath):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    data_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(data_dir, exist_ok=True)

    df = pd.read_csv(filepath)

    raw_path = os.path.join(data_dir, "Telco-Customer-Churn.csv")
    df.to_csv(raw_path, index=False)

    print(f"✅ Data extracted and saved at: {raw_path}")
    return raw_path


if __name__ == "__main__":
    input_file = r"C:\Users\rakes\Downloads\Telco-Customer-Churn.csv"
    extract_data(input_file)