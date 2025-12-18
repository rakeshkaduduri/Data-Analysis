import json
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import glob

STAGED_DIR = Path(__file__).resolve().parents[0] / "data" / "staged"
STAGED_DIR.mkdir(parents=True, exist_ok=True)

RAW_DIR = Path(__file__).resolve().parents[0] / "data" / "raw"

def aqi_from_pm25(pm25: float) -> str:
    """Convert PM2.5 to AQI category."""
    if pd.isna(pm25):
        return "Unknown"
    if pm25 <= 50:
        return "Good"
    elif pm25 <= 100:
        return "Moderate"
    elif pm25 <= 200:
        return "Unhealthy"
    elif pm25 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

def pollution_severity(pm25: float, pm10: float, no2: float, so2: float, 
                      co: float, o3: float) -> float:
    """Calculate weighted pollution severity score."""
    return (pm25 * 5) + (pm10 * 3) + (no2 * 4) + (so2 * 4) + (co * 2) + (o3 * 3)

def risk_classification(severity: float) -> str:
    """Classify risk based on severity score."""
    if pd.isna(severity):
        return "Unknown"
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"

def transform_single_file(raw_path: str, city: str) -> pd.DataFrame:
    """Transform single raw JSON file to tabular format."""
    with open(raw_path, 'r') as f:
        data = json.load(f)
    
    # Extract hourly data
    hourly = data.get('hourly', {})
    time_data = hourly.get('time', [])
    
    # Map pollutant keys (handles both snake_case and underscore variants)
    pollutants = {
        'pm10': hourly.get('pm10', []),
        'pm2_5': hourly.get('pm2_5', []) or hourly.get('pm25', []),
        'carbon_monoxide': hourly.get('carbon_monoxide', []) or hourly.get('co', []),
        'nitrogen_dioxide': hourly.get('nitrogen_dioxide', []) or hourly.get('no2', []),
        'sulphur_dioxide': hourly.get('sulphur_dioxide', []) or hourly.get('so2', []),
        'ozone': hourly.get('ozone', []) or hourly.get('o3', []),
        'uv_index': hourly.get('uv_index', []) or hourly.get('uvi', [])
    }
    
    # Create DataFrame
    df = pd.DataFrame({
        'city': city,
        'time': pd.to_datetime(time_data)
    })
    
    # Add pollutant columns
    for key, values in pollutants.items():
        if len(values) == len(time_data):
            df[key] = pd.to_numeric(values, errors='coerce')
        else:
            df[key] = pd.NA
    
    # Feature Engineering
    df['aqi_category'] = df['pm2_5'].apply(aqi_from_pm25)
    df['pollution_severity'] = df.apply(
        lambda row: pollution_severity(
            row['pm2_5'], row['pm10'], row['nitrogen_dioxide'], 
            row['sulphur_dioxide'], row['carbon_monoxide'], row['ozone']
        ), axis=1
    )
    df['risk_classification'] = df['pollution_severity'].apply(risk_classification)
    df['hour_of_day'] = df['time'].dt.hour
    
    # Filter out records where all pollutants are missing
    pollutant_cols = ['pm10', 'pm2_5', 'carbon_monoxide', 'nitrogen_dioxide', 
                     'sulphur_dioxide', 'ozone']
    df = df.dropna(subset=pollutant_cols, how='all')
    
    return df

def process_all_raw_files() -> pd.DataFrame:
    """Process all raw JSON files in RAW_DIR."""
    all_dfs = []
    
    # Find all raw JSON files
    raw_files = list(RAW_DIR.glob("*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No raw JSON files found in {RAW_DIR}")
    
    print(f"Found {len(raw_files)} raw files to process")
    
    for raw_path in raw_files:
        # Extract city from filename (e.g., delhi_raw_20231211T1015Z.json)
        filename = raw_path.stem
        if 'raw' in filename:
            city = filename.split('raw')[0].replace('_', ' ').title()
        else:
            city = "Unknown"
        
        print(f"Processing {raw_path.name} -> {city}")
        
        try:
            df = transform_single_file(str(raw_path), city)
            if not df.empty:
                all_dfs.append(df)
                print(f"  ✅ Added {len(df)} records for {city}")
            else:
                print(f"  ⚠️ No valid records for {city}")
        except Exception as e:
            print(f"  ❌ Error processing {raw_path.name}: {e}")
    
    if not all_dfs:
        raise ValueError("No valid data found in raw files")
    
    # Concatenate all city dataframes
    final_df = pd.concat(all_dfs, ignore_index=True)
    
    # Sort by city and time
    final_df = final_df.sort_values(['city', 'time']).reset_index(drop=True)
    
    return final_df

def save_transformed(df: pd.DataFrame, output_dir: Path = STAGED_DIR):
    """Save transformed data with metadata."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"air_quality_transformed_{timestamp}.parquet"
    
    # Save as Parquet (efficient, preserves dtypes)
    df.to_parquet(output_path, index=False)
    
    # Also save CSV for compatibility
    csv_path = output_dir / f"air_quality_transformed_{timestamp}.csv"
    df.to_csv(csv_path, index=False)
    
    print(f"✅ Saved {len(df)} records:")
    print(f"   Parquet: {output_path}")
    print(f"   CSV: {csv_path}")
    print(f"   Columns: {list(df.columns)}")
    print(f"   Cities: {df['city'].nunique()} | Time range: {df['time'].min()} to {df['time'].max()}")
    
    return str(output_path), str(csv_path)

if __name__ == "__main__":
    print("🚀 Starting Air Quality Transform Pipeline")
    print(f"Raw data dir: {RAW_DIR}")
    print(f"Staged output: {STAGED_DIR}")
    
    df_transformed = process_all_raw_files()
    parquet_path, csv_path = save_transformed(df_transformed)
    
    print("\n📊 Sample data:")
    print(df_transformed.head())
    print(f"\n✅ Transform complete! Files ready for load stage.")