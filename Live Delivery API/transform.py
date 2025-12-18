# transform.py
"""
Transform step for SwiftShip ETL.

Reads raw JSON files produced by extract.py, cleans and enriches deliveries data,
merges with route traffic snapshots, computes features and scores, and writes
a staged CSV into data/staged/ (timestamped).

Usage:
    python transform.py
    python transform.py --live-file path/to/deliveries_raw_....json --traffic-file path/to/traffic_routes_raw_....json
    python -c "from transform import transform; df=transform(); print(len(df))"

Environment:
    RAW_DIR (optional)         -> path where extract saved raw files (default: ./data/raw)
    STAGED_DIR (optional)      -> path to write staged CSV (default: ./data/staged)
    TIMEZONE (optional)        -> timezone name for display (not required for parsing)
"""
from __future__ import annotations

import json
import math
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
import logging

load_dotenv()

# Directories (configurable via env)
BASE_DIR = Path(__file__).resolve().parents[0]
RAW_DIR = Path(os.getenv("RAW_DIR", BASE_DIR / "data" / "raw"))
STAGED_DIR = Path(os.getenv("STAGED_DIR", BASE_DIR / "data" / "staged"))
STAGED_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = Path(os.getenv("LOG_DIR", BASE_DIR / "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Logging
logger = logging.getLogger("transform")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
fh = logging.FileHandler(LOG_DIR / "transform.log")
fh.setFormatter(fmt)
fh.setLevel(logging.INFO)
logger.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
logger.addHandler(ch)

# Other config
TIMEZONE = os.getenv("TIMEZONE", "")  # optional; not required for parsing but for info
# Accept file name prefixes used by extractor (flexible)
DELIVERIES_PREFIXES = ["deliveries", "live_deliveries", "live"]
TRAFFIC_PREFIXES = ["traffic_routes", "route_traffic", "traffic"]

# ----------------------
# Utility helper funcs
# ----------------------
def find_latest_raw_file(prefixes: List[str], raw_dir: Path = RAW_DIR) -> Optional[Path]:
    """Find the most recent raw file in RAW_DIR matching any of the prefixes."""
    candidates: List[Path] = []
    for p in prefixes:
        candidates.extend(sorted(raw_dir.glob(f"{p}_raw_*.json")))
        candidates.extend(sorted(raw_dir.glob(f"{p}_*.json")))
    if not candidates:
        return None
    return sorted(candidates)[-1]


def load_json_payload(path: Path) -> Any:
    """Load JSON from file and return parsed object."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.exception("Failed to read JSON from %s: %s", path, e)
        raise


def _now_ts() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")


# ----------------------
# Parsing & normalization
# ----------------------
def parse_timestamp(x: Any) -> Optional[pd.Timestamp]:
    if x is None:
        return None
    try:
        return pd.to_datetime(x, utc=True)
    except Exception:
        try:
            s = str(x)
            # common fallback formats
            for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%SZ"):
                try:
                    return pd.to_datetime(s, format=fmt, utc=True)
                except Exception:
                    pass
            return pd.to_datetime(s, utc=True)
        except Exception:
            return None


def normalize_weight_to_kg(w: Any) -> Optional[float]:
    """
    Heuristic to normalize weight to kg:
      - "500g" => 0.5
      - "1.2 kg" => 1.2
      - numeric <= 100 -> treat as kg
      - numeric > 100 and <= 1000 -> treat as grams
      - 'lb' -> convert
    """
    if w is None:
        return None
    # numeric
    try:
        if isinstance(w, (int, float)):
            val = float(w)
            if val <= 100:
                return val
            if val <= 1000:
                return val / 1000.0
            return val / 1000.0
    except Exception:
        pass

    s = str(w).strip().lower().replace(",", "")
    # kg
    m = re.search(r"([\d\.]+)\s*kg\b", s)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            pass
    # grams
    m = re.search(r"([\d\.]+)\s*g\b", s)
    if m:
        try:
            return float(m.group(1)) / 1000.0
        except Exception:
            pass
    # pounds
    m = re.search(r"([\d\.]+)\s*lb", s)
    if m:
        try:
            return float(m.group(1)) * 0.453592
        except Exception:
            pass
    # plain number fallback
    try:
        val = float(s)
        if val <= 100:
            return val
        if val <= 1000:
            return val / 1000.0
        return val / 1000.0
    except Exception:
        return None


def classify_delay(delay_min: float) -> str:
    if delay_min <= 0:
        return "On-Time"
    if 0 < delay_min <= 60:
        return "Slight Delay"
    if 60 < delay_min <= 180:
        return "Major Delay"
    return "Critical Delay"


def compute_agent_score(delay_min: float) -> int:
    if delay_min <= 0:
        return 5
    if delay_min <= 30:
        return 4
    if delay_min <= 60:
        return 3
    if delay_min <= 180:
        return 2
    return 1


def safe_div(x: Optional[float], y: Optional[float], default: float = 0.0) -> float:
    try:
        if y in (None, 0):
            return default
        return float(x) / float(y)
    except Exception:
        return default


# ----------------------
# Loading raw frames
# ----------------------
def load_deliveries_df_from_payload(payload: Any) -> pd.DataFrame:
    """
    Payload can be: {"data": [ ... ]}, list, or single dict.
    Returns a normalized DataFrame.
    """
    items = None
    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            items = payload["data"]
        elif "deliveries" in payload and isinstance(payload["deliveries"], list):
            items = payload["deliveries"]
        elif isinstance(payload.get("items"), list):
            items = payload["items"]
        else:
            # maybe dict representing one object
            items = [payload]
    elif isinstance(payload, list):
        items = payload
    else:
        items = [payload]

    try:
        df = pd.json_normalize(items)
        logger.info("Loaded %d delivery rows from payload", len(df))
        return df
    except Exception:
        logger.exception("Failed to json_normalize deliveries payload")
        return pd.DataFrame()


def load_traffic_df_from_payload(payload: Any) -> pd.DataFrame:
    items = None
    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            items = payload["data"]
        elif "routes" in payload and isinstance(payload["routes"], list):
            items = payload["routes"]
        elif isinstance(payload.get("items"), list):
            items = payload["items"]
        else:
            items = [payload]
    elif isinstance(payload, list):
        items = payload
    else:
        items = [payload]

    try:
        df = pd.json_normalize(items)
        logger.info("Loaded %d traffic rows from payload", len(df))
        return df
    except Exception:
        logger.exception("Failed to json_normalize traffic payload")
        return pd.DataFrame()


# ----------------------
# Main transform logic
# ----------------------
def transform(live_file: Optional[Path] = None, traffic_file: Optional[Path] = None) -> Optional[pd.DataFrame]:
    """
    Run transform: load raw -> clean -> enrich -> merge -> save staged CSV.
    If live_file/traffic_file are not provided, the function will attempt to find the latest files in RAW_DIR.
    Returns the transformed DataFrame or None on failure.
    """
    # determine files
    if live_file is None:
        live_file = find_latest_raw_file(DELIVERIES_PREFIXES)
        if live_file is None:
            logger.error("No deliveries raw file found in %s", RAW_DIR)
            return None
    if traffic_file is None:
        traffic_file = find_latest_raw_file(TRAFFIC_PREFIXES)
        if traffic_file is None:
            logger.warning("No traffic raw file found in %s; continuing without traffic merge", RAW_DIR)

    logger.info("Using live raw file: %s", live_file)
    if traffic_file:
        logger.info("Using traffic raw file: %s", traffic_file)

    # load payloads
    live_payload = load_json_payload(live_file)
    df_del = load_deliveries_df_from_payload(live_payload)
    if df_del.empty:
        logger.error("No delivery records to process.")
        return None

    df_traf = pd.DataFrame()
    if traffic_file:
        traffic_payload = load_json_payload(traffic_file)
        df_traf = load_traffic_df_from_payload(traffic_payload)

    # Canonicalize delivery columns: map common aliases to canonical names
    alias_map = {
        "shipment_id": ["shipment_id", "id", "shipmentId", "shipment_id_str"],
        "source_city": ["source_city", "origin", "from_city", "sourceCity"],
        "destination_city": ["destination_city", "destination", "to_city", "dest_city"],
        "dispatch_time": ["dispatch_time", "pickup_time", "dispatched_at"],
        "expected_delivery_time": ["expected_delivery_time", "scheduled_delivery_time", "expected_time"],
        "actual_delivery_time": ["actual_delivery_time", "delivery_time", "delivered_at"],
        "package_weight": ["package_weight", "weight", "package_weight_kg"],
        "delivery_agent_id": ["delivery_agent_id", "courier_id", "agent_id", "driver_id"],
    }

    rename_map = {}
    for canon, aliases in alias_map.items():
        for a in aliases:
            if a in df_del.columns:
                rename_map[a] = canon
                break
    if rename_map:
        df_del = df_del.rename(columns=rename_map)

    # Ensure canonical columns exist
    for col in alias_map.keys():
        if col not in df_del.columns:
            df_del[col] = None

    # Parse timestamps
    for tcol in ["dispatch_time", "expected_delivery_time", "actual_delivery_time"]:
        df_del[f"{tcol}_parsed"] = df_del[tcol].apply(parse_timestamp)

    # Drop rows with missing critical timestamps
    before = len(df_del)
    df_del = df_del[~(df_del["dispatch_time_parsed"].isna() | df_del["expected_delivery_time_parsed"].isna() | df_del["actual_delivery_time_parsed"].isna())].copy()
    logger.info("Dropped %d rows missing critical timestamps (remaining %d)", before - len(df_del), len(df_del))
    if df_del.empty:
        logger.error("No valid deliveries after timestamp parsing.")
        return None

    # Ensure parsed timestamps are datetime tz-aware
    df_del["dispatch_time_parsed"] = pd.to_datetime(df_del["dispatch_time_parsed"], utc=True)
    df_del["expected_delivery_time_parsed"] = pd.to_datetime(df_del["expected_delivery_time_parsed"], utc=True)
    df_del["actual_delivery_time_parsed"] = pd.to_datetime(df_del["actual_delivery_time_parsed"], utc=True)

    # Remove invalid durations (actual < dispatch or expected < dispatch)
    before = len(df_del)
    invalid_mask = (df_del["actual_delivery_time_parsed"] < df_del["dispatch_time_parsed"]) | (df_del["expected_delivery_time_parsed"] < df_del["dispatch_time_parsed"])
    df_del = df_del[~invalid_mask].copy()
    logger.info("Removed %d rows with invalid durations (remaining %d)", before - len(df_del), len(df_del))
    if df_del.empty:
        logger.error("No valid deliveries after duration checks.")
        return None

    # Compute delay_minutes and transit_minutes
    df_del["delay_minutes"] = (df_del["actual_delivery_time_parsed"] - df_del["expected_delivery_time_parsed"]).dt.total_seconds() / 60.0
    df_del["transit_minutes"] = (df_del["actual_delivery_time_parsed"] - df_del["dispatch_time_parsed"]).dt.total_seconds() / 60.0

    # Drop rows with NaN delay or negative transit
    before = len(df_del)
    df_del = df_del[df_del["delay_minutes"].notna()]
    df_del = df_del[df_del["transit_minutes"] >= 0]
    logger.info("Dropped %d rows with NaN delay or negative transit (remaining %d)", before - len(df_del), len(df_del))
    if df_del.empty:
        logger.error("No deliveries remaining after delay / transit checks.")
        return None

    # Normalize package weights
    df_del["package_weight_kg"] = df_del["package_weight"].apply(normalize_weight_to_kg)
    # Try nested metadata fields if present (common key patterns)
    if "metadata.package_weight" in df_del.columns:
        df_del["package_weight_kg"] = df_del["package_weight_kg"].fillna(df_del["metadata.package_weight"].apply(normalize_weight_to_kg))

    before = len(df_del)
    df_del = df_del[df_del["package_weight_kg"].notna()].copy()
    logger.info("Dropped %d rows missing package weight after normalization (remaining %d)", before - len(df_del), len(df_del))
    if df_del.empty:
        logger.error("No deliveries left after weight normalization.")
        return None

    # Delay classification and agent score
    df_del["delay_class"] = df_del["delay_minutes"].apply(lambda x: classify_delay(float(x)))
    df_del["agent_score"] = df_del["delay_minutes"].apply(lambda x: compute_agent_score(float(x)))

    # Prepare traffic DataFrame (canonicalize)
    if not df_traf.empty:
        traf_alias_map = {
            "city_name": ["city", "city_name", "name"],
            "congestion_score": ["congestion_score", "congestion", "score"],
            "avg_speed": ["avg_speed_kmh", "avg_speed", "avg_speed_km_h"],
            "weather_warnings": ["weather_warnings", "alerts", "warnings"],
            "timestamp": ["timestamp", "ts", "updated_at"]
        }
        traf_rename = {}
        for canon, aliases in traf_alias_map.items():
            for a in aliases:
                if a in df_traf.columns:
                    traf_rename[a] = canon
                    break
        if traf_rename:
            df_traf = df_traf.rename(columns=traf_rename)
        # ensure numeric
        if "congestion_score" in df_traf.columns:
            df_traf["congestion_score"] = pd.to_numeric(df_traf["congestion_score"], errors="coerce")
        if "avg_speed" in df_traf.columns:
            df_traf["avg_speed"] = pd.to_numeric(df_traf["avg_speed"], errors="coerce")
        # parse timestamp if present
        if "timestamp" in df_traf.columns:
            df_traf["timestamp_parsed"] = pd.to_datetime(df_traf["timestamp"], utc=True, errors="coerce")
            df_traf = df_traf.sort_values("timestamp_parsed")
        # pick latest per city
        df_traf_latest = df_traf.groupby("city_name", sort=False).last().reset_index()
    else:
        df_traf_latest = pd.DataFrame(columns=["city_name", "congestion_score", "avg_speed", "weather_warnings"])

    # Left join traffic info for source and destination
    src = df_traf_latest.rename(columns={
        "city_name": "source_city",
        "congestion_score": "src_congestion_score",
        "avg_speed": "src_avg_speed",
        "weather_warnings": "src_weather_warnings"
    })[["source_city", "src_congestion_score", "src_avg_speed", "src_weather_warnings"]]

    dest = df_traf_latest.rename(columns={
        "city_name": "destination_city",
        "congestion_score": "dest_congestion_score",
        "avg_speed": "dest_avg_speed",
        "weather_warnings": "dest_weather_warnings"
    })[["destination_city", "dest_congestion_score", "dest_avg_speed", "dest_weather_warnings"]]

    df = df_del.merge(src, on="source_city", how="left")
    df = df.merge(dest, on="destination_city", how="left")

    # Ensure numeric columns
    for c in ["src_congestion_score", "dest_congestion_score", "src_avg_speed", "dest_avg_speed"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Traffic impact per end and combined
    def compute_impact(cong: Optional[float], avg_spd: Optional[float]) -> Optional[float]:
        try:
            if cong is None or avg_spd in (None, 0) or math.isnan(avg_spd):
                return None
            return float(cong) * (1.0 / float(avg_spd)) * 10.0
        except Exception:
            return None

    df["src_traffic_impact"] = df.apply(lambda r: compute_impact(r.get("src_congestion_score"), r.get("src_avg_speed")), axis=1)
    df["dest_traffic_impact"] = df.apply(lambda r: compute_impact(r.get("dest_congestion_score"), r.get("dest_avg_speed")), axis=1)
    df["traffic_impact_score"] = df[["src_traffic_impact", "dest_traffic_impact"]].max(axis=1).fillna(0.0)

    def risk_from_score(sc: float) -> str:
        try:
            if sc > 15:
                return "High Risk"
            if sc > 7:
                return "Moderate Risk"
            return "Low Risk"
        except Exception:
            return "Low Risk"

    df["traffic_risk_level"] = df["traffic_impact_score"].apply(risk_from_score)

    # efficiency = (package_weight / (delay_minutes + 1)) * agent_score
    df["efficiency_raw"] = df.apply(lambda r: safe_div(r.get("package_weight_kg", 0.0), (r.get("delay_minutes", 0.0) + 1.0) if r.get("delay_minutes") is not None else 1.0) * r.get("agent_score", 1), axis=1)

    # predicted delay risk level (simple heuristic)
    def predicted_risk(r) -> str:
        dm = r.get("delay_minutes", 0.0) or 0.0
        tr = r.get("traffic_risk_level", "Low Risk")
        if tr == "High Risk" or dm > 60:
            return "High"
        if tr == "Moderate Risk" or (30 < dm <= 60):
            return "Medium"
        return "Low"

    df["predicted_delay_risk_level"] = df.apply(predicted_risk, axis=1)

    # delivery_efficiency_index: min-max scale efficiency_raw to 0-100
    eff = df["efficiency_raw"].replace([float("inf"), -float("inf")], pd.NA).fillna(0.0).astype(float)
    min_e = float(eff.min()) if not eff.empty else 0.0
    max_e = float(eff.max()) if not eff.empty else 0.0
    if math.isclose(min_e, max_e):
        df["delivery_efficiency_index"] = 50.0
    else:
        df["delivery_efficiency_index"] = ((eff - min_e) / (max_e - min_e) * 100.0).clip(0, 100)

    # Final output columns
    out_cols = [
        "shipment_id",
        "source_city",
        "destination_city",
        "dispatch_time_parsed",
        "expected_delivery_time_parsed",
        "actual_delivery_time_parsed",
        "package_weight_kg",
        "delivery_agent_id",
        "delay_minutes",
        "delay_class",
        "agent_score",
        "src_congestion_score",
        "src_avg_speed",
        "src_weather_warnings",
        "dest_congestion_score",
        "dest_avg_speed",
        "dest_weather_warnings",
        "traffic_impact_score",
        "traffic_risk_level",
        "efficiency_raw",
        "predicted_delay_risk_level",
        "delivery_efficiency_index",
        "transit_minutes",
    ]
    for c in out_cols:
        if c not in df.columns:
            df[c] = None

    df_out = df[out_cols].copy()
    df_out = df_out.rename(columns={
        "dispatch_time_parsed": "dispatch_time",
        "expected_delivery_time_parsed": "expected_delivery_time",
        "actual_delivery_time_parsed": "actual_delivery_time"
    })

    # Convert timestamps to ISO strings (UTC) for CSV readability
    for tcol in ["dispatch_time", "expected_delivery_time", "actual_delivery_time"]:
        if tcol in df_out.columns:
            df_out[tcol] = pd.to_datetime(df_out[tcol], utc=True)

    ts = _now_ts()
    out_file = STAGED_DIR / f"deliveries_transformed_{ts}.csv"
    try:
        df_out.to_csv(out_file, index=False)
        logger.info("Saved transformed staged CSV: %s (rows=%d)", out_file, len(df_out))
    except Exception as e:
        logger.exception("Failed to save staged CSV to %s: %s", out_file, e)
        return None

    return df_out


# ----------------------
# CLI
# ----------------------
def parse_args(argv: Optional[List[str]] = None) -> Tuple[Optional[Path], Optional[Path]]:
    import argparse
    p = argparse.ArgumentParser(description="Transform raw delivery + traffic JSON into staged CSV")
    p.add_argument("--live-file", type=str, help="Path to live deliveries raw JSON")
    p.add_argument("--traffic-file", type=str, help="Path to traffic raw JSON")
    args = p.parse_args(argv)
    lf = Path(args.live_file) if args.live_file else None
    tf = Path(args.traffic_file) if args.traffic_file else None
    return lf, tf


if __name__ == "__main__":
    lf_arg, tf_arg = parse_args()
    df_result = transform(live_file=lf_arg, traffic_file=tf_arg)
    if df_result is None:
        logger.error("Transform failed or produced no output.")
        sys.exit(1)
    logger.info("Transform completed successfully.")
