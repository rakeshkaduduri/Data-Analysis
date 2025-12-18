import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path(os.getenv("RAW_DIR", Path(__file__).resolve().parents[0] / "data" / "raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Use Open-Meteo (free, no auth) - define city coords
CITY_COORDS = {
    "Delhi": (28.6139, 77.2090),
    "Bengaluru": (12.9716, 77.5946),
    "Hyderabad": (17.3753, 78.4742),  # CVR College corrected
    "Mumbai": (19.0760, 72.8777),
    "Kolkata": (22.5726, 88.3639)
}
API_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"
DEFAULT_CITIES = os.getenv("AQ_CITIES", "Delhi,Bengaluru,Hyderabad,Mumbai,Kolkata").split(",")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "30"))
SLEEP_BETWEEN_CALLS = float(os.getenv("SLEEP_BETWEEN_CALLS", "0.5"))

def _now_ts() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _save_raw(payload: object, city: str) -> str:
    ts = _now_ts()
    filename = f"{city.replace(' ', '').lower()}_raw{ts}.json"
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    return str(path.resolve())

def _fetch_city(city: str, max_retries: int = MAX_RETRIES, timeout: int = TIMEOUT_SECONDS) -> Dict[str, Optional[str]]:
    if city not in CITY_COORDS:
        return {"city": city, "success": "false", "error": f"No coordinates for {city}"}
    
    lat, lon = CITY_COORDS[city]
    url = f"{API_BASE}?latitude={lat}&longitude={lon}&hourly=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide"
    
    attempt = 0
    last_error: Optional[str] = None
    
    while attempt < max_retries:
        attempt += 1
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            payload = resp.json()
            saved = _save_raw(payload, city)
            print(f"✅ [{city}] fetched and saved to: {saved}")
            return {"city": city, "success": "true", "raw_path": saved}
        except Exception as e:
            last_error = str(e)
            print(f"⚠️ [{city}] attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(2 ** (attempt - 1))
    
    return {"city": city, "success": "false", "error": last_error}

def fetch_all_cities(cities: Optional[List[str]] = None) -> List[Dict[str, Optional[str]]]:
    if cities is None:
        cities = [c.strip() for c in DEFAULT_CITIES if c.strip()]
    
    results: List[Dict[str, Optional[str]]] = []
    for city in cities:
        res = _fetch_city(city)
        results.append(res)
        time.sleep(SLEEP_BETWEEN_CALLS)
    return results

if __name__ == "__main__":
    print("Starting Open-Meteo air quality extraction (free API)")
    cities_env = [c.strip() for c in DEFAULT_CITIES if c.strip()]
    print(f"Cities: {cities_env}")
    out = fetch_all_cities(cities_env)
    print("Extraction complete. Summary:")
    for r in out:
        if r.get("success") == "true":
            print(f" - {r['city']}: saved -> {r['raw_path']}")
        else:
            print(f" - {r['city']}: ERROR -> {r.get('error')}")
