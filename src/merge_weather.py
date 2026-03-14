"""
merge_weather.py

Fetches historical hourly weather for Seattle from Open-Meteo (free, no key needed)
and merges it into the processed dataset by hour.

Open-Meteo archive API:
  https://archive-api.open-meteo.com/v1/archive

Usage:
  python src/merge_weather.py   # reads data/processed/dataset.csv, writes same file
"""

import os
import requests
import pandas as pd

SEATTLE_LAT = 47.6062
SEATTLE_LON = -122.3321
PROCESSED_FILE = "data/processed/dataset.csv"
WEATHER_CACHE = "data/processed/weather_cache.csv"

HOURLY_VARS = [
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "visibility",
    "weather_code",
]


def fetch_weather(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch hourly weather from Open-Meteo for a date range."""
    print(f"Fetching weather {start_date} → {end_date}...")
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": SEATTLE_LAT,
        "longitude": SEATTLE_LON,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "America/Los_Angeles",
        "wind_speed_unit": "kmh",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()["hourly"]

    df = pd.DataFrame(data)
    df.rename(columns={
        "time": "hour_bucket",
        "temperature_2m": "temperature_c",
        "precipitation": "precipitation_mm",
        "wind_speed_10m": "wind_speed_kmh",
        "visibility": "visibility_km",
        "weather_code": "weather_code",
    }, inplace=True)

    # WMO weather code → is_raining
    # Codes 51-67: drizzle/rain, 71-77: snow, 80-82: rain showers, 95-99: thunderstorm
    rain_codes = set(range(51, 68)) | set(range(80, 83)) | set(range(95, 100))
    df["is_raining"] = df["weather_code"].isin(rain_codes).astype(int)

    # Normalize hour_bucket to match format from post_process.py: "YYYY-MM-DD HH:00"
    df["hour_bucket"] = pd.to_datetime(df["hour_bucket"]).dt.strftime("%Y-%m-%d %H:00")

    return df


def load_or_fetch_weather(dates: list) -> pd.DataFrame:
    """Use cached weather if available, fetch missing dates."""
    dates = sorted(set(dates))
    start_date = dates[0]
    end_date = dates[-1]

    if os.path.exists(WEATHER_CACHE):
        cached = pd.read_csv(WEATHER_CACHE)
        cached_dates = set(pd.to_datetime(cached["hour_bucket"]).dt.date.astype(str))
        needed = set(dates) - cached_dates
        if not needed:
            print(f"Using cached weather ({len(cached)} rows)")
            return cached
        print(f"Cache missing {len(needed)} dates, fetching...")

    weather = fetch_weather(start_date, end_date)
    weather.to_csv(WEATHER_CACHE, index=False)
    print(f"Weather cached to {WEATHER_CACHE}")
    return weather


def main():
    if not os.path.exists(PROCESSED_FILE):
        print(f"{PROCESSED_FILE} not found. Run post_process.py first.")
        return

    df = pd.read_csv(PROCESSED_FILE)
    print(f"Loaded {len(df)} records from {PROCESSED_FILE}")

    if "hour_bucket" not in df.columns:
        print("Missing 'hour_bucket' column. Re-run post_process.py.")
        return

    dates = df["date"].dropna().unique().tolist()
    print(f"Date range: {min(dates)} → {max(dates)}")

    weather = load_or_fetch_weather(dates)

    # Drop any existing weather columns before merging (idempotent)
    weather_cols = ["temperature_c", "precipitation_mm", "wind_speed_kmh",
                    "visibility_km", "weather_code", "is_raining"]
    df = df.drop(columns=[c for c in weather_cols if c in df.columns])

    # Merge on hour_bucket
    merged = df.merge(weather, on="hour_bucket", how="left")

    missing_weather = merged["temperature_c"].isna().sum()
    if missing_weather > 0:
        print(f"Warning: {missing_weather} rows have no weather match")

    # Drop weather columns that are all NaN
    merged = merged.drop(columns=[c for c in merged.columns if merged[c].isna().all()])

    merged.to_csv(PROCESSED_FILE, index=False)
    print(f"\nSaved {len(merged)} records with weather to {PROCESSED_FILE}")

    # Quick summary
    rain_pct = merged["is_raining"].mean() * 100
    print(f"\nRainy records: {rain_pct:.1f}%")
    print(merged.groupby("is_raining")["delay_min"].describe().round(2))


if __name__ == "__main__":
    main()
