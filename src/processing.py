"""
processing.py

Builds a model input file from the processed dataset by keeping only
dry, rain_light, and rain_moderate records.

Usage:
  python src/processing.py
"""

import csv
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data" / "processed" / "dataset.csv"
OUTPUT_FILE = BASE_DIR / "data" / "processed" / "model_input.csv"

RAIN_MAP = {
    "51": "drizzle",
    "53": "drizzle",
    "55": "drizzle",
    "61": "rain_light",
    "63": "rain_moderate",
    "65": "rain_heavy",
    "66": "freezing_rain",
    "67": "freezing_rain",
    "71": "snow_light",
    "73": "snow_moderate",
    "75": "snow_heavy",
    "77": "snow_grains",
    "80": "shower",
    "81": "shower",
    "82": "shower",
    "85": "snow_shower_light",
    "86": "snow_shower_heavy",
    "95": "thunderstorm",
    "96": "thunderstorm",
    "99": "thunderstorm",
}

KEEP_CATEGORIES = {"dry", "rain_light", "rain_moderate", "rain_heavy"}
DROP_COLUMNS = {
    "trip_id",
    "vehicle_id",
    "snapshot_ts",
    "gtfs_scheduled_ts",
    "predicted_arrival_ts",
    "date",
    "hour_bucket",
    "month",
}


def categorize_weather(row: dict) -> str:
    precipitation = row.get("precipitation_mm", "").strip()
    weather_code = row.get("weather_code", "").strip()

    try:
        if precipitation and float(precipitation) <= 0:
            return "dry"
    except ValueError:
        pass

    return RAIN_MAP.get(weather_code, "other")


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    with INPUT_FILE.open("r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        fieldnames = list(reader.fieldnames or [])

        if "weather_code" not in fieldnames:
            raise ValueError("Missing required column: weather_code")
        if "precipitation_mm" not in fieldnames:
            raise ValueError("Missing required column: precipitation_mm")

        output_fields = [
            field for field in fieldnames
            if field not in DROP_COLUMNS
        ] + ["weather_category"]
        rows_written = 0

        with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=output_fields)
            writer.writeheader()

            for row in reader:
                category = categorize_weather(row)
                if category not in KEEP_CATEGORIES:
                    continue

                filtered_row = {
                    key: value for key, value in row.items()
                    if key in output_fields
                }
                filtered_row["weather_category"] = category
                writer.writerow(filtered_row)
                rows_written += 1

    print(f"Saved {rows_written} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
