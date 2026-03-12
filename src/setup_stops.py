"""
One-time script: Fetch all King County Metro routes and stops,
then sample up to MAX_STOPS stops for polling.
Output: data/stops.csv
"""

import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OBA_API_KEY")
BASE_URL = os.getenv("OBA_BASE_URL")
AGENCY_ID = os.getenv("KC_METRO_AGENCY_ID", "1")
MAX_STOPS = 500  # how many stops to poll
OUTPUT_PATH = "data/stops.csv"


def get(endpoint, params=None):
    params = params or {}
    params["key"] = API_KEY
    r = requests.get(f"{BASE_URL}/{endpoint}.json", params=params, timeout=10)
    r.raise_for_status()
    return r.json()["data"]


def fetch_routes():
    print(f"Fetching routes for agency {AGENCY_ID}...")
    data = get(f"routes-for-agency/{AGENCY_ID}")
    routes = data["list"]
    print(f"  Found {len(routes)} routes")
    return routes


def fetch_stops_for_route(route_id):
    data = get(f"stops-for-route/{route_id}")
    # stops-for-route returns references with stop objects
    stops = data.get("references", {}).get("stops", [])
    return stops


def main():
    routes = fetch_routes()

    all_stops = {}  # stop_id -> stop info
    for i, route in enumerate(routes):
        route_id = route["id"]
        route_name = route.get("shortName") or route.get("longName", "")
        print(f"[{i+1}/{len(routes)}] Route {route_name} ({route_id})")
        try:
            stops = fetch_stops_for_route(route_id)
            for s in stops:
                sid = s["id"]
                if sid not in all_stops:
                    all_stops[sid] = {
                        "stop_id": sid,
                        "stop_name": s.get("name", ""),
                        "lat": s.get("lat"),
                        "lon": s.get("lon"),
                        "direction": s.get("direction", ""),
                    }
        except Exception as e:
            print(f"  Error: {e}")
        time.sleep(0.3)  # be polite to the API

    df = pd.DataFrame(list(all_stops.values()))
    print(f"\nTotal unique stops: {len(df)}")

    # Sample up to MAX_STOPS evenly distributed
    if len(df) > MAX_STOPS:
        df = df.sample(n=MAX_STOPS, random_state=42).reset_index(drop=True)
        print(f"Sampled down to {MAX_STOPS} stops")

    os.makedirs("data", exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
