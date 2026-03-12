"""
Main data collector. Run this continuously (e.g. with nohup or a cron job).
Polls arrivals-and-departures for each stop every POLL_INTERVAL seconds.
Saves raw snapshots to data/raw/YYYY-MM-DD.csv

Columns saved per record:
  snapshot_ts, stop_id, route_id, trip_id, vehicle_id,
  scheduled_arrival_ts, predicted_arrival_ts, status
"""

import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OBA_API_KEY")
BASE_URL = os.getenv("OBA_BASE_URL")
STOPS_FILE = "data/stops.csv"
RAW_DIR = "data/raw"
POLL_INTERVAL = 300  # seconds between full polling cycles (5 minutes)
REQUEST_DELAY = 0.3  # seconds between individual API calls (avoids 429)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def get_arrivals(stop_id):
    url = f"{BASE_URL}/arrivals-and-departures-for-stop/{stop_id}.json"
    r = requests.get(url, params={"key": API_KEY, "minutesBefore": 0, "minutesAfter": 60}, timeout=10)
    r.raise_for_status()
    data = r.json().get("data") or {}
    entry = data.get("entry") or {}
    return entry.get("arrivalsAndDepartures", [])


def arrivals_to_records(stop_id, arrivals, snapshot_ts):
    records = []
    for a in arrivals:
        # Only save when the bus is AT this stop
        if a.get("numberOfStopsAway") != 0:
            continue
        if not a.get("predicted", False):
            continue

        scheduled_ms = a.get("scheduledArrivalTime")
        predicted_ms = a.get("predictedArrivalTime")

        if not scheduled_ms or not predicted_ms:
            continue

        records.append({
            "snapshot_ts": snapshot_ts,
            "stop_id": stop_id,
            "route_id": a.get("routeId", ""),
            "route_short_name": a.get("routeShortName", ""),
            "trip_id": a.get("tripId", ""),
            "vehicle_id": a.get("vehicleId", ""),
            "scheduled_arrival_ts": scheduled_ms // 1000,
            "predicted_arrival_ts": predicted_ms // 1000,
            "status": a.get("status", ""),
        })
    return records


def get_csv_path():
    today = date.today().isoformat()
    os.makedirs(RAW_DIR, exist_ok=True)
    return os.path.join(RAW_DIR, f"{today}.csv")


def save_records(records):
    if not records:
        return
    df = pd.DataFrame(records)
    path = get_csv_path()
    header = not os.path.exists(path)
    df.to_csv(path, mode="a", index=False, header=header)


def load_stops():
    if not os.path.exists(STOPS_FILE):
        raise FileNotFoundError(
            f"{STOPS_FILE} not found. Run src/setup_stops.py first."
        )
    df = pd.read_csv(STOPS_FILE)
    return df["stop_id"].tolist()


def poll_once(stop_ids):
    snapshot_ts = int(time.time())
    all_records = []
    errors = 0

    for i, stop_id in enumerate(stop_ids):
        try:
            arrivals = get_arrivals(stop_id)
            records = arrivals_to_records(stop_id, arrivals, snapshot_ts)
            all_records.extend(records)
        except Exception as e:
            errors += 1
            if errors <= 5:
                log.warning(f"Stop {stop_id} error: {e}")
        time.sleep(REQUEST_DELAY)

        if (i + 1) % 50 == 0:
            log.info(f"  Progress: {i+1}/{len(stop_ids)} stops, {len(all_records)} records so far")

    save_records(all_records)
    log.info(f"Saved {len(all_records)} records ({errors} errors)")
    return len(all_records)


def main():
    log.info("Loading stops...")
    stop_ids = load_stops()
    log.info(f"Loaded {len(stop_ids)} stops. Starting collection loop.")
    log.info(f"Poll interval: {POLL_INTERVAL}s | Request delay: {REQUEST_DELAY}s")
    log.info(f"Estimated cycle time: {len(stop_ids) * REQUEST_DELAY:.0f}s per round")

    cycle = 0
    while True:
        cycle += 1
        start = time.time()
        log.info(f"--- Cycle {cycle} ---")

        poll_once(stop_ids)

        elapsed = time.time() - start
        wait = max(0, POLL_INTERVAL - elapsed)
        if wait > 0:
            log.info(f"Cycle done in {elapsed:.1f}s. Waiting {wait:.1f}s...")
            time.sleep(wait)
        else:
            log.info(f"Cycle done in {elapsed:.1f}s (over budget, starting immediately)")


if __name__ == "__main__":
    main()
