"""
post_process.py

raw CSV(들) → processed CSV with delay computed.

Logic:
  Filter rows where number_of_stops_away == 0 (bus is AT the stop).
  At that moment, predicted_arrival_ts ≈ actual arrival time.
  delay = predicted_arrival_ts - scheduled_arrival_ts

  This is the cleanest proxy for actual delay without needing
  historical actual arrival data.

Usage:
  python src/post_process.py                    # process all raw files
  python src/post_process.py data/raw/2026-03-06.csv   # specific file
"""

import os
import sys
import glob
import pandas as pd

RAW_DIR = "data/raw"
OUTPUT = "data/processed/dataset.csv"
GTFS_STOP_TIMES = "data/gtfs/stop_times.txt"


def load_raw(paths):
    dfs = []
    for p in paths:
        try:
            df = pd.read_csv(p, on_bad_lines="skip")
            dfs.append(df)
            print(f"  Loaded {p}: {len(df)} rows")
        except Exception as e:
            print(f"  Skip {p}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def load_gtfs_scheduled():
    """Load GTFS stop_times and return a lookup: (trip_id, stop_id) → scheduled Unix ts.
    GTFS arrival_time is HH:MM:SS (may exceed 24h for overnight trips).
    We return just the time-of-day in seconds since midnight for joining later.
    """
    print("Loading GTFS stop_times...")
    st = pd.read_csv(GTFS_STOP_TIMES, dtype=str,
                     usecols=["trip_id", "stop_id", "arrival_time"])
    # Strip OBA prefix: "1_736436259" → "736436259"
    return st.rename(columns={"arrival_time": "gtfs_arrival_time"})


def merge_gtfs(df, gtfs):
    """Join OBA data with GTFS scheduled times via (trip_id, stop_id)."""
    df = df.copy()
    df["trip_id_bare"] = df["trip_id"].str.split("_", n=1).str[-1]
    df["stop_id_bare"] = df["stop_id"].str.split("_", n=1).str[-1]

    merged = df.merge(gtfs, left_on=["trip_id_bare", "stop_id_bare"],
                      right_on=["trip_id", "stop_id"], how="left",
                      suffixes=("", "_gtfs"))
    # Drop the duplicate columns from GTFS side
    merged = merged.drop(columns=["trip_id_gtfs", "stop_id_gtfs"], errors="ignore")

    # Convert GTFS HH:MM:SS to Unix timestamp using the date from snapshot_ts
    def gtfs_time_to_ts(row):
        if pd.isna(row["gtfs_arrival_time"]):
            return None
        parts = row["gtfs_arrival_time"].split(":")
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        # midnight of the snapshot date (local time Seattle)
        snap_dt = pd.Timestamp(row["snapshot_ts"], unit="s", tz="UTC").tz_convert("America/Los_Angeles")
        midnight = snap_dt.normalize()  # midnight local time
        return int(midnight.timestamp()) + h * 3600 + m * 60 + s

    print("Converting GTFS times to Unix timestamps...")
    merged["gtfs_scheduled_ts"] = merged.apply(gtfs_time_to_ts, axis=1)

    no_match = merged["gtfs_scheduled_ts"].isna().sum()
    if no_match > 0:
        print(f"  Warning: {no_match} rows had no GTFS match")

    return merged


def deduplicate(df):
    return (
        df.dropna(subset=["predicted_arrival_ts"])
          .sort_values("snapshot_ts")
          .drop_duplicates(subset=["stop_id", "trip_id", "gtfs_scheduled_ts"], keep="first")
    )


def add_time_features(df):
    df = df.copy()
    dt = pd.to_datetime(df["gtfs_scheduled_ts"], unit="s", utc=True).dt.tz_convert("America/Los_Angeles")
    df["hour_of_day"] = dt.dt.hour
    df["day_of_week"] = dt.dt.dayofweek   # 0=Mon, 6=Sun
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["is_peak_hour"] = (
        ((df["hour_of_day"] >= 7) & (df["hour_of_day"] <= 9)) |
        ((df["hour_of_day"] >= 16) & (df["hour_of_day"] <= 19))
    ).astype(int)
    df["month"] = dt.dt.month
    df["date"] = dt.dt.date.astype(str)
    df["hour_bucket"] = dt.dt.strftime("%Y-%m-%d %H:00")
    return df


def compute_delay(df):
    df = df.copy()
    # delay = actual arrival (snapshot when stops_away=0) - GTFS scheduled
    df["delay_min"] = (
        (df["snapshot_ts"] - df["gtfs_scheduled_ts"]) / 60
    ).round(2)
    return df


def main():
    if len(sys.argv) > 1:
        paths = sys.argv[1:]
    else:
        paths = sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))

    if not paths:
        print(f"No CSV files found in {RAW_DIR}")
        return

    print(f"Loading {len(paths)} file(s)...")
    raw = load_raw(paths)
    if raw.empty:
        print("No data loaded.")
        return

    print(f"Total raw rows: {len(raw)}")

    gtfs = load_gtfs_scheduled()
    df = merge_gtfs(raw, gtfs)

    print("Deduplicating...")
    df = deduplicate(df)
    print(f"  Records after dedup: {len(df)}")

    df = compute_delay(df)
    df = add_time_features(df)

    keep = ["stop_id", "route_id", "route_short_name", "trip_id", "vehicle_id",
            "snapshot_ts", "gtfs_scheduled_ts", "predicted_arrival_ts", "delay_min",
            "hour_of_day", "day_of_week", "is_weekend", "is_peak_hour",
            "month", "date", "hour_bucket", "status"]
    out = df[[c for c in keep if c in df.columns]].copy()

    # Sanity filter: drop extreme outliers (>2 hours delay or >30 min early)
    out = out[(out["delay_min"] >= -30) & (out["delay_min"] <= 120)]

    os.makedirs("data/processed", exist_ok=True)
    out.to_csv(OUTPUT, index=False)
    print(f"\nSaved {len(out)} records to {OUTPUT}")
    print(out[["route_short_name", "delay_min", "hour_of_day", "is_weekend"]].describe())
    print("\nSample delays:")
    print(out[["route_short_name", "gtfs_scheduled_ts", "snapshot_ts", "delay_min"]].head(10))


if __name__ == "__main__":
    main()
