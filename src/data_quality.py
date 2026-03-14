"""
Todo：
- dorp visibility_km, value all mising✅
- snapshot_pred_diff_min:The max value is 29.5 million minutes (~56 years) check for outliers. 79% good - 33lines predicted_arrival_ts = -1, 21% bad ✅
- F-Line, route 2, 161 and route 181 have most early arrivals.why?

"""

"""
Quick sanity checks for snapshot vs predicted arrival gaps on specific routes.

Usage:
  python src/data_cleaning.py            # uses default dataset.csv
  python src/data_cleaning.py path/to/dataset.csv

Focus routes: F Line, 2, 161.
Reports distribution of (snapshot_ts - predicted_arrival_ts) in minutes and
shares of records outside ±5 and ±10 minutes.
"""

import sys
import pandas as pd
from pathlib import Path

# Define routes of interest and default dataset path
ROUTES_OF_INTEREST = ["F Line", "2", "161", "181"]
DEFAULT_DATA_PATH = Path("data/processed/dataset.csv")


def route_gap_stats(df, route):
    sub = df[df["route_short_name"] == route].copy()
    if sub.empty:
        return None

    sub["snap_pred_diff_min"] = (sub["snapshot_ts"] - sub["predicted_arrival_ts"]) / 60
    s = sub["snap_pred_diff_min"]

    stats = {
        "count": len(sub),
        "mean": s.mean(),
        "median": s.median(),
        "std": s.std(),
        "min": s.min(),
        "max": s.max(),
        "p5": s.quantile(0.05),
        "p95": s.quantile(0.95),
        "share_outside_5min": (s.abs() > 5).mean() * 100,
        "share_outside_10min": (s.abs() > 10).mean() * 100,
    }

    # capture extreme examples for manual inspection
    extremes = pd.concat(
        [
            sub.nsmallest(3, "snap_pred_diff_min")[["date", "hour_of_day", "route_short_name", "snapshot_ts", "predicted_arrival_ts", "snap_pred_diff_min"]],
            sub.nlargest(3, "snap_pred_diff_min")[["date", "hour_of_day", "route_short_name", "snapshot_ts", "predicted_arrival_ts", "snap_pred_diff_min"]],
        ]
    )

    return stats, extremes


def main():
    data_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DATA_PATH
    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found: {data_path}")

    df = pd.read_csv(data_path)
    print(f"Loaded {len(df):,} rows from {data_path}")

   # report missing values before dropping snap_pred_diff_min
    print("\nMissing values per column:")
    missing = df.isna().sum()
    print(missing[missing > 0] if missing.any() else "  None")

    for route in ROUTES_OF_INTEREST:
        result = route_gap_stats(df, route)
        if result is None:
            print(f"\nRoute {route}: no records")
            continue
        stats, extremes = result
        print(f"\nRoute {route}")
        for k, v in stats.items():
            if isinstance(v, float):
                print(f"  {k}: {v:.2f}")
            else:
                print(f"  {k}: {v}")
        print("  Extreme examples (mins):")
        print(extremes.to_string(index=False))


if __name__ == "__main__":
    main()
