"""
rain_delay_summary.py

Summary of mean delay: rain vs no rain.

Usage:
  python src/rain_delay_summary.py
"""

import pandas as pd

PROCESSED_FILE = "data/processed/dataset.csv"

df = pd.read_csv(PROCESSED_FILE)

print(f"Total records: {len(df):,}\n")

summary = df.groupby("is_raining")["delay_min"].agg(
    count="count",
    mean="mean",
    median="median",
    std="std",
)
summary.index = summary.index.map({0: "No Rain", 1: "Rain"})
summary = summary.rename_axis("Condition")
summary = summary.round(2)

print(summary.to_string())

no_rain = df[df["is_raining"] == 0]["delay_min"].mean()
rain    = df[df["is_raining"] == 1]["delay_min"].mean()
diff    = rain - no_rain

print(f"\nRain vs No Rain: {diff:+.2f} min  ({'more' if diff > 0 else 'less'} delay when raining)")

# Hour-level breakdown for key hours
print("\n=== Rain impact by hour (key hours) ===")
key_hours = [9, 18, 19, 22]
hourly = df.groupby(["hour_of_day", "is_raining"])["delay_min"].mean().unstack()
hourly.columns = ["No Rain", "Rain"]
hourly["diff"] = (hourly["Rain"] - hourly["No Rain"]).round(2)

print(f"{'Hour':<8} {'No Rain':>10} {'Rain':>10} {'Diff':>10}")
print("-" * 42)
for h in key_hours:
    if h in hourly.index:
        row = hourly.loc[h]
        print(f"{int(h):02d}:00   {row['No Rain']:>10.2f} {row['Rain']:>10.2f} {row['diff']:>+10.2f}")
