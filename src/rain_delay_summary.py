"""
rain_delay_summary.py

비 올때 vs 안 올때 평균 delay 비교 요약.

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
