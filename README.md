# Seattle Bus Arrival Delay Prediction

Predicting King County Metro bus arrival delays using real-time transit data and weather conditions. The core research question: **does Seattle rain meaningfully degrade bus schedule reliability, and can ML models compensate?**

## Problem Formulation

- **Task**: Regression — predict bus arrival delay in minutes
- **Target**: `predicted_delay_min` = (OBA real-time predicted arrival) − (scheduled arrival)
- **Key feature**: Weather conditions (rain, temperature, wind) merged by hour

> Note: OneBusAway does not expose historical actual arrival times. We use the real-time GPS-based predicted arrival time from OBA as our ground truth proxy, which is accurate to within ~1–2 minutes of actual arrival.

## Data Sources

| Source | Data | API |
|--------|------|-----|
| [OneBusAway Puget Sound](https://api.pugetsound.onebusaway.org) | Real-time bus predictions (scheduled vs. predicted arrival) | Key required |
| [King County Metro GTFS](https://www.soundtransit.org/GTFS-KCM/google_transit.zip) | Static schedule data — `stop_times.txt` used to get scheduled arrival times per (trip_id, stop_id) | Free download, no key |
| [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) | Hourly historical weather for Seattle (lat 47.6062, lon -122.3321) via `archive-api.open-meteo.com/v1/archive` — variables: `temperature_2m`, `precipitation`, `wind_speed_10m`, `visibility`, `weather_code` (WMO) | Free, no key required |

## Features

| Feature | Description |
|---------|-------------|
| `route_id`, `stop_id` | Route and stop identifiers |
| `hour_of_day`, `day_of_week` | Time of day and day |
| `is_weekend`, `is_peak_hour` | Derived time features |
| `temperature_c` | Air temperature |
| `precipitation_mm` | Hourly rainfall amount |
| `is_raining` | Binary rain indicator (WMO weather code) |
| `wind_speed_kmh` | Wind speed |
| `visibility_km` | Visibility (captures fog/snow) |
| `predicted_delay_min` | **Target variable** |

## Models

Linear Regression → Ridge/Lasso → Decision Tree → Random Forest → XGBoost → MLP → (time permitting) LSTM

## Project Structure

```
ML6140-project/
├── data/
│   ├── raw/              # Hourly snapshots from OBA API (one CSV per day)
│   ├── processed/        # Merged dataset with weather features
│   └── stops.csv         # Sampled stops for polling (~500 stops)
├── src/
│   ├── setup_stops.py    # One-time: fetch all KC Metro routes and stops
│   ├── collector.py      # Continuous polling loop (runs every 5 minutes)
│   ├── post_process.py   # Raw snapshots → inferred delay records
│   ├── merge_weather.py  # Attach hourly weather from Open-Meteo
│   ├── processing.py     # Filter weather categories for model input
│   └── run_pipeline.py   # Run the full processing pipeline end to end
├── notebooks/
│   ├── 01_eda.ipynb
│   ├── 02_linear_regression.ipynb
│   ├── 03_random_forest.ipynb
│   ├── 04_xgboost.ipynb
│   └── 05_model_input_analysis.ipynb
└── requirements.txt
```

## Pipeline

```bash
# 1. One-time setup: fetch stops
python src/setup_stops.py

# 2. Continuous data collection (run in background)
nohup python src/collector.py > logs/collector.log 2>&1 &

# 3. After data accumulates (1+ days), run the full pipeline
python src/run_pipeline.py

# Or run each step manually
python src/post_process.py
python src/merge_weather.py
python src/processing.py

# Outputs:
# - data/processed/dataset.csv      (full processed dataset)
# - data/processed/model_input.csv  (model-ready weather subset)
```

`dataset.csv` remains the full processed dataset. `model_input.csv` is a derived
file that keeps only `dry`, `rain_light`, and `rain_moderate` rows and adds a
`weather_category` column for modeling and analysis.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # add your OBA API key
```
