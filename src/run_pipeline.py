"""
run_pipeline.py

Runs the full data pipeline:
  1. post_process.py -> data/processed/dataset.csv
  2. merge_weather.py -> enriches data/processed/dataset.csv
  3. processing.py -> data/processed/model_input.csv

Usage:
  python src/run_pipeline.py
  python src/run_pipeline.py data/raw/2026-03-24.csv
"""

import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = BASE_DIR / "src"


def run_step(script_name: str, extra_args: list[str] | None = None) -> None:
    cmd = [sys.executable, str(SRC_DIR / script_name)]
    if extra_args:
        cmd.extend(extra_args)

    print(f"\n=== Running {script_name} ===")
    subprocess.run(cmd, check=True, cwd=BASE_DIR)


def main() -> None:
    raw_paths = sys.argv[1:]

    run_step("post_process.py", raw_paths)
    run_step("merge_weather.py")
    run_step("processing.py")

    print("\nPipeline complete.")
    print("Full dataset: data/processed/dataset.csv")
    print("Model input:  data/processed/model_input.csv")


if __name__ == "__main__":
    main()
