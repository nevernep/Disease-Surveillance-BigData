"""Validate already-created Part 3 population reference CSV files."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from spark.prepare_population_reference import validate_population


def validate_file(path: Path, year: int) -> None:
    frame = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
    if "ประชากรรวม" in frame.columns:
        frame["ประชากรรวม"] = pd.to_numeric(
            frame["ประชากรรวม"], errors="coerce"
        )
    if "ปี" in frame.columns:
        frame["ปี"] = pd.to_numeric(frame["ปี"], errors="coerce")
    validate_population(frame, year, str(path))
    print(f"VALID: {path} ({len(frame)} districts)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, default=Path("."))
    args = parser.parse_args()
    reference = (
        args.project_root / "data" / "raw" / "disease" / "reference"
    ).resolve()
    found = 0
    for year in (2568, 2569):
        path = reference / f"population_summary_{year}.csv"
        if not path.is_file():
            print(f"MISSING: {path} (year {year} will use the latest available year)")
            continue
        validate_file(path, year)
        found += 1
    if found == 0:
        raise FileNotFoundError(f"No population_summary_*.csv in {reference}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
