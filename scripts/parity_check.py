"""Compare gold features with the notebook feature table.

Both tables must contain the same station and overlapping hours. The script
prints the maximum absolute difference per feature column, so a mismatch
introduced by a pipeline change is caught before a model is retrained.

Example:
    python scripts/parity_check.py \
        --gold data/gold/delta/features \
        --notebook data/processed/features_complete_11150500.parquet \
        --station 11150500
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

FEATURE_COLUMNS = (
    "stage_now",
    "stage_lag_1", "stage_lag_2", "stage_lag_3", "stage_lag_6", "stage_lag_12",
    "stage_lag_24", "stage_lag_48", "stage_lag_72", "stage_lag_168",
    "stage_mean_3", "stage_mean_6", "stage_mean_12", "stage_mean_24",
    "stage_mean_72", "stage_mean_168",
    "stage_max_3", "stage_max_6", "stage_max_12", "stage_max_24",
    "stage_max_72", "stage_max_168",
    "stage_diff_1", "stage_diff_3", "stage_diff_6", "stage_diff_24",
    "discharge_lag_1", "discharge_lag_3", "discharge_lag_6",
    "discharge_lag_24", "discharge_lag_72",
    "precip_sum_1", "precip_sum_3", "precip_sum_6", "precip_sum_24",
    "precip_sum_72", "precip_sum_168",
    "precip_sum_24_lag_24", "precip_sum_72_lag_24",
    "soil_moist_0_7", "soil_moist_7_28", "temp_c", "et0_mm", "rh_pct",
    "hour_sin", "hour_cos", "doy_sin", "doy_cos",
)

#: Columns that are expected to differ while discharge is only ingested daily.
KNOWN_DIFFERENCES = ("discharge_lag_1", "discharge_lag_3", "discharge_lag_6",
                     "discharge_lag_24", "discharge_lag_72")


def load_gold(path: Path, station: str) -> pd.DataFrame:
    try:
        from deltalake import DeltaTable

        frame = DeltaTable(str(path)).to_pandas()
    except Exception:
        frame = pd.read_parquet(path)
    if "station_id" in frame.columns:
        frame = frame[frame["station_id"].astype(str) == station]
    frame["time_utc"] = pd.to_datetime(frame["time_utc"], utc=True)
    return frame.set_index("time_utc").sort_index()


def load_notebook(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    frame.index = pd.to_datetime(frame.index, utc=True)
    frame.index.name = "time_utc"
    return frame.sort_index()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold", required=True, type=Path)
    parser.add_argument("--notebook", required=True, type=Path)
    parser.add_argument("--station", required=True)
    parser.add_argument("--tolerance", type=float, default=1e-6)
    args = parser.parse_args()

    gold = load_gold(args.gold, args.station)
    notebook = load_notebook(args.notebook)

    common = gold.index.intersection(notebook.index)
    if common.empty:
        print("no overlapping hours between gold and notebook", file=sys.stderr)
        return 2

    print(f"overlapping hours: {len(common)}")
    failures = []
    for column in FEATURE_COLUMNS:
        if column not in gold.columns or column not in notebook.columns:
            print(f"{column:24s} MISSING")
            failures.append(column)
            continue
        difference = (gold.loc[common, column] - notebook.loc[common, column]).abs()
        worst = float(difference.max())
        note = " (expected, discharge still daily)" if column in KNOWN_DIFFERENCES else ""
        print(f"{column:24s} max|diff| = {worst:.3e}{note}")
        if worst > args.tolerance and column not in KNOWN_DIFFERENCES:
            failures.append(column)

    if failures:
        print("\nparity FAILED on: " + ", ".join(failures))
        return 1
    print("\nparity OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
