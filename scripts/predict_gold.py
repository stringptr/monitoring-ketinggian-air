"""Write gold predictions from the LightGBM models of the modelling notebook.

The notebook saves ``lightgbm_<station>_h<horizon>.txt`` plus ``metadata.json``
in ``data/models``. This script loads the latest gold features, applies each
model, adds ``stage_now`` back (the models predict a delta) and writes
``gold/predictions``, which is the table the dashboard reads.

Rows with incomplete feature values are skipped: LightGBM accepts nulls, but a
forecast from a half-empty window is not a forecast the dashboard should show.
``issued_at`` is the feature timestamp, so ``target_time - issued_at`` is
exactly the model horizon. Partitions use the hive flavour so ``station_id``
and ``issue_date`` survive in the path and can be read back by the dashboard.

Run where ``lightgbm`` and the gold tables are reachable, for example inside
the worker:

    python /prefect/scripts/predict_gold.py --models-dir /prefect/models --days 7
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

for _candidate in (
    Path("/prefect"),
    Path(__file__).resolve().parents[1] / "prefect" / "app",
):
    if (_candidate / "gold").is_dir() and str(_candidate) not in sys.path:
        sys.path.insert(0, str(_candidate))
        break

from gold.config import GoldSettings
from gold.delta import read_delta, write_delta

MODEL_VERSION_FALLBACK = "local"


def _write_predictions(
    settings: GoldSettings, frame: pd.DataFrame
) -> int:
    issue_dates = pd.to_datetime(frame["issue_date"], utc=True)
    predicate = (
        f"issue_date >= '{issue_dates.min().date().isoformat()}' "
        f"AND issue_date <= '{issue_dates.max().date().isoformat()}'"
    )
    write_delta(
        frame,
        settings,
        "predictions",
        mode="overwrite",
        predicate=predicate,
        partition_by=["station_id", "issue_date"],
    )
    return len(frame)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models-dir", type=Path, default=Path("data/models"))
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--horizon", type=int, action="append", default=None)
    parser.add_argument("--station", action="append", default=None)
    args = parser.parse_args()

    import lightgbm as lgb

    settings = GoldSettings.from_env()
    metadata_path = args.models_dir / "metadata.json"
    if not metadata_path.exists():
        raise SystemExit(f"{metadata_path} not found; run the notebook first")
    metadata = json.loads(metadata_path.read_text())
    if args.station:
        metadata = [item for item in metadata if item["station"] in args.station]
    if args.horizon:
        metadata = [item for item in metadata if item["horizon"] in args.horizon]

    features = read_delta(settings, "features")
    features["time_utc"] = pd.to_datetime(features["time_utc"], utc=True)
    since = features["time_utc"].max() - timedelta(days=args.days)
    features = features[features["time_utc"] >= since]

    records: list[pd.DataFrame] = []
    for item in metadata:
        station = str(item["station"])
        horizon = int(item["horizon"])
        model_path = args.models_dir / item["file"]
        if not model_path.exists():
            raise SystemExit(f"{model_path} not found")

        part = features[features["station_id"].astype(str) == station]
        if part.empty:
            continue

        missing = [column for column in item["features"] if column not in part.columns]
        if missing:
            raise SystemExit(
                f"{station} h+{horizon}: gold is missing features {missing}"
            )

        # Do not issue a forecast from an incomplete feature row. LightGBM
        # accepts missing values, but a prediction from a half-empty window is
        # not a forecast the dashboard should display.
        part = part.dropna(subset=item["features"])
        if part.empty:
            continue

        booster = lgb.Booster(model_file=str(model_path))
        prediction = booster.predict(part[item["features"]])

        frame = pd.DataFrame(
            {
                "station_id": station,
                # The horizon is measured from the feature timestamp, so this
                # is the issue time. Adding one hour here silently shortened the
                # operational lead time by one hour.
                "issued_at": part["time_utc"].to_numpy(),
                "target_time": part["time_utc"].to_numpy() + timedelta(hours=horizon),
                "horizon": horizon,
                "y_pred": prediction + part["stage_now"].to_numpy(),
                "y_obs": part[f"y_h{horizon}"].to_numpy(),
                "model_version": str(item.get("dataset_version", MODEL_VERSION_FALLBACK)),
                "dataset_version": part["dataset_version"].astype(str).to_numpy(),
                "created_at": datetime.now(timezone.utc),
            }
        )
        frame["issue_date"] = pd.to_datetime(frame["issued_at"], utc=True).dt.strftime(
            "%Y-%m-%d"
        )
        frame["station_id"] = frame["station_id"].astype(str)
        records.append(frame.dropna(subset=["y_pred"]))

    if not records:
        print("no models matched the gold features")
        return 1

    combined = pd.concat(records, ignore_index=True)
    written = _write_predictions(settings, combined)
    print(f"wrote {written} prediction rows for {len(records)} model(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
