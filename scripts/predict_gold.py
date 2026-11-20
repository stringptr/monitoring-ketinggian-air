"""Write gold predictions from the LightGBM models of the modelling notebook.

The notebook saves ``lightgbm_<station>_h<horizon>.txt`` plus ``metadata.json``
in ``data/models``. This script loads the latest gold features, applies each
model, adds ``stage_now`` back (the models predict a delta) and appends the
result to ``gold/predictions``, which is the table the dashboard reads.

Run where ``lightgbm`` is installed, for example inside the worker after
``pip install lightgbm``:

    python scripts/predict_gold.py --models-dir data/models --days 7
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

GOLD_BUCKET = os.getenv("S3_BUCKET_GOLD", "gold")
GOLD_LOCAL_DIR = os.getenv("GOLD_LOCAL_DIR", "/prefect/data/gold")
MODEL_VERSION_FALLBACK = "local"


def _dataset(name: str):
    import pyarrow.dataset as ds
    import pyarrow.fs as pafs
    from urllib.parse import urlparse

    endpoint = os.getenv("S3_ENDPOINT_URL", "").strip()
    if not endpoint:
        filesystem = pafs.LocalFileSystem()
        path = f"{GOLD_LOCAL_DIR}/{name}"
    else:
        parsed = urlparse(endpoint)
        filesystem = pafs.S3FileSystem(
            access_key=os.getenv("GARAGE_DEFAULT_ACCESS_KEY", ""),
            secret_key=os.getenv("GARAGE_DEFAULT_SECRET_KEY", ""),
            endpoint_override=parsed.netloc,
            scheme=parsed.scheme or "http",
            region=os.getenv("S3_REGION", "garage"),
        )
        path = f"{GOLD_BUCKET}/{name}"

    if filesystem.get_file_info(path).type == pafs.FileType.NotFound:
        raise SystemExit(f"{path} not found; run gold first")
    return ds.dataset(
        path, filesystem=filesystem, format="parquet", partitioning="hive"
    )


def _write_predictions(frame: pd.DataFrame) -> int:
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.fs as pafs
    from urllib.parse import urlparse

    table = pa.Table.from_pandas(frame, preserve_index=False)
    endpoint = os.getenv("S3_ENDPOINT_URL", "").strip()
    if not endpoint:
        filesystem = pafs.LocalFileSystem()
        root = f"{GOLD_LOCAL_DIR}/predictions"
    else:
        parsed = urlparse(endpoint)
        filesystem = pafs.S3FileSystem(
            access_key=os.getenv("GARAGE_DEFAULT_ACCESS_KEY", ""),
            secret_key=os.getenv("GARAGE_DEFAULT_SECRET_KEY", ""),
            endpoint_override=parsed.netloc,
            scheme=parsed.scheme or "http",
            region=os.getenv("S3_REGION", "garage"),
        )
        root = f"{GOLD_BUCKET}/predictions"

    ds.write_dataset(
        table,
        base_dir=root,
        filesystem=filesystem,
        format="parquet",
        partitioning=["station_id", "issue_date"],
        existing_data_behavior="overwrite_or_ignore",
    )
    return len(table)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models-dir", type=Path, default=Path("data/models"))
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--horizon", type=int, action="append", default=None)
    parser.add_argument("--station", action="append", default=None)
    args = parser.parse_args()

    import lightgbm as lgb

    metadata_path = args.models_dir / "metadata.json"
    if not metadata_path.exists():
        raise SystemExit(f"{metadata_path} not found; run the notebook first")
    metadata = json.loads(metadata_path.read_text())
    if args.station:
        metadata = [item for item in metadata if item["station"] in args.station]
    if args.horizon:
        metadata = [item for item in metadata if item["horizon"] in args.horizon]

    features = _dataset("features").to_table().to_pandas()
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

        booster = lgb.Booster(model_file=str(model_path))
        prediction = booster.predict(part[item["features"]])

        frame = pd.DataFrame(
            {
                "station_id": station,
                "issued_at": part["time_utc"].to_numpy() + timedelta(hours=1),
                "target_time": part["time_utc"].to_numpy() + timedelta(hours=horizon),
                "horizon": horizon,
                "y_pred": prediction + part["stage_now"].to_numpy(),
                "y_obs": part[f"y_h{horizon}"].to_numpy(),
                "model_version": str(item.get("dataset_version", MODEL_VERSION_FALLBACK)),
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
    written = _write_predictions(combined)
    print(f"wrote {written} prediction rows for {len(records)} model(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
