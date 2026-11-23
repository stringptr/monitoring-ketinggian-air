"""Read gold features and split them into train, validation and test."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd

from gold.schemas import FEATURE_COLS, TARGET_COLS
from training.config import TrainingSettings

BASE_COLUMNS = (
    "station_id",
    "time_utc",
    "water_year",
    "is_trainable",
    "is_partial",
    "stage_now",
    "discharge_grain",
    "dataset_version",
)


def selected_columns() -> list[str]:
    """Return the gold columns needed for training, without duplicates."""

    return list(
        dict.fromkeys(list(BASE_COLUMNS) + list(FEATURE_COLS) + list(TARGET_COLS))
    )


def _dataset(settings: TrainingSettings):
    import pyarrow.dataset as ds
    import pyarrow.fs as pafs
    from urllib.parse import urlparse

    endpoint = (settings.s3_endpoint_url or "").strip()
    if not endpoint:
        path = str(Path(settings.local_output_dir) / "gold" / "features")
        filesystem = pafs.LocalFileSystem()
    else:
        parsed = urlparse(endpoint)
        filesystem = pafs.S3FileSystem(
            access_key=settings.access_key,
            secret_key=settings.secret_key,
            endpoint_override=parsed.netloc,
            scheme=parsed.scheme or "http",
            region=settings.s3_region,
        )
        path = f"{settings.s3_bucket_gold}/features"

    if filesystem.get_file_info(path).type == pafs.FileType.NotFound:
        raise FileNotFoundError(f"{path} not found; run gold-backfill first")

    return ds.dataset(path, filesystem=filesystem, format="parquet", partitioning="hive")


def read_features(settings: TrainingSettings) -> pd.DataFrame:
    """Read the gold feature table as a pandas frame."""

    frame = _dataset(settings).to_table(columns=selected_columns()).to_pandas()
    frame["time_utc"] = pd.to_datetime(frame["time_utc"], utc=True)
    frame["station_id"] = frame["station_id"].astype(str)
    return frame


def _bounds(start: str, end: str, tz: str = "UTC") -> tuple[pd.Timestamp, pd.Timestamp]:
    return (
        pd.Timestamp(start, tz=tz),
        pd.Timestamp(end, tz=tz) + timedelta(days=1),
    )


def split_features(
    frame: pd.DataFrame,
    settings: TrainingSettings,
) -> dict[str, pd.DataFrame]:
    """Filter usable rows and split them chronologically by UTC date."""

    usable = frame[frame["is_trainable"]].copy()
    if not settings.include_partial:
        usable = usable[~usable["is_partial"]].copy()
    splits: dict[str, pd.DataFrame] = {}

    for name, start, end in (
        ("train", settings.train_start, settings.train_end),
        ("val", settings.val_start, settings.val_end),
        ("test", settings.test_start, settings.test_end),
    ):
        low, high = _bounds(start, end)
        splits[name] = usable[
            (usable["time_utc"] >= low) & (usable["time_utc"] < high)
        ].copy()

    return splits
