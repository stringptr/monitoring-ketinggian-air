"""Helpers for writing bronze partitions to the data lake.

Data is written as Parquet partitioned by ``station`` and ``year``. When
``S3_ENDPOINT_URL`` is configured the partitions are uploaded to the
S3-compatible object store (Garage); otherwise they fall back to a local
directory so the flows stay runnable without the storage backend.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from settings import Settings


def _storage_options(settings: Settings) -> dict:
    access_key = os.getenv("GARAGE_DEFAULT_ACCESS_KEY")
    secret_key = os.getenv("GARAGE_DEFAULT_SECRET_KEY")
    if not access_key or not secret_key:
        raise RuntimeError(
            "GARAGE_DEFAULT_ACCESS_KEY and GARAGE_DEFAULT_SECRET_KEY must be set "
            "to write to the S3 data lake"
        )
    return {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {
            "endpoint_url": settings.s3_endpoint_url,
            "region_name": settings.s3_region,
        },
    }


def write_bronze(
    df: pd.DataFrame,
    dataset: str,
    station: str,
    settings: Settings,
    time_column: str,
) -> list[str]:
    """Write ``df`` to the bronze bucket/dir partitioned by station and year.

    Returns the list of written locations. Existing files for a year are
    replaced, which makes re-running a range idempotent.
    """

    if df.empty:
        return []

    frame = df.copy()
    frame["_year"] = pd.to_datetime(frame[time_column]).dt.year

    locations: list[str] = []
    for year, chunk in frame.groupby("_year"):
        chunk = chunk.drop(columns="_year")
        relative = f"{dataset}/station={station}/year={int(year)}/data.parquet"

        if settings.s3_endpoint_url:
            location = f"s3://{settings.s3_bucket_bronze}/{relative}"
            chunk.to_parquet(
                location,
                index=False,
                engine="pyarrow",
                storage_options=_storage_options(settings),
            )
        else:
            path = Path(settings.local_output_dir) / "bronze" / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            chunk.to_parquet(path, index=False, engine="pyarrow")
            location = str(path)

        locations.append(location)

    return locations
