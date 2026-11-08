"""Helpers for writing raw JSON bronze partitions to the data lake.

Bronze stores the payload returned by the source APIs verbatim (no column
renames, no metadata columns). Partitions use the bare-value hierarchy:

* USGS: ``usgs/{daily|continuous}/parameter_code={code}/station_id={id}``
* Open-Meteo: ``open-meteo/station_id={id}``

Each ingestion run appends one ``ingest_date=YYYY-MM-DD`` partition containing a
single ``part-000.json``. Writes overwrite an existing file, so re-running the
same day is idempotent. When ``S3_ENDPOINT_URL`` is configured the partitions
are uploaded to the S3-compatible object store (Garage); otherwise they fall
back to a local directory so the flows stay runnable without the storage
backend.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from settings import Settings

PARTITION_PREFIX = "ingest_date="
PART_FILE = "part-000.json"


def incremental_start(today_iso: str) -> str:
    """Return the start date for an incremental fetch (last 7 days)."""

    return (datetime.fromisoformat(today_iso).date() - timedelta(days=7)).isoformat()


def usgs_prefix(station_site_no: str, data_type: str, parameter_code: str) -> str:
    """Relative partition prefix for a USGS parameter/station."""

    return (
        f"usgs/{data_type}/parameter_code={parameter_code}"
        f"/station_id={station_site_no}"
    )


def open_meteo_prefix(station_site_no: str) -> str:
    """Relative partition prefix for an Open-Meteo station."""

    return f"open-meteo/station_id={station_site_no}"


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


def _s3_filesystem(settings: Settings):
    import s3fs

    return s3fs.S3FileSystem(**_storage_options(settings))


def partition_exists(settings: Settings, prefix: str) -> bool:
    """Return ``True`` when any ``ingest_date=*`` partition exists below prefix.

    Existing data means the historical load already happened, so the next run
    only needs the rolling 7-day window.
    """

    if settings.s3_endpoint_url:
        filesystem = _s3_filesystem(settings)
        base = f"{settings.s3_bucket_bronze}/{prefix}"
        try:
            entries = filesystem.ls(base, detail=False)
        except FileNotFoundError:
            return False
        return any(
            Path(entry).name.startswith(PARTITION_PREFIX) for entry in entries
        )

    base_path = Path(settings.local_output_dir) / "bronze" / prefix
    if not base_path.is_dir():
        return False
    return any(
        entry.name.startswith(PARTITION_PREFIX) for entry in base_path.iterdir()
    )


def write_payload(
    settings: Settings,
    prefix: str,
    ingest_date: str,
    payload: dict,
) -> str:
    """Write ``payload`` as ``prefix/ingest_date=<date>/part-000.json``.

    Returns the written location (an ``s3://`` URI or a local path).
    """

    relative = f"{prefix}/{PARTITION_PREFIX}{ingest_date}/{PART_FILE}"
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    if settings.s3_endpoint_url:
        location = f"s3://{settings.s3_bucket_bronze}/{relative}"
        filesystem = _s3_filesystem(settings)
        with filesystem.open(location, "wb") as handle:
            handle.write(body.encode("utf-8"))
        return location

    path = Path(settings.local_output_dir) / "bronze" / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return str(path)
