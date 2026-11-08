"""Helpers for writing raw JSON bronze partitions to the data lake.

Bronze stores the payload returned by the source APIs verbatim (no column
renames, no metadata columns). Partitions use the bare-value hierarchy:

* USGS: ``usgs/{daily|continuous}/parameter_code={code}/station_id={id}``
* Open-Meteo: ``open-meteo/station_id={id}``

Every calendar day is stored as its own ``ingest_date=YYYY-MM-DD`` partition
containing a single ``part-000.json``. All dates are UTC. Writing a partition
is idempotent: existing files are skipped so a backfill can be stopped and
resumed later.

When ``S3_ENDPOINT_URL`` is configured the partitions are uploaded to the
S3-compatible object store (Garage); otherwise they fall back to a local
directory so the flows stay runnable without the storage backend.
"""

from __future__ import annotations

import json
import os
from datetime import date, timedelta
from pathlib import Path

from settings import Settings

PARTITION_PREFIX = "ingest_date="
PART_FILE = "part-000.json"


def date_range(start: str, end: str):
    """Yield every ISO date from ``start`` to ``end`` (inclusive)."""

    cursor = date.fromisoformat(start)
    stop = date.fromisoformat(end)
    while cursor <= stop:
        yield cursor.isoformat()
        cursor += timedelta(days=1)


def contiguous_runs(dates):
    """Yield ``(start, end)`` tuples for runs of consecutive ISO dates."""

    ordered = sorted(dates)
    if not ordered:
        return

    run_start = run_end = date.fromisoformat(ordered[0])
    for raw in ordered[1:]:
        current = date.fromisoformat(raw)
        if current == run_end + timedelta(days=1):
            run_end = current
            continue
        yield run_start.isoformat(), run_end.isoformat()
        run_start = run_end = current
    yield run_start.isoformat(), run_end.isoformat()


def year_chunks(start: str, end: str):
    """Yield calendar-year windows covering ``start..end``.

    The USGS ``continuous`` collection rejects ranges longer than 1100 days, so
    yearly windows keep every request safely below that limit.
    """

    cursor = date.fromisoformat(start)
    stop = date.fromisoformat(end)
    while cursor <= stop:
        chunk_end = min(date(cursor.year, 12, 31), stop)
        yield cursor.isoformat(), chunk_end.isoformat()
        cursor = chunk_end + timedelta(days=1)


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


_FILESYSTEMS: dict[tuple, object] = {}


def _s3_filesystem(settings: Settings):
    import s3fs

    cache_key = (
        settings.s3_endpoint_url,
        settings.s3_region,
        os.getenv("GARAGE_DEFAULT_ACCESS_KEY"),
        os.getenv("GARAGE_DEFAULT_SECRET_KEY"),
    )
    filesystem = _FILESYSTEMS.get(cache_key)
    if filesystem is None:
        filesystem = s3fs.S3FileSystem(**_storage_options(settings))
        _FILESYSTEMS[cache_key] = filesystem
    return filesystem


def _partition_dates_from_names(names) -> set[str]:
    dates: set[str] = set()
    for name in names:
        if name.startswith(PARTITION_PREFIX):
            dates.add(name[len(PARTITION_PREFIX) :])
    return dates


def existing_dates(settings: Settings, prefix: str) -> set[str]:
    """Return the set of ``ingest_date`` values that already have a part file."""

    if settings.s3_endpoint_url:
        filesystem = _s3_filesystem(settings)
        pattern = (
            f"{settings.s3_bucket_bronze}/{prefix}/"
            f"{PARTITION_PREFIX}*/{PART_FILE}"
        )
        paths = filesystem.glob(pattern)
        return _partition_dates_from_names(Path(path).parts[-2] for path in paths)

    base_path = Path(settings.local_output_dir) / "bronze" / prefix
    if not base_path.is_dir():
        return set()
    paths = base_path.glob(f"{PARTITION_PREFIX}*/{PART_FILE}")
    return _partition_dates_from_names(path.parent.name for path in paths)


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