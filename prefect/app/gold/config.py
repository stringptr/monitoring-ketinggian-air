"""Configuration and date helpers for the gold layer.

Gold is the bridge between silver and the two consumers of the project:

* the model, which reads ``gold/features``;
* the dashboard, which reads ``gold/station_hourly`` and ``gold/predictions``.

The layer keeps the schema of the final dataset even while the data is still
partial. Columns whose source is not ingested yet (for example hourly
discharge) are written as ``null`` with an availability column, so the schema
does not change when the remaining data is ingested on Azure.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

TIMEZONE_NAME = "America/Los_Angeles"

DEFAULT_HISTORIC_START_DATE = "2015-10-01"
DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_LOCAL_DIR = "/prefect/data"

#: A USGS 15-minute series has four samples per hour; an hour counts as
#: complete when three of them are present.
STAGE_SAMPLES_PER_HOUR = 4
STAGE_COMPLETE_MIN_SAMPLES = 3

#: Longest feature window (``stage_lag_168``, ``precip_sum_168``) in hours.
FEATURE_LOOKBACK_HOURS = 168

#: Horizons predicted by the model, in hours.
TARGET_HORIZONS: tuple[int, ...] = (1, 6, 24)

TABLE_STATION_HOURLY = "station_hourly"
TABLE_FEATURES = "features"
TABLE_PREDICTIONS = "predictions"
TABLE_MODEL_METRICS = "model_metrics"
TABLE_DATASET_VERSIONS = "dataset_versions"
TABLE_DQ = "_dq"

#: Minimum share of rows that must be trainable, otherwise the DQ check fails.
DEFAULT_MIN_TRAINABLE_RATIO = 0.85

#: How the hourly discharge column is filled. ``auto`` prefers hourly silver
#: data and falls back to the daily table, recording which one was used.
DISCHARGE_SOURCES = ("auto", "hourly", "daily")


def _env(name: str, default: str | None = None) -> str | None:
    """Read an environment variable, treating the empty string as unset."""

    value = os.getenv(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    return int(raw) if raw is not None else default


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    return float(raw) if raw is not None else default


@dataclass(frozen=True)
class GoldSettings:
    """Environment driven settings for the gold flows."""

    historic_start_date: str
    lookback_days: int
    feature_lookback_hours: int
    min_trainable_ratio: float
    discharge_source: str
    s3_endpoint_url: str | None
    s3_region: str
    s3_bucket_silver: str
    s3_bucket_gold: str
    local_output_dir: str
    access_key: str | None
    secret_key: str | None

    @classmethod
    def from_env(cls) -> GoldSettings:
        return cls(
            historic_start_date=_env(
                "GOLD_HISTORIC_START_DATE",
                _env(
                    "SILVER_HISTORIC_START_DATE",
                    _env("USGS_HISTORIC_START_DATE", DEFAULT_HISTORIC_START_DATE),
                ),
            )
            or DEFAULT_HISTORIC_START_DATE,
            lookback_days=_env_int("GOLD_LOOKBACK_DAYS", DEFAULT_LOOKBACK_DAYS),
            feature_lookback_hours=_env_int(
                "GOLD_FEATURE_LOOKBACK_HOURS",
                FEATURE_LOOKBACK_HOURS,
            ),
            min_trainable_ratio=_env_float(
                "GOLD_MIN_TRAINABLE_RATIO",
                DEFAULT_MIN_TRAINABLE_RATIO,
            ),
            discharge_source=_env("GOLD_DISCHARGE_SOURCE", "auto") or "auto",
            s3_endpoint_url=_env("S3_ENDPOINT_URL"),
            s3_region=_env("S3_REGION", "garage") or "garage",
            s3_bucket_silver=_env("S3_BUCKET_SILVER", "silver") or "silver",
            s3_bucket_gold=_env("S3_BUCKET_GOLD", "gold") or "gold",
            local_output_dir=_env("BRONZE_LOCAL_DIR", DEFAULT_LOCAL_DIR)
            or DEFAULT_LOCAL_DIR,
            access_key=_env("GARAGE_DEFAULT_ACCESS_KEY"),
            secret_key=_env("GARAGE_DEFAULT_SECRET_KEY"),
        )


def default_end_date(now: datetime | None = None) -> str:
    """Return the last complete UTC day (yesterday UTC)."""

    moment = now or datetime.now(timezone.utc)
    return (moment.astimezone(timezone.utc).date() - timedelta(days=1)).isoformat()


def rolling_window(
    settings: GoldSettings,
    end_date: str | None = None,
) -> tuple[str, str]:
    """Return the inclusive UTC date window processed by a daily run."""

    end = end_date or default_end_date()
    start = (
        date.fromisoformat(end) - timedelta(days=max(settings.lookback_days, 1) - 1)
    ).isoformat()
    return start, end


def window_bounds(
    settings: GoldSettings,
    start_date: str,
    end_date: str,
) -> tuple[datetime, datetime]:
    """Return the UTC timestamps gold has to read and compute over.

    The lower bound is pulled back by the longest feature window, and the upper
    bound is pushed forward by the longest target horizon, so the first and
    last rows of the requested range have complete features and targets.
    """

    start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc) + timedelta(
        hours=23
    )
    return (
        start - timedelta(hours=settings.feature_lookback_hours),
        end + timedelta(hours=max(TARGET_HORIZONS)),
    )


def dataset_version(max_date_local: str | None, fallback: str) -> str:
    """Return a stable dataset version string (``YYYY-MM-DD``)."""

    return max_date_local or fallback


def local_uri(path: Path) -> str:
    """Return a Spark-friendly ``file:///`` URI without URL-encoding ``=``."""

    resolved = path.resolve().as_posix()
    return "file:///" + resolved.lstrip("/").replace(" ", "%20")
