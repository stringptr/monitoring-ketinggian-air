"""Configuration and date helpers for the native-resolution silver layer.

Silver keeps the original observation resolution: USGS 15-minute gage height,
USGS daily discharge and Open-Meteo hourly weather. It only deduplicates,
cleans, adds local-time columns, validates the contract and reports data
quality. No aggregation, resampling, imputation or cross-source join happens
here; those belong to gold/modelling.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

TIMEZONE_NAME = "America/Los_Angeles"

DEFAULT_HISTORIC_START_DATE = "2015-10-01"
#: Silver must cover the gold window plus gold's 168-hour feature lookback.
#: The daily gold window is 7 days, so silver reprocesses 14 days.
DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_USGS_COMPLETENESS_MIN = 0.90
DEFAULT_METEO_COMPLETENESS_MIN = 0.95
DEFAULT_MAX_GAP_HOURS = 3
DEFAULT_FRESHNESS_DAYS = 2

DEFAULT_LOCAL_DIR = "/prefect/data"

EXPECTED_USGS_SAMPLES_PER_DAY = 96
EXPECTED_METEO_HOURS_PER_DAY = 24
EXPECTED_USGS_DAILY_VALUES = 1

TABLE_USGS_CONTINUOUS = "usgs_continuous_00065"
TABLE_USGS_DAILY = "usgs_daily_00060"
TABLE_METEO_HOURLY = "open_meteo_hourly"

PATH_USGS_CONTINUOUS = "usgs/continuous/parameter_code=00065"
PATH_USGS_DAILY = "usgs/daily/parameter_code=00060"
PATH_METEO_HOURLY = "open-meteo"

TABLE_DQ = "_dq"
TABLE_QUARANTINE = "_quarantine"


def _env(name: str, default: str | None = None) -> str | None:
    """Read an environment variable, treating the empty string as unset."""

    value = os.getenv(name)
    return value if value not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    return float(raw) if raw is not None else default


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    return int(raw) if raw is not None else default


@dataclass(frozen=True)
class SilverSettings:
    """Environment driven settings for the silver flows."""

    historic_start_date: str
    lookback_days: int
    s3_endpoint_url: str | None
    s3_region: str
    s3_bucket_bronze: str
    s3_bucket_silver: str
    local_output_dir: str
    usgs_completeness_min: float
    meteo_completeness_min: float
    max_gap_hours: int
    freshness_days: int
    access_key: str | None
    secret_key: str | None

    @classmethod
    def from_env(cls) -> SilverSettings:
        return cls(
            historic_start_date=_env(
                "SILVER_HISTORIC_START_DATE",
                _env(
                    "USGS_HISTORIC_START_DATE",
                    DEFAULT_HISTORIC_START_DATE,
                ),
            )
            or DEFAULT_HISTORIC_START_DATE,
            lookback_days=_env_int("SILVER_LOOKBACK_DAYS", DEFAULT_LOOKBACK_DAYS),
            s3_endpoint_url=_env("S3_ENDPOINT_URL"),
            s3_region=_env("S3_REGION", "garage") or "garage",
            s3_bucket_bronze=_env("S3_BUCKET_BRONZE", "bronze") or "bronze",
            s3_bucket_silver=_env("S3_BUCKET_SILVER", "silver") or "silver",
            local_output_dir=_env("BRONZE_LOCAL_DIR", DEFAULT_LOCAL_DIR)
            or DEFAULT_LOCAL_DIR,
            usgs_completeness_min=_env_float(
                "SILVER_USGS_COMPLETENESS_MIN",
                _env_float(
                    "SILVER_GAGE_COMPLETENESS_MIN",
                    DEFAULT_USGS_COMPLETENESS_MIN,
                ),
            ),
            meteo_completeness_min=_env_float(
                "SILVER_METEO_COMPLETENESS_MIN",
                DEFAULT_METEO_COMPLETENESS_MIN,
            ),
            max_gap_hours=_env_int("SILVER_MAX_GAP_HOURS", DEFAULT_MAX_GAP_HOURS),
            freshness_days=_env_int(
                "SILVER_FRESHNESS_DAYS",
                DEFAULT_FRESHNESS_DAYS,
            ),
            access_key=_env("GARAGE_DEFAULT_ACCESS_KEY"),
            secret_key=_env("GARAGE_DEFAULT_SECRET_KEY"),
        )


def default_end_date(now: datetime | None = None) -> str:
    """Return the last complete UTC ingest day (yesterday UTC).

    Silver processes bronze partitions, which are keyed by UTC observation
    day. The bronze run at 02:00 UTC has already completed yesterday UTC, so
    yesterday is the natural end of the daily window.
    """

    moment = now or datetime.now(timezone.utc)
    return (moment.astimezone(timezone.utc).date() - timedelta(days=1)).isoformat()


def rolling_window(
    settings: SilverSettings,
    end_date: str | None = None,
) -> tuple[str, str]:
    """Return the inclusive UTC ingest-date window processed by a daily run."""

    end = end_date or default_end_date()
    start = (
        date.fromisoformat(end) - timedelta(days=max(settings.lookback_days, 1) - 1)
    ).isoformat()
    return start, end


def next_day(day: str) -> str:
    return (date.fromisoformat(day) + timedelta(days=1)).isoformat()


def local_uri(path: Path) -> str:
    """Return a Spark-friendly ``file:///`` URI without URL-encoding ``=``.

    ``Path.as_uri()`` percent-encodes ``=`` in Hive-style partition names
    (``ingest_date=...``), which Spark then treats as a literal character.
    """

    resolved = path.resolve().as_posix()
    return "file:///" + resolved.lstrip("/").replace(" ", "%20")
