"""Configuration and small date helpers for the daily silver layer.

Values come from the same environment variables the bronze flows already use,
so no existing configuration file has to be rewritten. Anything the silver
layer adds has a sensible default and can be overridden with the ``SILVER_*``
variables documented in ``SILVER.md``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

#: The USGS daily values are reported for the local water day of the station.
TIMEZONE_NAME = "America/Los_Angeles"
TIMEZONE = ZoneInfo(TIMEZONE_NAME)

DEFAULT_HISTORIC_START_DATE = "2015-10-01"
DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_GAGE_COMPLETENESS_MIN = 0.90
DEFAULT_METEO_COMPLETENESS_MIN = 0.95
DEFAULT_MAX_GAP_HOURS = 3
DEFAULT_FRESHNESS_DAYS = 2

DEFAULT_LOCAL_DIR = "/prefect/data"

#: USGS continuous data is published every 15 minutes.
SAMPLES_PER_HOUR = 4
SECONDS_PER_SAMPLE = 3600 // SAMPLES_PER_HOUR

TABLE_USGS_DAILY = "usgs_daily"
TABLE_GAGE_DAILY = "usgs_gage_height_daily"
TABLE_METEO_DAILY = "meteo_daily"
TABLE_STATION_DAILY = "station_daily"

TABLE_DQ = "_dq"
TABLE_QUARANTINE = "_quarantine"

QUARANTINE_GAGE = "usgs_continuous_00065"
QUARANTINE_USGS_DAILY = "usgs_daily_00060"
QUARANTINE_METEO = "meteo_hourly"


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
    gage_completeness_min: float
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
            gage_completeness_min=_env_float(
                "SILVER_GAGE_COMPLETENESS_MIN",
                DEFAULT_GAGE_COMPLETENESS_MIN,
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


def expected_counts(day: str) -> tuple[int, int]:
    """Return ``(expected_samples, expected_hours)`` for a local day.

    Most days contain 96 quarter-hour samples and 24 hours. DST transition
    days contain 92/23 or 100/25, which keeps the completeness checks honest.
    """

    day_date = date.fromisoformat(day)
    start = datetime.combine(day_date, time.min, tzinfo=TIMEZONE)
    end = datetime.combine(day_date + timedelta(days=1), time.min, tzinfo=TIMEZONE)
    seconds = int(
        (end.astimezone(timezone.utc) - start.astimezone(timezone.utc)).total_seconds()
    )
    return seconds // SECONDS_PER_SAMPLE, seconds // 3600


def local_today(now: datetime | None = None) -> date:
    """Return the current date in the station timezone."""

    moment = now or datetime.now(timezone.utc)
    return moment.astimezone(TIMEZONE).date()


def default_end_date(now: datetime | None = None) -> str:
    """Return the last local day that is guaranteed to be complete.

    A local day can only be aggregated once the following midnight in
    ``America/Los_Angeles`` has passed, which is 07:00/08:00 UTC. Subtracting
    one day from the local date keeps the calculation correct on both sides
    of that boundary.
    """

    return (local_today(now) - timedelta(days=1)).isoformat()


def rolling_window(
    settings: SilverSettings,
    end_date: str | None = None,
) -> tuple[str, str]:
    """Return the inclusive local-date window processed by a daily run."""

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
