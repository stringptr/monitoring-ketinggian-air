"""Environment driven configuration for the hydrology pipelines."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date

DEFAULT_START_DATE = "2023-01-01"
DEFAULT_LOCAL_DIR = "/prefect/data"


def _env(name: str, default: str | None = None) -> str | None:
    """Read an environment variable, treating the empty string as unset."""

    value = os.getenv(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    start_date: str
    end_date: str
    usgs_api_key: str | None
    s3_endpoint_url: str | None
    s3_region: str
    s3_bucket_bronze: str
    local_output_dir: str


def get_settings(
    start_date: str | None = None, end_date: str | None = None
) -> Settings:
    """Build settings from the environment, allowing the date range to be
    overridden per flow run."""

    return Settings(
        start_date=start_date or _env("HYDROLOGY_START_DATE", DEFAULT_START_DATE),
        end_date=end_date or _env("HYDROLOGY_END_DATE") or date.today().isoformat(),
        usgs_api_key=_env("USGS_API_KEY"),
        s3_endpoint_url=_env("S3_ENDPOINT_URL"),
        s3_region=_env("S3_REGION", "garage"),
        s3_bucket_bronze=_env("S3_BUCKET_BRONZE", "bronze"),
        local_output_dir=_env("BRONZE_LOCAL_DIR", DEFAULT_LOCAL_DIR),
    )
