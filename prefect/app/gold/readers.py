"""Read silver Delta tables into Spark DataFrames for gold assembly.

Spark Delta cannot read Garage tables because S3A turns a missing
``_delta_log`` into HTTP 400. The readers therefore use ``delta-rs`` and convert
the small window to Spark. On Azure, the same tables can be read by Spark
directly.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from common.delta import read_delta
from gold.config import GoldSettings
from gold.schemas import METEO_COLUMNS, METEO_SOURCE_MAP
from stations import Station

SILVER_CONTINUOUS_00065 = "usgs/continuous/parameter_code=00065"
SILVER_CONTINUOUS_00060 = "usgs/continuous/parameter_code=00060"
SILVER_DAILY_00060 = "usgs/daily/parameter_code=00060"
SILVER_METEO = "open-meteo"


def _read_silver(
    settings: GoldSettings,
    table: str,
    *,
    columns: list[str] | None = None,
    filters: list[tuple[str, str, object]] | None = None,
) -> pd.DataFrame:
    try:
        return read_delta(
            settings,
            table,
            columns=columns,
            filters=filters,
            bucket_attr="s3_bucket_silver",
            root="silver",
        )
    except Exception:
        return pd.DataFrame()


def _utc_value(value: datetime) -> datetime:
    """Return a naive UTC datetime for a pyarrow timestamp filter."""

    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _empty(spark, schema: str):
    return spark.createDataFrame([], schema)


def read_stage(
    spark,
    settings: GoldSettings,
    station: Station,
    start: datetime,
    end: datetime,
):
    """Read 15-minute gage height and rename the value column."""

    frame = _read_silver(
        settings,
        SILVER_CONTINUOUS_00065,
        columns=["station_id", "time_utc", "value"],
        filters=[
            ("station_id", "=", station.site_no),
            ("time_utc", ">=", _utc_value(start)),
            ("time_utc", "<=", _utc_value(end)),
        ],
    )
    if frame.empty:
        return _empty(
            spark, "station_id string, time_utc timestamp, stage_ft double"
        )

    frame["time_utc"] = pd.to_datetime(frame["time_utc"], utc=True).dt.tz_localize(
        None
    )
    frame = frame[frame["value"].notna()].copy()
    frame["stage_ft"] = frame["value"].astype(float)
    return spark.createDataFrame(frame[["station_id", "time_utc", "stage_ft"]])


def read_discharge_hourly(
    spark,
    settings: GoldSettings,
    station: Station,
    start: datetime,
    end: datetime,
):
    """Read continuous discharge (15 minute) when the table has rows."""

    frame = _read_silver(
        settings,
        SILVER_CONTINUOUS_00060,
        columns=["station_id", "time_utc", "value"],
        filters=[
            ("station_id", "=", station.site_no),
            ("time_utc", ">=", _utc_value(start)),
            ("time_utc", "<=", _utc_value(end)),
        ],
    )
    if frame.empty:
        return None

    frame["time_utc"] = pd.to_datetime(frame["time_utc"], utc=True).dt.tz_localize(
        None
    )
    frame = frame[frame["value"].notna()].copy()
    frame["discharge_cfs"] = frame["value"].astype(float)
    return spark.createDataFrame(
        frame[["station_id", "time_utc", "discharge_cfs"]]
    )


def read_discharge_daily(
    spark,
    settings: GoldSettings,
    station: Station,
    start_date: str,
    end_date: str,
):
    """Read the native daily discharge table."""

    frame = _read_silver(
        settings,
        SILVER_DAILY_00060,
        columns=["station_id", "date_local", "value"],
        filters=[
            ("station_id", "=", station.site_no),
            ("date_local", ">=", start_date),
            ("date_local", "<=", end_date),
        ],
    )
    if frame.empty:
        return _empty(
            spark, "station_id string, date_local string, discharge_cfs double"
        )

    frame = frame[frame["value"].notna()].copy()
    frame["discharge_cfs"] = frame["value"].astype(float)
    return spark.createDataFrame(
        frame[["station_id", "date_local", "discharge_cfs"]]
    )


def read_meteo(
    spark,
    settings: GoldSettings,
    station: Station,
    start: datetime,
    end: datetime,
):
    """Read hourly weather and map the silver names to canonical names."""

    frame = _read_silver(
        settings,
        SILVER_METEO,
        filters=[
            ("station_id", "=", station.site_no),
            ("time_utc", ">=", _utc_value(start)),
            ("time_utc", "<=", _utc_value(end)),
        ],
    )
    if frame.empty:
        return (
            _empty(
                spark,
                "station_id string, time_utc timestamp, "
                + ", ".join(f"{column} double" for column in METEO_COLUMNS),
            ),
            (),
        )

    frame["time_utc"] = pd.to_datetime(frame["time_utc"], utc=True).dt.tz_localize(
        None
    )
    available = tuple(
        target for source, target in METEO_SOURCE_MAP.items() if source in frame.columns
    )
    frame = frame.rename(columns=METEO_SOURCE_MAP)
    for column in METEO_COLUMNS:
        if column not in frame.columns:
            frame[column] = None

    selected = frame[["station_id", "time_utc", *METEO_COLUMNS]].copy()
    schema = "station_id string, time_utc timestamp, " + ", ".join(
        f"{column} double" for column in METEO_COLUMNS
    )
    return spark.createDataFrame(selected, schema=schema), available
