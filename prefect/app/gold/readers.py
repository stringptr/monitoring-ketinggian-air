"""Readers that turn the silver Parquet partitions into typed DataFrames.

Gold only reads silver; it never touches bronze or the upstream APIs. Because a
silver table holds more than a thousand daily partitions, the readers resolve
the partition directories first and pass only the days inside the requested
window to Spark. Reading the whole table and filtering afterwards makes the
job wait on thousands of S3 requests before it computes anything.

Each reader is tolerant of missing tables: while the local prototype still
lacks hourly discharge or a weather variable, the column is returned as
``null`` instead of failing the flow, so the final schema can be defined now
and filled in later.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from stations import Station

from gold.config import GoldSettings, local_uri
from gold.schemas import METEO_COLUMNS, METEO_SOURCE_MAP

SILVER_PREFIX_CONTINUOUS = "usgs/continuous/parameter_code=00065"
SILVER_PREFIX_CONTINUOUS_DISCHARGE = "usgs/continuous/parameter_code=00060"
SILVER_PREFIX_DAILY = "usgs/daily/parameter_code=00060"
SILVER_PREFIX_METEO = "open-meteo"

_PARTITION_PREFIX = "ingest_date="
_S3_CLIENTS: dict[tuple, object] = {}
_DAY_CACHE: dict[tuple, set[str]] = {}


def _s3_filesystem(settings: GoldSettings):
    key = (
        settings.s3_endpoint_url,
        settings.s3_region,
        settings.access_key,
        settings.secret_key,
    )
    filesystem = _S3_CLIENTS.get(key)
    if filesystem is None:
        import s3fs

        filesystem = s3fs.S3FileSystem(
            key=settings.access_key,
            secret=settings.secret_key,
            client_kwargs={
                "endpoint_url": settings.s3_endpoint_url,
                "region_name": settings.s3_region,
            },
        )
        _S3_CLIENTS[key] = filesystem
    return filesystem


def _s3_table_key(settings: GoldSettings, prefix: str, station: Station) -> str:
    return f"{settings.s3_bucket_silver}/{prefix}/station_id={station.site_no}"


def _local_table_path(
    settings: GoldSettings, prefix: str, station: Station
) -> Path:
    return (
        Path(settings.local_output_dir)
        / "silver"
        / prefix
        / f"station_id={station.site_no}"
    )


def _existing_days(
    settings: GoldSettings,
    prefix: str,
    station: Station,
) -> set[str]:
    """List the ``ingest_date`` partitions that exist for one station."""

    key = (
        settings.s3_endpoint_url,
        settings.s3_bucket_silver,
        settings.local_output_dir,
        prefix,
        station.site_no,
    )
    cached = _DAY_CACHE.get(key)
    if cached is not None:
        return cached

    days: set[str] = set()
    if settings.s3_endpoint_url:
        filesystem = _s3_filesystem(settings)
        try:
            entries = filesystem.ls(
                _s3_table_key(settings, prefix, station), detail=False
            )
        except FileNotFoundError:
            entries = []
        for entry in entries:
            name = str(entry).rsplit("/", 1)[-1]
            if name.startswith(_PARTITION_PREFIX):
                days.add(name[len(_PARTITION_PREFIX) :])
    else:
        base = _local_table_path(settings, prefix, station)
        if base.is_dir():
            for child in base.iterdir():
                if child.name.startswith(_PARTITION_PREFIX):
                    days.add(child.name[len(_PARTITION_PREFIX) :])

    _DAY_CACHE[key] = days
    return days


def _day_range(start: datetime, end: datetime) -> list[str]:
    days: list[str] = []
    cursor = start.date()
    stop = end.date()
    while cursor <= stop:
        days.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return days


def _paths(
    settings: GoldSettings,
    prefix: str,
    station: Station,
    days: list[str],
) -> list[str]:
    present = _existing_days(settings, prefix, station)
    wanted = [day for day in days if day in present]
    if not wanted:
        return []

    if settings.s3_endpoint_url:
        base = f"s3a://{_s3_table_key(settings, prefix, station)}"
    else:
        base = local_uri(_local_table_path(settings, prefix, station))

    return [
        f"{base}/{_PARTITION_PREFIX}{day}/*.parquet"
        for day in wanted
    ]


def _read(spark: SparkSession, paths: list[str]) -> DataFrame | None:
    if not paths:
        return None
    try:
        return spark.read.parquet(*paths)
    except AnalysisException:
        return None


def _empty(spark: SparkSession, schema: str) -> DataFrame:
    return spark.createDataFrame([], schema)


def read_stage(
    spark: SparkSession,
    settings: GoldSettings,
    station: Station,
    start: datetime,
    end: datetime,
) -> DataFrame:
    """Read 15-minute gage height and rename the value column."""

    paths = _paths(
        settings, SILVER_PREFIX_CONTINUOUS, station, _day_range(start, end)
    )
    df = _read(spark, paths)
    if df is None:
        return _empty(spark, "station_id string, time_utc timestamp, stage_ft double")

    return df.select(
        F.lit(station.site_no).cast("string").alias("station_id"),
        F.col("time_utc"),
        F.col("value").cast("double").alias("stage_ft"),
    ).filter(
        (F.col("time_utc") >= F.lit(start))
        & (F.col("time_utc") <= F.lit(end))
        & F.col("stage_ft").isNotNull()
    )


def read_discharge_hourly(
    spark: SparkSession,
    settings: GoldSettings,
    station: Station,
    start: datetime,
    end: datetime,
) -> DataFrame | None:
    """Read continuous discharge (15 minute) when it has been ingested.

    The current pipeline stores discharge only from the daily collection, so
    this returns ``None`` until ``00060`` continuous is added to bronze.
    """

    paths = _paths(
        settings,
        SILVER_PREFIX_CONTINUOUS_DISCHARGE,
        station,
        _day_range(start, end),
    )
    df = _read(spark, paths)
    if df is None:
        return None

    return df.select(
        F.lit(station.site_no).cast("string").alias("station_id"),
        F.col("time_utc"),
        F.col("value").cast("double").alias("discharge_cfs"),
    ).filter(
        (F.col("time_utc") >= F.lit(start))
        & (F.col("time_utc") <= F.lit(end))
        & F.col("discharge_cfs").isNotNull()
    )


def read_discharge_daily(
    spark: SparkSession,
    settings: GoldSettings,
    station: Station,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """Read the native daily discharge table."""

    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)
    paths = _paths(settings, SILVER_PREFIX_DAILY, station, _day_range(start, end))
    df = _read(spark, paths)
    if df is None:
        return _empty(
            spark, "station_id string, date_local string, discharge_cfs double"
        )

    return df.select(
        F.lit(station.site_no).cast("string").alias("station_id"),
        F.col("date_local").cast("string").alias("date_local"),
        F.col("value").cast("double").alias("discharge_cfs"),
    ).filter(
        (F.col("date_local") >= F.lit(start_date))
        & (F.col("date_local") <= F.lit(end_date))
    )


def read_meteo(
    spark: SparkSession,
    settings: GoldSettings,
    station: Station,
    start: datetime,
    end: datetime,
) -> DataFrame:
    """Read hourly weather and map the silver names to the canonical names."""

    paths = _paths(settings, SILVER_PREFIX_METEO, station, _day_range(start, end))
    df = _read(spark, paths)
    if df is None:
        return _empty(
            spark,
            "station_id string, time_utc timestamp, "
            + ", ".join(f"{column} double" for column in METEO_COLUMNS),
        )

    filtered = df.filter(
        (F.col("time_utc") >= F.lit(start)) & (F.col("time_utc") <= F.lit(end))
    )

    selections = [
        F.lit(station.site_no).cast("string").alias("station_id"),
        F.col("time_utc"),
    ]
    for source, target in METEO_SOURCE_MAP.items():
        selections.append(
            F.col(source).cast("double").alias(target)
            if source in filtered.columns
            else F.lit(None).cast("double").alias(target)
        )

    return filtered.select(*selections)
