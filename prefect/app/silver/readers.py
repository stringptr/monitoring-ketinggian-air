"""Readers that turn bronze JSON partitions into typed Spark DataFrames."""

from __future__ import annotations

from pathlib import Path

from datalake import existing_dates, open_meteo_prefix, usgs_prefix
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from stations import Station

from silver.config import SilverSettings, local_uri
from silver.schemas import (
    CONTINUOUS_SOURCE_SCHEMA,
    METEO_COLLECTION_SCHEMA,
    METEO_SOURCE_SCHEMA,
    METEO_VARIABLES,
    USGS_COLLECTION_SCHEMA,
    USGS_DAILY_SOURCE_SCHEMA,
)


def available_dates(settings: SilverSettings, prefix: str) -> set[str]:
    """Return the ``ingest_date`` values that already have a bronze part file.

    ``datalake.existing_dates`` only relies on the storage attributes shared by
    both settings objects, so it can be reused without touching bronze code.
    """

    return existing_dates(settings, prefix)  # type: ignore[arg-type]


def _bronze_base(settings: SilverSettings, prefix: str) -> str:
    if settings.s3_endpoint_url:
        return f"s3a://{settings.s3_bucket_bronze}/{prefix}"
    base = Path(settings.local_output_dir) / "bronze" / prefix
    return local_uri(base)


def _paths(settings: SilverSettings, prefix: str, days: list[str]) -> list[str]:
    base = _bronze_base(settings, prefix)
    return [f"{base}/ingest_date={day}/part-000.json" for day in days]


def continuous_prefix(station: Station) -> str:
    return usgs_prefix(station.site_no, "continuous", "00065")


def daily_prefix(station: Station) -> str:
    return usgs_prefix(station.site_no, "daily", "00060")


def meteo_prefix(station: Station) -> str:
    return open_meteo_prefix(station.site_no)


def _dedupe_latest(df: DataFrame, keys: list[str]) -> DataFrame:
    window = Window.partitionBy(*keys).orderBy(F.col("last_modified").desc_nulls_last())
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def read_usgs_continuous(
    spark: SparkSession,
    settings: SilverSettings,
    station: Station,
    days: list[str],
) -> DataFrame:
    """Read USGS gage-height (00065) continuous partitions for a station."""

    if not days:
        return spark.createDataFrame([], CONTINUOUS_SOURCE_SCHEMA)

    raw = spark.read.schema(USGS_COLLECTION_SCHEMA).json(
        _paths(settings, continuous_prefix(station), days)
    )
    properties = raw.select(F.explode("features").alias("feature")).select(
        "feature.properties.*"
    )

    df = properties.select(
        F.lit(station.site_no).cast("string").alias("station_id"),
        F.col("time_series_id").cast("string").alias("time_series_id"),
        F.col("parameter_code").cast("string").alias("parameter_code"),
        F.col("statistic_id").cast("string").alias("statistic_id"),
        F.col("time").cast("string").alias("time_raw"),
        F.col("value").cast("string").alias("value_raw"),
        F.col("unit_of_measure").cast("string").alias("unit"),
        F.col("approval_status").cast("string").alias("approval_status"),
        F.col("qualifier").cast("string").alias("qualifier"),
        F.col("last_modified").cast("string").alias("last_modified"),
        F.to_timestamp(F.col("time")).alias("ts_utc"),
    )

    return _dedupe_latest(df, ["station_id", "parameter_code", "time_raw"])


def read_usgs_daily(
    spark: SparkSession,
    settings: SilverSettings,
    station: Station,
    days: list[str],
) -> DataFrame:
    """Read USGS daily discharge (00060) partitions for a station."""

    if not days:
        return spark.createDataFrame([], USGS_DAILY_SOURCE_SCHEMA)

    raw = spark.read.schema(USGS_COLLECTION_SCHEMA).json(
        _paths(settings, daily_prefix(station), days)
    )
    properties = raw.select(F.explode("features").alias("feature")).select(
        "feature.properties.*"
    )

    df = properties.select(
        F.lit(station.site_no).cast("string").alias("station_id"),
        F.date_format(F.to_date(F.col("time")), "yyyy-MM-dd").alias("date"),
        F.col("parameter_code").cast("string").alias("parameter_code"),
        F.col("value").cast("string").alias("value_raw"),
        F.col("unit_of_measure").cast("string").alias("unit"),
        F.col("statistic_id").cast("string").alias("statistic_id"),
        F.col("approval_status").cast("string").alias("approval_status"),
        F.col("last_modified").cast("string").alias("last_modified"),
    )

    return _dedupe_latest(df, ["station_id", "date", "parameter_code"])


def read_meteo(
    spark: SparkSession,
    settings: SilverSettings,
    station: Station,
    days: list[str],
) -> DataFrame:
    """Read Open-Meteo hourly partitions and explode the hourly arrays."""

    if not days:
        return spark.createDataFrame([], METEO_SOURCE_SCHEMA)

    raw = spark.read.schema(METEO_COLLECTION_SCHEMA).json(
        _paths(settings, meteo_prefix(station), days)
    )

    arrays = [F.col("hourly.time").alias("time")] + [
        F.col(f"hourly.{variable}").alias(variable) for variable in METEO_VARIABLES
    ]
    zipped = F.arrays_zip(*arrays)

    return raw.select(F.explode(zipped).alias("z")).select(
        F.lit(station.site_no).cast("string").alias("station_id"),
        F.col("z.time").cast("string").alias("time_raw"),
        *[
            F.col(f"z.{variable}").cast("double").alias(variable)
            for variable in METEO_VARIABLES
        ],
        F.to_timestamp(F.col("z.time")).alias("ts_utc"),
    )
