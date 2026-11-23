"""Readers that turn bronze JSON partitions into typed Spark DataFrames.

The readers keep the original observation granularity. They only flatten the
upstream JSON payload and attach the bronze ``ingest_date`` (read from the
partition path) so downstream cleaning can deduplicate and report DQ per
partition.
"""

from __future__ import annotations

from pathlib import Path

from datalake import existing_dates, open_meteo_prefix, usgs_prefix
from pyspark.sql import DataFrame, SparkSession
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

_INGEST_DATE_PATTERN = r"ingest_date=([0-9-]+)"


def available_dates(settings: SilverSettings, prefix: str) -> set[str]:
    """Return the ``ingest_date`` values that already have a bronze part file."""

    return existing_dates(settings, prefix)


def _bronze_base(settings: SilverSettings, prefix: str) -> str:
    if settings.s3_endpoint_url:
        return f"s3a://{settings.s3_bucket_bronze}/{prefix}"
    base = Path(settings.local_output_dir) / "bronze" / prefix
    return local_uri(base)


def _paths(settings: SilverSettings, prefix: str, days: list[str]) -> list[str]:
    base = _bronze_base(settings, prefix)
    return [f"{base}/ingest_date={day}/part-000.json" for day in days]


def continuous_prefix(station: Station, parameter_code: str = "00065") -> str:
    return usgs_prefix(station.site_no, "continuous", parameter_code)


def daily_prefix(station: Station) -> str:
    return usgs_prefix(station.site_no, "daily", "00060")


def meteo_prefix(station: Station) -> str:
    return open_meteo_prefix(station.site_no)


def read_usgs_continuous(
    spark: SparkSession,
    settings: SilverSettings,
    station: Station,
    days: list[str],
    parameter_code: str = "00065",
) -> DataFrame:
    """Read one raw USGS continuous parameter (00065 or 00060)."""

    if not days:
        return spark.createDataFrame([], CONTINUOUS_SOURCE_SCHEMA)

    raw = spark.read.schema(USGS_COLLECTION_SCHEMA).json(
        _paths(settings, continuous_prefix(station, parameter_code), days)
    )
    properties = (
        raw.withColumn("_file", F.input_file_name())
        .select(F.explode("features").alias("feature"), "_file")
        .select("feature.properties.*", "_file")
    )

    return properties.select(
        F.lit(station.site_no).cast("string").alias("station_id"),
        F.regexp_extract(F.col("_file"), _INGEST_DATE_PATTERN, 1).alias("ingest_date"),
        F.col("time_series_id").cast("string").alias("time_series_id"),
        F.col("monitoring_location_id").cast("string").alias("monitoring_location_id"),
        F.col("parameter_code").cast("string").alias("parameter_code"),
        F.col("statistic_id").cast("string").alias("statistic_id"),
        F.col("time").cast("string").alias("time_raw"),
        F.col("value").cast("string").alias("value_raw"),
        F.col("unit_of_measure").cast("string").alias("unit"),
        F.col("approval_status").cast("string").alias("approval_status"),
        F.col("qualifier").cast("string").alias("qualifier"),
        F.col("last_modified").cast("string").alias("last_modified_raw"),
        F.col("method_category").cast("string").alias("method_category"),
        F.to_timestamp(F.col("time")).alias("ts_utc"),
    )


def read_usgs_daily(
    spark: SparkSession,
    settings: SilverSettings,
    station: Station,
    days: list[str],
) -> DataFrame:
    """Read raw USGS daily discharge (00060) features."""

    if not days:
        return spark.createDataFrame([], USGS_DAILY_SOURCE_SCHEMA)

    raw = spark.read.schema(USGS_COLLECTION_SCHEMA).json(
        _paths(settings, daily_prefix(station), days)
    )
    properties = (
        raw.withColumn("_file", F.input_file_name())
        .select(F.explode("features").alias("feature"), "_file")
        .select("feature.properties.*", "_file")
    )

    return properties.select(
        F.lit(station.site_no).cast("string").alias("station_id"),
        F.regexp_extract(F.col("_file"), _INGEST_DATE_PATTERN, 1).alias("ingest_date"),
        F.col("time_series_id").cast("string").alias("time_series_id"),
        F.col("monitoring_location_id").cast("string").alias("monitoring_location_id"),
        F.col("parameter_code").cast("string").alias("parameter_code"),
        F.col("statistic_id").cast("string").alias("statistic_id"),
        F.col("time").cast("string").alias("date_raw"),
        F.col("value").cast("string").alias("value_raw"),
        F.col("unit_of_measure").cast("string").alias("unit"),
        F.col("approval_status").cast("string").alias("approval_status"),
        F.col("qualifier").cast("string").alias("qualifier"),
        F.col("last_modified").cast("string").alias("last_modified_raw"),
    )


def read_meteo(
    spark: SparkSession,
    settings: SilverSettings,
    station: Station,
    days: list[str],
) -> DataFrame:
    """Read raw Open-Meteo partitions and explode the hourly arrays."""

    if not days:
        return spark.createDataFrame([], METEO_SOURCE_SCHEMA)

    raw = spark.read.schema(METEO_COLLECTION_SCHEMA).json(
        _paths(settings, meteo_prefix(station), days)
    )

    arrays = [F.col("hourly.time").alias("time")] + [
        F.col(f"hourly.{variable}").alias(variable) for variable in METEO_VARIABLES
    ]
    zipped = F.arrays_zip(*arrays)

    exploded = raw.withColumn("_file", F.input_file_name()).select(
        F.lit(station.site_no).cast("string").alias("station_id"),
        F.regexp_extract(F.col("_file"), _INGEST_DATE_PATTERN, 1).alias("ingest_date"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("elevation").cast("double").alias("elevation"),
        F.col("timezone").cast("string").alias("timezone"),
        F.to_json(F.col("hourly_units")).alias("units_json"),
        F.explode(zipped).alias("z"),
    )

    return exploded.select(
        "station_id",
        "ingest_date",
        F.col("z.time").cast("string").alias("time_raw"),
        *[
            F.col(f"z.{variable}").cast("double").alias(variable)
            for variable in METEO_VARIABLES
        ],
        F.to_timestamp(F.col("z.time")).alias("ts_utc"),
        "units_json",
        "latitude",
        "longitude",
        "elevation",
        "timezone",
    )
