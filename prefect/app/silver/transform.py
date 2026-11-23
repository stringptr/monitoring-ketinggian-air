"""Cleaning primitives for the native-resolution silver layer.

These functions never aggregate or join. They deduplicate, cast, parse
timestamps, add local-time columns, and split unusable rows into quarantine.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from silver.config import TIMEZONE_NAME
from silver.schemas import METEO_RANGES, METEO_VARIABLES


def empty_frame(spark: SparkSession, schema) -> DataFrame:
    return spark.createDataFrame([], schema)


def dedupe_latest(df: DataFrame, keys: list[str]) -> DataFrame:
    """Keep the newest revision per natural key using ``last_modified_raw``."""

    window = Window.partitionBy(*keys).orderBy(
        F.col("last_modified_raw").desc_nulls_last()
    )
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def _invalid_ingest_date() -> F.Column:
    return F.col("ingest_date").isNull() | (F.col("ingest_date") == "")


def _add_local_time(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("time_utc", F.col("ts_utc"))
        .withColumn("time_local", F.from_utc_timestamp(F.col("ts_utc"), TIMEZONE_NAME))
        .withColumn("date_local", F.date_format(F.col("time_local"), "yyyy-MM-dd"))
    )


def clean_continuous(spark: SparkSession, df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Deduplicate and clean 15-minute gage-height features."""

    latest = dedupe_latest(df, ["station_id", "parameter_code", "time_raw"])
    prepared = (
        latest.withColumn("value", F.col("value_raw").cast("double"))
        .withColumn("last_modified", F.to_timestamp(F.col("last_modified_raw")))
        .transform(_add_local_time)
    )

    invalid = (
        _invalid_ingest_date()
        | F.col("time_raw").isNull()
        | F.col("ts_utc").isNull()
        | (F.col("value_raw").isNotNull() & F.col("value").isNull())
    )

    quarantine = prepared.filter(invalid).select(
        "station_id",
        "ingest_date",
        "time_raw",
        "parameter_code",
        "value_raw",
        "unit",
        F.when(_invalid_ingest_date(), F.lit("invalid ingest_date"))
        .when(
            F.col("time_raw").isNull() | F.col("ts_utc").isNull(),
            F.lit("invalid timestamp"),
        )
        .otherwise(F.lit("non-numeric value"))
        .alias("reason"),
    )

    valid = prepared.filter(~invalid).select(
        "station_id",
        "ingest_date",
        "time_utc",
        "time_local",
        "date_local",
        "time_series_id",
        "monitoring_location_id",
        "parameter_code",
        "statistic_id",
        "value",
        "unit",
        "approval_status",
        "qualifier",
        "last_modified",
        "method_category",
    )

    return valid, quarantine


def clean_usgs_daily(spark: SparkSession, df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Deduplicate and clean USGS daily discharge features."""

    latest = dedupe_latest(df, ["station_id", "parameter_code", "date_raw"])
    prepared = (
        latest.withColumn("value", F.col("value_raw").cast("double"))
        .withColumn(
            "date_local",
            F.date_format(F.to_date(F.col("date_raw")), "yyyy-MM-dd"),
        )
        .withColumn("last_modified", F.to_timestamp(F.col("last_modified_raw")))
    )

    invalid = (
        _invalid_ingest_date()
        | F.col("date_local").isNull()
        | (
            F.col("value_raw").isNotNull()
            & (F.col("value").isNull() | (F.col("value") < 0))
        )
    )

    quarantine = prepared.filter(invalid).select(
        "station_id",
        "ingest_date",
        "date_raw",
        "parameter_code",
        "value_raw",
        F.when(_invalid_ingest_date(), F.lit("invalid ingest_date"))
        .when(F.col("date_local").isNull(), F.lit("invalid date"))
        .when(F.col("value").isNull(), F.lit("non-numeric value"))
        .otherwise(F.lit("negative value"))
        .alias("reason"),
    )

    valid = prepared.filter(~invalid).select(
        "station_id",
        "ingest_date",
        "time_series_id",
        "monitoring_location_id",
        "parameter_code",
        "statistic_id",
        "date_local",
        "value",
        "unit",
        "approval_status",
        "qualifier",
        "last_modified",
    )

    return valid, quarantine


def clean_meteo(spark: SparkSession, df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Clean hourly Open-Meteo values without changing the hourly grain."""

    prepared = _add_local_time(df)

    invalid_timestamp = (
        _invalid_ingest_date() | F.col("time_raw").isNull() | F.col("ts_utc").isNull()
    )

    valid_timestamp = prepared.filter(~invalid_timestamp)

    value_columns = []
    violations = []
    for variable in METEO_VARIABLES:
        low, high = METEO_RANGES[variable]
        column = F.col(variable)
        in_range = (column >= low) & (column <= high)

        value_columns.append(
            F.when(in_range, column)
            .otherwise(F.lit(None).cast("double"))
            .alias(variable)
        )
        violations.append(
            valid_timestamp.filter(column.isNotNull() & ~in_range).select(
                "station_id",
                "ingest_date",
                "time_raw",
                "date_local",
                F.lit(variable).alias("variable"),
                column.alias("value"),
                F.lit(f"out of range [{low}, {high}]").alias("reason"),
            )
        )

    violations.append(
        prepared.filter(invalid_timestamp).select(
            "station_id",
            "ingest_date",
            "time_raw",
            F.lit(None).cast("string").alias("date_local"),
            F.lit(None).cast("string").alias("variable"),
            F.lit(None).cast("double").alias("value"),
            F.when(_invalid_ingest_date(), F.lit("invalid ingest_date"))
            .otherwise(F.lit("invalid timestamp"))
            .alias("reason"),
        )
    )

    quarantine = reduce(DataFrame.unionByName, violations)

    dedupe_window = Window.partitionBy("station_id", "time_utc").orderBy(
        F.col("ingest_date").desc_nulls_last(),
        F.col("time_raw").desc_nulls_last(),
    )
    latest = (
        valid_timestamp.withColumn("_row_number", F.row_number().over(dedupe_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    clean = latest.select(
        "station_id",
        "ingest_date",
        "time_utc",
        "time_local",
        "date_local",
        *value_columns,
        "units_json",
        "latitude",
        "longitude",
        "elevation",
        "timezone",
    )

    return clean, quarantine


__all__ = [
    "clean_continuous",
    "clean_meteo",
    "clean_usgs_daily",
    "dedupe_latest",
    "empty_frame",
]
