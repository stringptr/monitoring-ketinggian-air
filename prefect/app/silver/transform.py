"""Transformations that turn parsed bronze data into daily silver tables."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from silver.config import TIMEZONE_NAME, expected_counts
from silver.schemas import (
    GAGE_DAILY_SCHEMA,
    METEO_DAILY_SCHEMA,
    METEO_RANGES,
    METEO_VARIABLES,
    QUARANTINE_CONTINUOUS_SCHEMA,
    QUARANTINE_METEO_SCHEMA,
    QUARANTINE_USGS_DAILY_SCHEMA,
    STATION_DAILY_SCHEMA,
    USGS_DAILY_SCHEMA,
)


def _local_date() -> F.Column:
    """Return the local-day expression for a UTC timestamp column.

    Built lazily because PySpark columns need an active Spark context.
    """

    return F.date_format(
        F.from_utc_timestamp(F.col("ts_utc"), TIMEZONE_NAME),
        "yyyy-MM-dd",
    )


def empty_frame(spark: SparkSession, schema: StructType) -> DataFrame:
    return spark.createDataFrame([], schema)


def day_dimension(spark: SparkSession, days: list[str]) -> DataFrame:
    """Build one row per local day with the expected sample/hour counts."""

    rows = [(day, *expected_counts(day)) for day in days]
    schema = StructType(
        [
            StructField("date", StringType()),
            StructField("expected_samples", IntegerType()),
            StructField("expected_hours", IntegerType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def with_local_date(df: DataFrame) -> DataFrame:
    return df.withColumn("date", _local_date())


def clean_continuous(spark: SparkSession, df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Cast 15-minute gage-height values and split off unusable samples."""

    casted = df.withColumn("value_ft", F.col("value_raw").cast("double"))

    invalid = (
        F.col("time_raw").isNull()
        | F.col("ts_utc").isNull()
        | (
            F.col("value_raw").isNotNull()
            & (F.col("value_ft").isNull() | (F.col("value_ft") < 0))
        )
    )

    quarantine = casted.filter(invalid).select(
        "station_id",
        _local_date().alias("date"),
        F.col("time_raw"),
        F.col("parameter_code"),
        F.col("value_raw"),
        F.col("unit"),
        F.when(
            F.col("time_raw").isNull() | F.col("ts_utc").isNull(),
            F.lit("invalid timestamp"),
        )
        .when(F.col("value_ft").isNull(), F.lit("non-numeric value"))
        .otherwise(F.lit("negative value"))
        .alias("reason"),
    )

    valid = casted.filter(~invalid).select(
        "station_id",
        _local_date().alias("date"),
        F.col("ts_utc"),
        F.col("value_ft"),
        F.col("approval_status"),
    )

    return valid, quarantine


def clean_usgs_daily(spark: SparkSession, df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Cast daily discharge values and quarantine unusable rows."""

    casted = df.withColumn("value_cfs", F.col("value_raw").cast("double"))

    invalid = F.col("date").isNull() | (
        F.col("value_raw").isNotNull()
        & (F.col("value_cfs").isNull() | (F.col("value_cfs") < 0))
    )

    quarantine = casted.filter(invalid).select(
        "station_id",
        "date",
        "parameter_code",
        "value_raw",
        F.when(F.col("date").isNull(), F.lit("invalid date"))
        .when(F.col("value_cfs").isNull(), F.lit("non-numeric value"))
        .otherwise(F.lit("negative value"))
        .alias("reason"),
    )

    valid = casted.filter(~invalid).select(
        "station_id",
        "date",
        "parameter_code",
        "value_cfs",
        "unit",
        "statistic_id",
        "approval_status",
        "last_modified",
    )

    return valid, quarantine


def clean_meteo(spark: SparkSession, df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Null out implausible Open-Meteo values and emit them as quarantine rows."""

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
            df.filter(column.isNotNull() & ~in_range).select(
                "station_id",
                "time_raw",
                "ts_utc",
                F.lit(variable).alias("variable"),
                column.alias("value"),
                F.lit(f"out of range [{low}, {high}]").alias("reason"),
            )
        )

    violations.append(
        df.filter(F.col("ts_utc").isNull()).select(
            "station_id",
            "time_raw",
            "ts_utc",
            F.lit(None).cast("string").alias("variable"),
            F.lit(None).cast("double").alias("value"),
            F.lit("invalid timestamp").alias("reason"),
        )
    )

    quarantine = reduce(DataFrame.unionByName, violations).select(
        "station_id",
        _local_date().alias("date"),
        "time_raw",
        "variable",
        "value",
        "reason",
    )

    clean = df.select(
        "station_id",
        "time_raw",
        "ts_utc",
        *value_columns,
    ).filter(F.col("ts_utc").isNotNull())

    return clean, quarantine


def aggregate_gage(df: DataFrame, dim: DataFrame) -> DataFrame:
    """Collapse 15-minute gage-height samples into one local-day row."""

    window = Window.partitionBy("station_id", "date").orderBy("ts_utc")
    with_gap = df.withColumn(
        "gap_seconds",
        F.unix_timestamp("ts_utc") - F.unix_timestamp(F.lag("ts_utc").over(window)),
    )

    aggregated = with_gap.groupBy("station_id", "date").agg(
        F.avg("value_ft").alias("gage_height_mean_ft"),
        F.min("value_ft").alias("gage_height_min_ft"),
        F.max("value_ft").alias("gage_height_max_ft"),
        F.count("value_ft").cast("int").alias("sample_count"),
        F.sum(F.when(F.col("approval_status") == "Approved", 1).otherwise(0))
        .cast("int")
        .alias("approved_samples"),
        F.sum(F.when(F.col("approval_status") == "Provisional", 1).otherwise(0))
        .cast("int")
        .alias("provisional_samples"),
        F.max("gap_seconds").cast("int").alias("max_gap_seconds"),
    )

    return aggregated.join(dim, "date", "left").select(
        "station_id",
        "date",
        "gage_height_mean_ft",
        "gage_height_min_ft",
        "gage_height_max_ft",
        "sample_count",
        F.coalesce("expected_samples", F.lit(96)).alias("expected_samples"),
        "approved_samples",
        "provisional_samples",
        "max_gap_seconds",
    )


def aggregate_meteo(df: DataFrame, dim: DataFrame) -> DataFrame:
    """Collapse hourly Open-Meteo data into one local-day row."""

    aggregated = df.groupBy("station_id", "date").agg(
        F.sum("precipitation").alias("precipitation_mm"),
        F.sum("rain").alias("rain_mm"),
        F.sum("et0_fao_evapotranspiration").alias("et0_mm"),
        F.avg("temperature_2m").alias("temperature_2m_mean_c"),
        F.min("temperature_2m").alias("temperature_2m_min_c"),
        F.max("temperature_2m").alias("temperature_2m_max_c"),
        F.avg("relative_humidity_2m").alias("relative_humidity_2m_mean_pct"),
        F.avg("surface_pressure").alias("surface_pressure_mean_hpa"),
        F.avg("soil_moisture_0_to_7cm").alias("soil_moisture_0_7_mean"),
        F.avg("soil_moisture_7_to_28cm").alias("soil_moisture_7_28_mean"),
        F.avg("soil_moisture_28_to_100cm").alias("soil_moisture_28_100_mean"),
        F.avg("wind_speed_10m").alias("wind_speed_10m_mean_kmh"),
        F.max("wind_speed_10m").alias("wind_speed_10m_max_kmh"),
        F.avg("shortwave_radiation").alias("shortwave_radiation_mean_wm2"),
        F.sum("shortwave_radiation").alias("shortwave_radiation_sum_whm2"),
        F.avg("cloud_cover").alias("cloud_cover_mean_pct"),
        F.count("ts_utc").cast("int").alias("observed_hours"),
    )

    return aggregated.join(dim, "date", "left").select(
        "station_id",
        "date",
        "precipitation_mm",
        "rain_mm",
        "et0_mm",
        "temperature_2m_mean_c",
        "temperature_2m_min_c",
        "temperature_2m_max_c",
        "relative_humidity_2m_mean_pct",
        "surface_pressure_mean_hpa",
        "soil_moisture_0_7_mean",
        "soil_moisture_7_28_mean",
        "soil_moisture_28_100_mean",
        "wind_speed_10m_mean_kmh",
        "wind_speed_10m_max_kmh",
        "shortwave_radiation_mean_wm2",
        "shortwave_radiation_sum_whm2",
        "cloud_cover_mean_pct",
        "observed_hours",
        F.coalesce("expected_hours", F.lit(24)).alias("expected_hours"),
    )


def build_station_daily(
    meteo: DataFrame,
    usgs_daily: DataFrame,
    gage: DataFrame,
) -> DataFrame:
    """Join the three daily sources into the analysis-ready silver table."""

    meteo_renamed = meteo.withColumnRenamed(
        "observed_hours", "meteo_observed_hours"
    ).withColumnRenamed("expected_hours", "meteo_expected_hours")

    usgs_renamed = (
        usgs_daily.withColumnRenamed("value_cfs", "discharge_cfs")
        .withColumnRenamed("unit", "discharge_unit")
        .withColumnRenamed("statistic_id", "discharge_statistic_id")
        .withColumnRenamed("approval_status", "discharge_approval_status")
        .drop("parameter_code", "last_modified")
    )

    gage_renamed = (
        gage.withColumnRenamed("sample_count", "gage_height_sample_count")
        .withColumnRenamed("expected_samples", "gage_height_expected_samples")
        .withColumnRenamed("provisional_samples", "gage_height_provisional_samples")
        .withColumnRenamed("max_gap_seconds", "gage_height_max_gap_seconds")
    )

    joined = meteo_renamed.join(
        usgs_renamed, ["station_id", "date"], "full_outer"
    ).join(gage_renamed, ["station_id", "date"], "full_outer")

    return joined.select(
        "station_id",
        "date",
        "discharge_cfs",
        "discharge_unit",
        "discharge_statistic_id",
        "discharge_approval_status",
        "gage_height_mean_ft",
        "gage_height_min_ft",
        "gage_height_max_ft",
        "gage_height_sample_count",
        "gage_height_expected_samples",
        "gage_height_provisional_samples",
        (
            F.col("gage_height_sample_count") / F.col("gage_height_expected_samples")
        ).alias("gage_height_completeness"),
        "gage_height_max_gap_seconds",
        "precipitation_mm",
        "rain_mm",
        "et0_mm",
        "temperature_2m_mean_c",
        "temperature_2m_min_c",
        "temperature_2m_max_c",
        "relative_humidity_2m_mean_pct",
        "surface_pressure_mean_hpa",
        "soil_moisture_0_7_mean",
        "soil_moisture_7_28_mean",
        "soil_moisture_28_100_mean",
        "wind_speed_10m_mean_kmh",
        "wind_speed_10m_max_kmh",
        "shortwave_radiation_mean_wm2",
        "shortwave_radiation_sum_whm2",
        "cloud_cover_mean_pct",
        "meteo_observed_hours",
        "meteo_expected_hours",
        (F.col("meteo_observed_hours") / F.col("meteo_expected_hours")).alias(
            "meteo_completeness"
        ),
        F.col("discharge_cfs").isNotNull().alias("has_discharge"),
        F.col("gage_height_sample_count").isNotNull().alias("has_gage_height"),
        F.col("meteo_observed_hours").isNotNull().alias("has_meteo"),
    )


__all__ = [
    "GAGE_DAILY_SCHEMA",
    "METEO_DAILY_SCHEMA",
    "QUARANTINE_CONTINUOUS_SCHEMA",
    "QUARANTINE_METEO_SCHEMA",
    "QUARANTINE_USGS_DAILY_SCHEMA",
    "STATION_DAILY_SCHEMA",
    "USGS_DAILY_SCHEMA",
    "aggregate_gage",
    "aggregate_meteo",
    "build_station_daily",
    "clean_continuous",
    "clean_meteo",
    "clean_usgs_daily",
    "day_dimension",
    "empty_frame",
    "with_local_date",
]
