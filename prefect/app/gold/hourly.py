"""Hourly assembly for the gold layer.

Silver stores three tables at different resolutions. This module puts them on
one shared grid: one row per station per hour, from the first requested hour
through the last. The grid is built explicitly, because the observations have
holes (a station can be offline for days) and a row-based window function would
otherwise jump over the missing hours.
"""

from __future__ import annotations

from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from stations import Station

from gold.config import (
    STAGE_COMPLETE_MIN_SAMPLES,
    TIMEZONE_NAME,
    GoldSettings,
)
from gold.schemas import METEO_COLUMNS


def stations_frame(spark: SparkSession, stations: tuple[Station, ...]) -> DataFrame:
    return spark.createDataFrame(
        [
            (station.site_no, station.name, station.latitude, station.longitude)
            for station in stations
        ],
        "station_id string, station_name string, latitude double, longitude double",
    )


def build_skeleton(
    spark: SparkSession,
    stations_df: DataFrame,
    start: datetime,
    end: datetime,
) -> DataFrame:
    """Return one row per station and hour, including hours without data."""

    hours = spark.sql(
        "select explode(sequence("
        f"timestamp'{start.strftime('%Y-%m-%d %H:%M:%S')}', "
        f"timestamp'{end.strftime('%Y-%m-%d %H:%M:%S')}', "
        "interval 1 hour)) as time_utc"
    )
    return (
        stations_df.select("station_id")
        .crossJoin(hours)
        .withColumn(
            "time_local", F.from_utc_timestamp(F.col("time_utc"), TIMEZONE_NAME)
        )
    )


def rollup_stage(stage: DataFrame) -> DataFrame:
    """Aggregate 15-minute gage height into one row per hour."""

    return (
        stage.withColumn("time_utc", F.date_trunc("hour", F.col("time_utc")))
        .groupBy("station_id", "time_utc")
        .agg(
            F.avg("stage_ft").alias("stage_mean_ft"),
            F.max("stage_ft").alias("stage_max_ft"),
            F.min("stage_ft").alias("stage_min_ft"),
            F.count("stage_ft").cast("long").alias("stage_n"),
        )
    )


def rollup_discharge(discharge: DataFrame) -> DataFrame:
    """Aggregate 15-minute discharge into one row per hour."""

    return (
        discharge.withColumn("time_utc", F.date_trunc("hour", F.col("time_utc")))
        .groupBy("station_id", "time_utc")
        .agg(
            F.avg("discharge_cfs").alias("discharge_mean_cfs"),
            F.count("discharge_cfs").cast("long").alias("discharge_n"),
        )
    )


def daily_to_hourly(
    skeleton: DataFrame,
    daily: DataFrame,
) -> DataFrame:
    """Copy the native daily discharge onto every hour of its local day."""

    days = skeleton.select(
        "station_id",
        "time_utc",
        F.date_format("time_local", "yyyy-MM-dd").alias("date_local"),
    ).distinct()
    joined = days.join(daily, ["station_id", "date_local"], "left")
    return joined.select(
        "station_id",
        "time_utc",
        F.col("discharge_cfs").cast("double").alias("discharge_daily_cfs"),
    )


def assemble(
    skeleton: DataFrame,
    stage: DataFrame,
    discharge_hourly: DataFrame | None,
    discharge_daily: DataFrame,
    meteo: DataFrame,
    meteo_available: tuple[str, ...],
    settings: GoldSettings,
    version: str,
) -> DataFrame:
    """Join the rolled-up sources onto the hourly grid."""

    frame = (
        skeleton.withColumn("date_local", F.date_format("time_local", "yyyy-MM-dd"))
        .join(rollup_stage(stage), ["station_id", "time_utc"], "left")
        .join(
            discharge_hourly
            if discharge_hourly is not None
            else skeleton.select(
                "station_id",
                "time_utc",
                F.lit(None).cast("double").alias("discharge_mean_cfs"),
                F.lit(None).cast("long").alias("discharge_n"),
            ),
            ["station_id", "time_utc"],
            "left",
        )
        .join(daily_to_hourly(skeleton, discharge_daily), ["station_id", "time_utc"], "left")
        .join(meteo, ["station_id", "time_utc"], "left")
    )

    if settings.discharge_source == "hourly":
        model_value = F.col("discharge_mean_cfs")
        grain = F.when(F.col("discharge_mean_cfs").isNotNull(), F.lit("hourly"))
    elif settings.discharge_source == "daily":
        model_value = F.col("discharge_daily_cfs")
        grain = F.when(F.col("discharge_daily_cfs").isNotNull(), F.lit("daily"))
    else:
        model_value = F.coalesce(
            F.col("discharge_mean_cfs"), F.col("discharge_daily_cfs")
        )
        grain = (
            F.when(F.col("discharge_mean_cfs").isNotNull(), F.lit("hourly"))
            .when(F.col("discharge_daily_cfs").isNotNull(), F.lit("daily"))
            .otherwise(F.lit(None))
        )

    meteo_count = reduce(
        lambda left, right: left + right,
        [
            F.when(F.col(column).isNotNull(), F.lit(1)).otherwise(F.lit(0))
            for column in meteo_available
        ]
        or [F.lit(0)],
    )

    return frame.select(
        "station_id",
        "time_utc",
        "time_local",
        "date_local",
        F.date_format("time_utc", "yyyy-MM-dd").alias("date_utc"),
        F.year("time_local").cast("int").alias("year"),
        F.when(F.month("time_local") >= 10, F.year("time_local") + 1)
        .otherwise(F.year("time_local"))
        .cast("int")
        .alias("water_year"),
        "stage_mean_ft",
        "stage_max_ft",
        "stage_min_ft",
        "stage_n",
        (
            F.coalesce(F.col("stage_n"), F.lit(0)) >= STAGE_COMPLETE_MIN_SAMPLES
        ).alias("stage_complete"),
        "discharge_mean_cfs",
        "discharge_n",
        "discharge_daily_cfs",
        model_value.cast("double").alias("discharge_model_cfs"),
        grain.cast("string").alias("discharge_grain"),
        *METEO_COLUMNS,
        meteo_count.cast("long").alias("meteo_n"),
        F.lit(len(meteo_available)).cast("long").alias("meteo_variables_available"),
        F.lit(version).cast("string").alias("dataset_version"),
    )
