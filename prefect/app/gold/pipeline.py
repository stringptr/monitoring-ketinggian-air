"""Orchestration of the gold layer.

One run reads the silver window, builds the hourly grid, fills the model
features, writes the two gold tables and records the data-quality report and
the dataset version. Gold is fully rebuildable from silver: nothing here is
incremental state that cannot be recomputed.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from common.spark import build_spark_session
from stations import get_stations

from gold.config import (
    TARGET_HORIZONS,
    TABLE_DATASET_VERSIONS,
    TABLE_FEATURES,
    TABLE_STATION_HOURLY,
    GoldSettings,
    window_bounds,
)
from gold.features import build_features
from gold.hourly import (
    assemble,
    build_skeleton,
    rollup_discharge,
    stations_frame,
)
from gold.quality import run_checks
from gold.readers import (
    read_discharge_daily,
    read_discharge_hourly,
    read_meteo,
    read_stage,
)
from gold.schemas import (
    DATASET_VERSIONS_SCHEMA,
    FEATURE_COLS,
    METEO_COLUMNS,
)
from gold.validation import validate_features
from gold.version import dataset_version
from gold.writer import write_dataset_version, write_dq_report, write_table


def run_id() -> str:
    return f"gold-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def _union(frames: list[DataFrame]) -> DataFrame:
    return reduce(lambda left, right: left.unionByName(right), frames)


def _window_filter(df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    stop = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc) + timedelta(
        days=1
    )
    return df.filter(
        (F.col("time_utc") >= F.lit(start)) & (F.col("time_utc") < F.lit(stop))
    )


def _grain_label(df: DataFrame) -> str:
    rows = (
        df.filter(F.col("discharge_grain").isNotNull())
        .groupBy("discharge_grain")
        .count()
        .collect()
    )
    labels = sorted(row["discharge_grain"] for row in rows)
    if not labels:
        return "none"
    return labels[0] if len(labels) == 1 else "mixed"


def run_gold(
    start_date: str,
    end_date: str,
    settings: GoldSettings,
) -> dict:
    """Build the gold tables for the inclusive UTC date range."""

    if date.fromisoformat(start_date) > date.fromisoformat(end_date):
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")

    spark = build_spark_session(settings)
    try:
        identifier = run_id()
        stations = get_stations()
        start_ts, end_ts = window_bounds(settings, start_date, end_date)

        print(
            f"gold {start_date}..{end_date}: reading silver "
            f"{start_ts.isoformat()}..{end_ts.isoformat()}"
        )

        skeleton = build_skeleton(
            spark,
            stations_frame(spark, stations),
            start_ts,
            end_ts,
        )

        stage_frames: list[DataFrame] = []
        discharge_hourly_frames: list[DataFrame] = []
        daily_frames: list[DataFrame] = []
        meteo_frames: list[DataFrame] = []
        meteo_seen: set[str] = set()

        daily_start = (start_ts - timedelta(days=1)).date().isoformat()
        daily_end = (end_ts + timedelta(days=1)).date().isoformat()

        for station in stations:
            stage_frames.append(
                read_stage(spark, settings, station, start_ts, end_ts)
            )
            hourly_discharge = read_discharge_hourly(
                spark, settings, station, start_ts, end_ts
            )
            if hourly_discharge is not None:
                discharge_hourly_frames.append(hourly_discharge)
            daily_frames.append(
                read_discharge_daily(
                    spark, settings, station, daily_start, daily_end
                )
            )
            meteo_frame, meteo_variables = read_meteo(
                spark, settings, station, start_ts, end_ts
            )
            meteo_frames.append(meteo_frame)
            meteo_seen.update(meteo_variables)

        stage = _union(stage_frames)
        discharge_daily = _union(daily_frames)
        meteo = _union(meteo_frames)

        discharge_hourly = (
            rollup_discharge(_union(discharge_hourly_frames))
            if discharge_hourly_frames
            else None
        )

        if stage.limit(1).count() == 0:
            raise RuntimeError(
                "no silver gage-height rows in the requested window; "
                "run the bronze and silver backfill first"
            )

        max_stage_time = stage.agg(F.max("time_utc")).collect()[0][0]
        if max_stage_time is not None:
            available_end = max_stage_time.date().isoformat()
            if available_end < end_date:
                print(
                    f"gold: trimming end_date from {end_date} to {available_end} "
                    "(last day with gage-height data)"
                )
                end_date = available_end

        if date.fromisoformat(start_date) > date.fromisoformat(end_date):
            raise RuntimeError(
                f"no gage-height data between {start_date} and {end_date}"
            )

        hourly = assemble(
            skeleton,
            stage,
            discharge_hourly,
            discharge_daily,
            meteo,
            tuple(column for column in METEO_COLUMNS if column in meteo_seen),
            settings,
            end_date,
        )

        features = build_features(hourly, TARGET_HORIZONS)

        hourly_window = _window_filter(hourly, start_date, end_date)
        features_window = _window_filter(features, start_date, end_date)

        version = dataset_version()
        hourly_window = hourly_window.withColumn("dataset_version", F.lit(version))
        features_window = features_window.withColumn("dataset_version", F.lit(version))

        # Validate before publishing: a hard failure here leaves storage untouched.
        validate_features(features_window, TABLE_FEATURES)
        dq = run_checks(
            spark,
            features_window,
            TABLE_FEATURES,
            identifier,
            settings,
        )

        write_table(
            spark,
            hourly_window,
            settings,
            TABLE_STATION_HOURLY,
            partition_cols=("station_id", "date_utc"),
        )
        write_table(
            spark,
            features_window,
            settings,
            TABLE_FEATURES,
            partition_cols=("station_id", "date_utc"),
        )
        write_dq_report(spark, dq, settings, TABLE_FEATURES)

        rows_hourly = hourly_window.count()
        rows_features = features_window.count()
        rows_trainable = features_window.filter(F.col("is_trainable")).count()
        grain = _grain_label(features_window)
        processed_at = datetime.now(timezone.utc)

        # Written last: its presence marks the run as committed for consumers.
        summary_row = {
            "dataset_version": version,
            "run_id": identifier,
            "status": "committed",
            "data_start_date": start_date,
            "data_end_date": end_date,
            "processed_at": processed_at,
            "max_time_utc": features_window.agg(F.max("time_utc")).collect()[0][0],
            "rows_hourly": rows_hourly,
            "rows_features": rows_features,
            "rows_trainable": rows_trainable,
            "feature_count": len(FEATURE_COLS),
            "discharge_grain": grain,
            "created_at": processed_at,
        }
        write_dataset_version(
            spark,
            spark.createDataFrame([summary_row], DATASET_VERSIONS_SCHEMA),
            settings,
            TABLE_DATASET_VERSIONS,
        )

        dq_summary = (
            dq.groupBy("severity", "passed").count().collect() if dq.columns else []
        )

        result = {
            "run_id": identifier,
            "dataset_version": version,
            "start_date": start_date,
            "end_date": end_date,
            "rows": {
                TABLE_STATION_HOURLY: rows_hourly,
                TABLE_FEATURES: rows_features,
                "trainable": rows_trainable,
            },
            "discharge_grain": grain,
            "dq": {
                f"{row['severity']}:{'pass' if row['passed'] else 'fail'}": row["count"]
                for row in dq_summary
            },
        }
        print(f"gold complete: {result}")
        return result
    finally:
        spark.stop()

