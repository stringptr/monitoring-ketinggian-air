"""Data-quality checks for the gold layer.

Gold checks different things from silver. Silver guards the source data; gold
guards the joined dataset: is every hour present, are the hours actually
complete, how much of each feature is missing, and how many rows are usable
for training. Hard failures stop the flow before anything is written.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gold.config import GoldSettings
from gold.schemas import DQ_REPORT_SCHEMA, FEATURE_COLS, TARGET_COLS

HARD = "hard"
SOFT = "soft"
INFO = "info"

HOURS_PER_DAY = 24
DEFAULT_MAX_NULL_RATIO = 0.20
EDGE_DATE = "edge_date"


class GoldQualityError(RuntimeError):
    """Raised when a hard data-quality rule is violated."""


def assert_columns(df: DataFrame, required: tuple[str, ...], table: str) -> None:
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise GoldQualityError(
            f"{table}: missing columns {', '.join(sorted(missing))}"
        )


def assert_no_duplicates(df: DataFrame, keys: tuple[str, ...], table: str) -> None:
    duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
    if duplicates:
        raise GoldQualityError(
            f"{table}: duplicate keys on {', '.join(keys)}"
        )


def _result(
    run_id: str,
    table: str,
    day: str,
    station_id: str,
    check_name: str,
    severity: str,
    failed_count: int,
    total_count: int,
    details: dict | None = None,
) -> dict:
    return {
        "run_id": run_id,
        "table": table,
        "date": day,
        "station_id": station_id,
        "check_name": check_name,
        "severity": severity,
        "passed": int(failed_count) == 0,
        "failed_count": int(failed_count),
        "total_count": int(total_count),
        "details": json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _edge_dates(df: DataFrame) -> set[tuple[str, str]]:
    """Return the first and last local date of every station.

    Those dates are clipped by the requested window, so they are legitimately
    shorter than 24 hours and are exempt from the hour-completeness check.
    """

    bounds = df.groupBy("station_id").agg(
        F.min("date_local").alias("first_day"),
        F.max("date_local").alias("last_day"),
    )
    edges: set[tuple[str, str]] = set()
    for row in bounds.collect():
        edges.add((row["station_id"], row["first_day"]))
        edges.add((row["station_id"], row["last_day"]))
    return edges


def _daily_rows(
    df: DataFrame,
    run_id: str,
    table: str,
    edges: set[tuple[str, str]],
) -> list[dict]:
    summary = df.groupBy("station_id", "date_local").agg(
        F.count("*").cast("int").alias("hours"),
        F.sum(F.when(F.col("stage_complete"), F.lit(1)).otherwise(F.lit(0)))
        .cast("int")
        .alias("complete_hours"),
        F.sum(F.when(F.col("is_trainable"), F.lit(1)).otherwise(F.lit(0)))
        .cast("int")
        .alias("trainable_rows"),
    )

    rows: list[dict] = []
    for record in summary.collect():
        station = record["station_id"]
        day = record["date_local"]
        is_edge = (station, day) in edges
        hours = int(record["hours"] or 0)
        complete = int(record["complete_hours"] or 0)
        trainable = int(record["trainable_rows"] or 0)

        rows.append(
            _result(
                run_id,
                table,
                day,
                station,
                "hours_completeness",
                SOFT,
                0 if is_edge else max(HOURS_PER_DAY - hours, 0),
                HOURS_PER_DAY,
                {"hours": hours, "expected": HOURS_PER_DAY, "edge_date": is_edge},
            )
        )
        rows.append(
            _result(
                run_id,
                table,
                day,
                station,
                "stage_completeness",
                SOFT,
                0 if is_edge else max(hours - complete, 0),
                hours,
                {
                    "complete_hours": complete,
                    "hours": hours,
                    "min_samples_per_hour": 3,
                    "edge_date": is_edge,
                },
            )
        )
        rows.append(
            _result(
                run_id,
                table,
                day,
                station,
                "trainable_rows",
                INFO,
                max(hours - trainable, 0),
                hours,
                {"trainable_rows": trainable, "hours": hours},
            )
        )
    return rows


def _feature_null_rows(
    df: DataFrame,
    run_id: str,
    table: str,
    day: str,
) -> list[dict]:
    """Report how much of each feature column is null over the whole window."""

    total = df.count()
    if total == 0:
        return []

    aggregations = [
        F.sum(F.when(F.col(column).isNull(), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias(column)
        for column in (*FEATURE_COLS, *TARGET_COLS)
    ]
    counts = df.agg(*aggregations).collect()[0].asDict()
    ratios = {
        column: round(int(counts.get(column) or 0) / total, 4)
        for column in (*FEATURE_COLS, *TARGET_COLS)
    }
    failing = {
        column: ratio
        for column, ratio in ratios.items()
        if ratio > DEFAULT_MAX_NULL_RATIO
    }
    return [
        _result(
            run_id,
            table,
            day,
            "all",
            "feature_null_rate",
            SOFT,
            len(failing),
            len(ratios),
            {
                "threshold": DEFAULT_MAX_NULL_RATIO,
                "rows": total,
                "failing": failing,
                "worst": dict(
                    sorted(ratios.items(), key=lambda item: item[1], reverse=True)[:10]
                ),
            },
        )
    ]


def _trainable_row(
    df: DataFrame,
    run_id: str,
    table: str,
    day: str,
    settings: GoldSettings,
) -> list[dict]:
    total = df.count()
    if total == 0:
        return []

    trainable = df.filter(F.col("is_trainable")).count()
    ratio = trainable / total
    return [
        _result(
            run_id,
            table,
            day,
            "all",
            "trainable_ratio",
            SOFT,
            0 if ratio >= settings.min_trainable_ratio else 1,
            total,
            {
                "trainable_rows": trainable,
                "ratio": round(ratio, 4),
                "min_ratio": settings.min_trainable_ratio,
            },
        )
    ]


def run_checks(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    run_id: str,
    settings: GoldSettings,
    required: tuple[str, ...],
) -> DataFrame:
    """Run the gold checks and return them in the DQ report schema."""

    assert_columns(df, required, table)
    assert_no_duplicates(df, ("station_id", "time_utc"), table)

    edges = _edge_dates(df)
    latest_day = df.agg(F.max("date_local")).collect()[0][0]
    day = latest_day or ""

    rows = _daily_rows(df, run_id, table, edges)
    rows += _feature_null_rows(df, run_id, table, day)
    rows += _trainable_row(df, run_id, table, day, settings)

    if not rows:
        return spark.createDataFrame([], DQ_REPORT_SCHEMA)
    return spark.createDataFrame(rows, DQ_REPORT_SCHEMA)
