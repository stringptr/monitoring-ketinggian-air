"""Data-quality checks for the gold layer.

Hard validation lives in ``gold/validation.py`` and runs before anything is
written. This module only produces the report: it never raises on a soft or
informational finding and it never writes the gold tables.

Two kinds of checks are recorded separately:

* ``scope="date"`` — the finding describes one local date (hours present,
  complete hours, trainable rows on that date);
* ``scope="window"`` — the finding describes the whole processed range
  (feature null rate, trainable ratio), so it carries ``window_start`` and
  ``window_end`` and leaves ``date`` empty.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gold.config import GoldSettings
from gold.schemas import DQ_REPORT_SCHEMA, FEATURE_COLS, TARGET_COLS

SOFT = "soft"
INFO = "info"

SCOPE_DATE = "date"
SCOPE_WINDOW = "window"

HOURS_PER_DAY = 24
DEFAULT_MAX_NULL_RATIO = 0.20
EDGE_DATE = "edge_date"


def _result(
    run_id: str,
    table: str,
    scope: str,
    day: str | None,
    window_start: str | None,
    window_end: str | None,
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
        "scope": scope,
        "date": day,
        "window_start": window_start,
        "window_end": window_end,
        "station_id": station_id,
        "check_name": check_name,
        "severity": severity,
        "passed": int(failed_count) == 0,
        "failed_count": int(failed_count),
        "total_count": int(total_count),
        "details": json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _window_bounds(df: DataFrame) -> tuple[str, str]:
    row = df.agg(
        F.min("date_local").alias("first_day"),
        F.max("date_local").alias("last_day"),
    ).collect()[0]
    return row["first_day"] or "", row["last_day"] or ""


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
                SCOPE_DATE,
                day,
                day,
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
                SCOPE_DATE,
                day,
                day,
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
                SCOPE_DATE,
                day,
                day,
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
    window_start: str,
    window_end: str,
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
            SCOPE_WINDOW,
            None,
            window_start,
            window_end,
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
    window_start: str,
    window_end: str,
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
            SCOPE_WINDOW,
            None,
            window_start,
            window_end,
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
) -> DataFrame:
    """Run the soft checks and return them in the DQ report schema."""

    window_start, window_end = _window_bounds(df)
    edges = _edge_dates(df) if window_start else set()

    rows: list[dict] = []
    if window_start:
        rows += _daily_rows(df, run_id, table, edges)
        rows += _feature_null_rows(df, run_id, table, window_start, window_end)
        rows += _trainable_row(
            df, run_id, table, window_start, window_end, settings
        )

    if not rows:
        return spark.createDataFrame([], DQ_REPORT_SCHEMA)
    return spark.createDataFrame(rows, DQ_REPORT_SCHEMA)
