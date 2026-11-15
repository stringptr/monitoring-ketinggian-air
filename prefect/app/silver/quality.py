"""Basic data-quality checks for the native-resolution silver tables.

Hard failures (schema contract, duplicate natural keys) stop the flow. Every
other check is recorded in ``silver/_dq`` so it can be inspected later, and
invalid source rows are written to ``silver/_quarantine``.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from pandera.errors import SchemaError, SchemaErrors
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from silver.config import (
    TABLE_METEO_HOURLY,
    TABLE_USGS_CONTINUOUS,
    TABLE_USGS_DAILY,
    SilverSettings,
)
from silver.schemas import DQ_REPORT_SCHEMA, METEO_VARIABLES

HARD = "hard"
SOFT = "soft"
INFO = "info"


class SilverQualityError(RuntimeError):
    """Raised when a hard data-quality rule is violated."""


def validate_contract(pdf, schema, table: str) -> None:
    """Validate a collected table against its pandera contract."""

    try:
        schema.validate(pdf, lazy=True)
    except (SchemaError, SchemaErrors) as exc:
        raise SilverQualityError(f"{table}: schema contract failed\n{exc}") from exc


def assert_no_duplicates(df: DataFrame, keys: list[str], table: str) -> None:
    duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
    if duplicates:
        raise SilverQualityError(
            f"{table}: duplicate natural keys on {', '.join(keys)}"
        )


def _result(
    run_id: str,
    table: str,
    ingest_date: str,
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
        "ingest_date": ingest_date,
        "station_id": station_id,
        "check_name": check_name,
        "severity": severity,
        "passed": int(failed_count) == 0,
        "failed_count": int(failed_count),
        "total_count": int(total_count),
        "details": json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _collect_counts(
    df: DataFrame,
    aggregations: list,
    skeleton: DataFrame,
):
    """Join per (station, ingest_date) counts onto the requested skeleton."""

    counts = df.groupBy("station_id", "ingest_date").agg(*aggregations)
    joined = skeleton.join(counts, ["station_id", "ingest_date"], "left").fillna(0)
    return joined.toPandas()


def _max_ingest_date(skeleton: DataFrame) -> str:
    value = skeleton.agg(F.max("ingest_date")).collect()[0][0]
    return value or ""


def _freshness_result(
    run_id: str,
    table: str,
    ingest_date: str,
    newest,
    settings: SilverSettings,
    *,
    date_only: bool = False,
) -> dict:
    now = datetime.now(timezone.utc)
    if date_only:
        cutoff_date = now.date() - timedelta(days=settings.freshness_days)
        newest_date = date.fromisoformat(newest) if newest else None
        failed = 0 if newest_date is not None and newest_date >= cutoff_date else 1
        details = {
            "newest": newest_date.isoformat() if newest_date else None,
            "cutoff": cutoff_date.isoformat(),
        }
    else:
        cutoff_dt = now - timedelta(days=settings.freshness_days)
        newest_dt = newest
        if newest_dt is not None and newest_dt.tzinfo is None:
            newest_dt = newest_dt.replace(tzinfo=timezone.utc)
        failed = 0 if newest_dt is not None and newest_dt >= cutoff_dt else 1
        details = {
            "newest": newest_dt.isoformat() if newest_dt else None,
            "cutoff": cutoff_dt.isoformat(),
        }
    return _result(
        run_id,
        table,
        ingest_date,
        "all",
        "freshness",
        SOFT,
        failed,
        1,
        details,
    )


def run_continuous_checks(
    df: DataFrame,
    skeleton: DataFrame,
    settings: SilverSettings,
    run_id: str,
    check_freshness: bool,
) -> list[dict]:
    """Checks for 15-minute USGS gage-height samples."""

    results: list[dict] = []
    counts = _collect_counts(
        df,
        [
            F.count("*").alias("row_count"),
            F.sum(F.when(F.col("value").isNull(), 1).otherwise(0)).alias(
                "missing_values"
            ),
            F.sum(F.when(F.col("value") < 0, 1).otherwise(0)).alias("negative_values"),
            F.sum(
                F.when(F.col("approval_status") == "Provisional", 1).otherwise(0)
            ).alias("provisional_values"),
            F.sum(
                F.when(
                    F.col("unit").isNotNull() & (F.col("unit") != "ft"),
                    1,
                ).otherwise(0)
            ).alias("wrong_unit"),
        ],
        skeleton,
    )

    for row in counts.itertuples(index=False):
        expected = int(row.expected_samples)
        total_rows = int(row.row_count)
        results.extend(
            [
                _result(
                    run_id,
                    TABLE_USGS_CONTINUOUS,
                    row.ingest_date,
                    row.station_id,
                    "completeness",
                    SOFT,
                    max(expected - total_rows, 0),
                    expected,
                    {"expected_samples": expected, "actual_samples": total_rows},
                ),
                _result(
                    run_id,
                    TABLE_USGS_CONTINUOUS,
                    row.ingest_date,
                    row.station_id,
                    "missing_values",
                    SOFT,
                    int(row.missing_values),
                    total_rows,
                ),
                _result(
                    run_id,
                    TABLE_USGS_CONTINUOUS,
                    row.ingest_date,
                    row.station_id,
                    "negative_values",
                    SOFT,
                    int(row.negative_values),
                    total_rows,
                ),
                _result(
                    run_id,
                    TABLE_USGS_CONTINUOUS,
                    row.ingest_date,
                    row.station_id,
                    "provisional_values",
                    INFO,
                    int(row.provisional_values),
                    total_rows,
                ),
                _result(
                    run_id,
                    TABLE_USGS_CONTINUOUS,
                    row.ingest_date,
                    row.station_id,
                    "unit_consistency",
                    SOFT,
                    int(row.wrong_unit),
                    total_rows,
                    {"expected": "ft"},
                ),
            ]
        )

    window = Window.partitionBy("station_id", "ingest_date").orderBy("time_utc")
    gaps = (
        df.withColumn(
            "gap_seconds",
            F.unix_timestamp("time_utc")
            - F.unix_timestamp(F.lag("time_utc").over(window)),
        )
        .groupBy("station_id", "ingest_date")
        .agg(F.max("gap_seconds").alias("max_gap_seconds"))
    )
    gap_rows = skeleton.join(gaps, ["station_id", "ingest_date"], "left").fillna(0)
    threshold = settings.max_gap_hours * 3600
    for row in gap_rows.toPandas().itertuples(index=False):
        max_gap = int(row.max_gap_seconds)
        results.append(
            _result(
                run_id,
                TABLE_USGS_CONTINUOUS,
                row.ingest_date,
                row.station_id,
                "max_gap",
                SOFT,
                1 if max_gap > threshold else 0,
                1,
                {"max_gap_seconds": max_gap, "threshold_seconds": threshold},
            )
        )

    if check_freshness:
        newest = df.agg(F.max("time_utc")).collect()[0][0]
        results.append(
            _freshness_result(
                run_id,
                TABLE_USGS_CONTINUOUS,
                _max_ingest_date(skeleton),
                newest,
                settings,
            )
        )

    return results


def run_usgs_daily_checks(
    df: DataFrame,
    skeleton: DataFrame,
    settings: SilverSettings,
    run_id: str,
    check_freshness: bool,
) -> list[dict]:
    """Checks for native daily USGS discharge values."""

    results: list[dict] = []
    counts = _collect_counts(
        df,
        [
            F.count("*").alias("row_count"),
            F.sum(F.when(F.col("value").isNull(), 1).otherwise(0)).alias(
                "missing_values"
            ),
            F.sum(F.when(F.col("value") < 0, 1).otherwise(0)).alias("negative_values"),
            F.sum(
                F.when(F.col("approval_status") == "Provisional", 1).otherwise(0)
            ).alias("provisional_values"),
            F.sum(
                F.when(
                    F.col("unit").isNotNull() & (F.col("unit") != "ft^3/s"),
                    1,
                ).otherwise(0)
            ).alias("wrong_unit"),
        ],
        skeleton,
    )

    for row in counts.itertuples(index=False):
        expected = int(row.expected_daily)
        total_rows = int(row.row_count)
        results.extend(
            [
                _result(
                    run_id,
                    TABLE_USGS_DAILY,
                    row.ingest_date,
                    row.station_id,
                    "completeness",
                    SOFT,
                    max(expected - total_rows, 0),
                    expected,
                    {"expected_values": expected, "actual_values": total_rows},
                ),
                _result(
                    run_id,
                    TABLE_USGS_DAILY,
                    row.ingest_date,
                    row.station_id,
                    "missing_values",
                    SOFT,
                    int(row.missing_values),
                    total_rows,
                ),
                _result(
                    run_id,
                    TABLE_USGS_DAILY,
                    row.ingest_date,
                    row.station_id,
                    "negative_values",
                    SOFT,
                    int(row.negative_values),
                    total_rows,
                ),
                _result(
                    run_id,
                    TABLE_USGS_DAILY,
                    row.ingest_date,
                    row.station_id,
                    "provisional_values",
                    INFO,
                    int(row.provisional_values),
                    total_rows,
                ),
                _result(
                    run_id,
                    TABLE_USGS_DAILY,
                    row.ingest_date,
                    row.station_id,
                    "unit_consistency",
                    SOFT,
                    int(row.wrong_unit),
                    total_rows,
                    {"expected": "ft^3/s"},
                ),
            ]
        )

    if check_freshness:
        newest = df.agg(F.max("date_local")).collect()[0][0]
        results.append(
            _freshness_result(
                run_id,
                TABLE_USGS_DAILY,
                _max_ingest_date(skeleton),
                newest,
                settings,
                date_only=True,
            )
        )

    return results


def run_meteo_checks(
    df: DataFrame,
    skeleton: DataFrame,
    settings: SilverSettings,
    run_id: str,
    check_freshness: bool,
) -> list[dict]:
    """Checks for hourly Open-Meteo values."""

    results: list[dict] = []
    missing_exprs = [
        F.sum(F.when(F.col(variable).isNull(), 1).otherwise(0)).alias(variable)
        for variable in METEO_VARIABLES
    ]
    counts = _collect_counts(
        df,
        [
            F.count("*").alias("row_count"),
            F.sum(F.when(F.col("units_json").isNull(), 1).otherwise(0)).alias(
                "missing_units"
            ),
            *missing_exprs,
        ],
        skeleton,
    )

    for row in counts.itertuples(index=False):
        expected = int(row.expected_hours)
        total_rows = int(row.row_count)
        missing_by_variable = {
            variable: int(getattr(row, variable)) for variable in METEO_VARIABLES
        }
        missing_total = sum(missing_by_variable.values())
        results.extend(
            [
                _result(
                    run_id,
                    TABLE_METEO_HOURLY,
                    row.ingest_date,
                    row.station_id,
                    "completeness",
                    SOFT,
                    max(expected - total_rows, 0),
                    expected,
                    {"expected_hours": expected, "actual_hours": total_rows},
                ),
                _result(
                    run_id,
                    TABLE_METEO_HOURLY,
                    row.ingest_date,
                    row.station_id,
                    "missing_variable_values",
                    SOFT,
                    missing_total,
                    total_rows * len(METEO_VARIABLES),
                    missing_by_variable,
                ),
                _result(
                    run_id,
                    TABLE_METEO_HOURLY,
                    row.ingest_date,
                    row.station_id,
                    "missing_units",
                    SOFT,
                    int(row.missing_units),
                    total_rows,
                ),
            ]
        )

    if check_freshness:
        newest = df.agg(F.max("time_utc")).collect()[0][0]
        results.append(
            _freshness_result(
                run_id,
                TABLE_METEO_HOURLY,
                _max_ingest_date(skeleton),
                newest,
                settings,
            )
        )

    return results


def run_quarantine_check(
    df: DataFrame,
    quarantine: DataFrame,
    table: str,
    run_id: str,
    ingest_date: str,
) -> dict:
    total = df.count()
    quarantined = quarantine.count()
    return _result(
        run_id,
        table,
        ingest_date,
        "all",
        "quarantined_rows",
        SOFT,
        quarantined,
        total + quarantined,
    )


def build_report(spark: SparkSession, results: list[dict]) -> DataFrame:
    if not results:
        return spark.createDataFrame([], DQ_REPORT_SCHEMA)
    return spark.createDataFrame(results, DQ_REPORT_SCHEMA)


__all__ = [
    "SilverQualityError",
    "assert_no_duplicates",
    "build_report",
    "run_continuous_checks",
    "run_meteo_checks",
    "run_quarantine_check",
    "run_usgs_daily_checks",
    "validate_contract",
]
