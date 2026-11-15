"""Basic data-quality checks for the daily silver tables.

Hard failures (schema contract, duplicate natural keys) stop the flow. Every
other check is recorded in the ``silver/_dq`` table so it can be inspected
later, and the offending source rows are written to ``silver/_quarantine``.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from pandera.errors import SchemaError, SchemaErrors
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver.config import (
    TABLE_GAGE_DAILY,
    TABLE_METEO_DAILY,
    TABLE_STATION_DAILY,
    TABLE_USGS_DAILY,
    SilverSettings,
    local_today,
)
from silver.schemas import DQ_REPORT_SCHEMA

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


def assert_no_duplicates(
    df: DataFrame,
    keys: list[str],
    table: str,
) -> None:
    duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
    if duplicates:
        raise SilverQualityError(
            f"{table}: duplicate natural keys on {', '.join(keys)}"
        )


def _result(
    run_id: str,
    table: str,
    check_name: str,
    severity: str,
    failed_count: int,
    total_count: int,
    details: dict | None = None,
) -> dict:
    return {
        "run_id": run_id,
        "table": table,
        "check_name": check_name,
        "severity": severity,
        "passed": failed_count == 0,
        "failed_count": int(failed_count),
        "total_count": int(total_count),
        "details": json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _count(df: DataFrame, condition=None) -> int:
    scoped = df if condition is None else df.filter(condition)
    return scoped.count()


def _daily_range_predicate() -> F.Column:
    """Return a predicate that is true for implausible daily aggregates."""

    return (
        (F.col("gage_height_mean_ft") < 0)
        | (F.col("gage_height_min_ft") < 0)
        | (F.col("gage_height_max_ft") < 0)
        | (F.col("discharge_cfs") < 0)
        | (F.col("precipitation_mm") < 0)
        | (F.col("rain_mm") < 0)
        | (F.col("et0_mm") < 0)
        | (
            F.col("relative_humidity_2m_mean_pct").isNotNull()
            & ~F.col("relative_humidity_2m_mean_pct").between(0, 100)
        )
        | (
            F.col("cloud_cover_mean_pct").isNotNull()
            & ~F.col("cloud_cover_mean_pct").between(0, 100)
        )
        | (
            F.col("soil_moisture_0_7_mean").isNotNull()
            & ~F.col("soil_moisture_0_7_mean").between(0, 1.5)
        )
        | (
            F.col("soil_moisture_7_28_mean").isNotNull()
            & ~F.col("soil_moisture_7_28_mean").between(0, 1.5)
        )
        | (
            F.col("soil_moisture_28_100_mean").isNotNull()
            & ~F.col("soil_moisture_28_100_mean").between(0, 1.5)
        )
        | (
            F.col("temperature_2m_mean_c").isNotNull()
            & ~F.col("temperature_2m_mean_c").between(-60, 60)
        )
        | (
            F.col("surface_pressure_mean_hpa").isNotNull()
            & ~F.col("surface_pressure_mean_hpa").between(800, 1100)
        )
        | (F.col("wind_speed_10m_mean_kmh") < 0)
        | (F.col("wind_speed_10m_max_kmh") < 0)
        | (F.col("shortwave_radiation_mean_wm2") < 0)
        | (F.col("shortwave_radiation_sum_whm2") < 0)
    )


def run_station_checks(
    df: DataFrame,
    settings: SilverSettings,
    run_id: str,
    check_freshness: bool,
) -> list[dict]:
    """Soft checks over the joined ``station_daily`` table."""

    results: list[dict] = []
    total = _count(df)
    has_gage = F.col("has_gage_height")
    has_meteo = F.col("has_meteo")

    gage_total = _count(df, has_gage)
    results.append(
        _result(
            run_id,
            TABLE_STATION_DAILY,
            "gage_height_completeness",
            SOFT,
            _count(
                df,
                has_gage
                & (F.col("gage_height_completeness") < settings.gage_completeness_min),
            ),
            gage_total,
            {"threshold": settings.gage_completeness_min},
        )
    )

    meteo_total = _count(df, has_meteo)
    results.append(
        _result(
            run_id,
            TABLE_STATION_DAILY,
            "meteo_completeness",
            SOFT,
            _count(
                df,
                has_meteo
                & (F.col("meteo_completeness") < settings.meteo_completeness_min),
            ),
            meteo_total,
            {"threshold": settings.meteo_completeness_min},
        )
    )

    max_gap = settings.max_gap_hours * 3600
    results.append(
        _result(
            run_id,
            TABLE_STATION_DAILY,
            "gage_height_max_gap",
            SOFT,
            _count(
                df,
                has_gage & (F.col("gage_height_max_gap_seconds") > max_gap),
            ),
            gage_total,
            {"threshold_seconds": max_gap},
        )
    )

    results.append(
        _result(
            run_id,
            TABLE_STATION_DAILY,
            "missing_discharge",
            SOFT,
            _count(df, ~F.col("has_discharge")),
            total,
        )
    )
    results.append(
        _result(
            run_id,
            TABLE_STATION_DAILY,
            "missing_gage_height",
            SOFT,
            _count(df, ~has_gage),
            total,
        )
    )
    results.append(
        _result(
            run_id,
            TABLE_STATION_DAILY,
            "missing_meteo",
            SOFT,
            _count(df, ~has_meteo),
            total,
        )
    )

    results.append(
        _result(
            run_id,
            TABLE_STATION_DAILY,
            "value_range",
            SOFT,
            _count(df, _daily_range_predicate()),
            total,
        )
    )

    results.append(
        _result(
            run_id,
            TABLE_STATION_DAILY,
            "provisional_gage_height",
            INFO,
            _count(
                df,
                has_gage & (F.col("gage_height_provisional_samples") > 0),
            ),
            gage_total,
        )
    )

    if check_freshness:
        newest = df.agg(F.max("date")).collect()[0][0]
        cutoff = (local_today() - timedelta(days=settings.freshness_days)).isoformat()
        failed = 0 if newest is not None and newest >= cutoff else 1
        results.append(
            _result(
                run_id,
                TABLE_STATION_DAILY,
                "freshness",
                SOFT,
                failed,
                total,
                {"newest_date": newest, "cutoff": cutoff},
            )
        )

    return results


def run_usgs_daily_checks(
    df: DataFrame,
    settings: SilverSettings,
    run_id: str,
) -> list[dict]:
    total = _count(df)
    return [
        _result(
            run_id,
            TABLE_USGS_DAILY,
            "unit_expected_cfs",
            SOFT,
            _count(
                df,
                F.col("unit").isNotNull() & (F.col("unit") != "ft^3/s"),
            ),
            total,
            {"expected": "ft^3/s"},
        ),
        _result(
            run_id,
            TABLE_USGS_DAILY,
            "missing_value",
            SOFT,
            _count(df, F.col("value_cfs").isNull()),
            total,
        ),
    ]


def run_gage_checks(
    df: DataFrame,
    settings: SilverSettings,
    run_id: str,
) -> list[dict]:
    total = _count(df)
    return [
        _result(
            run_id,
            TABLE_GAGE_DAILY,
            "negative_value",
            SOFT,
            _count(
                df,
                (F.col("gage_height_mean_ft") < 0)
                | (F.col("gage_height_min_ft") < 0)
                | (F.col("gage_height_max_ft") < 0),
            ),
            total,
        ),
        _result(
            run_id,
            TABLE_GAGE_DAILY,
            "sample_count_above_expected",
            SOFT,
            _count(
                df,
                F.col("sample_count") > F.col("expected_samples"),
            ),
            total,
        ),
    ]


def run_meteo_checks(
    df: DataFrame,
    settings: SilverSettings,
    run_id: str,
) -> list[dict]:
    total = _count(df)
    predicate = (
        (F.col("precipitation_mm") < 0)
        | (F.col("rain_mm") < 0)
        | (F.col("et0_mm") < 0)
        | (
            F.col("relative_humidity_2m_mean_pct").isNotNull()
            & ~F.col("relative_humidity_2m_mean_pct").between(0, 100)
        )
        | (
            F.col("cloud_cover_mean_pct").isNotNull()
            & ~F.col("cloud_cover_mean_pct").between(0, 100)
        )
        | (
            F.col("temperature_2m_mean_c").isNotNull()
            & ~F.col("temperature_2m_mean_c").between(-60, 60)
        )
        | (
            F.col("surface_pressure_mean_hpa").isNotNull()
            & ~F.col("surface_pressure_mean_hpa").between(800, 1100)
        )
        | (F.col("wind_speed_10m_mean_kmh") < 0)
        | (F.col("shortwave_radiation_mean_wm2") < 0)
    )
    return [
        _result(
            run_id,
            TABLE_METEO_DAILY,
            "value_range",
            SOFT,
            _count(df, predicate),
            total,
        ),
        _result(
            run_id,
            TABLE_METEO_DAILY,
            "observed_hours_above_expected",
            SOFT,
            _count(df, F.col("observed_hours") > F.col("expected_hours")),
            total,
        ),
    ]


def build_report(spark: SparkSession, results: list[dict]) -> DataFrame:
    if not results:
        return spark.createDataFrame([], DQ_REPORT_SCHEMA)
    return spark.createDataFrame(results, DQ_REPORT_SCHEMA)


def freshness_cutoff(settings: SilverSettings) -> str:
    return (local_today() - timedelta(days=settings.freshness_days)).isoformat()


__all__ = [
    "SilverQualityError",
    "assert_no_duplicates",
    "build_report",
    "freshness_cutoff",
    "run_gage_checks",
    "run_meteo_checks",
    "run_station_checks",
    "run_usgs_daily_checks",
    "validate_contract",
]
