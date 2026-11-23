"""Continuous 00060 must be a first-class silver table."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession

from common.spark import build_spark_session
from silver.config import CONTINUOUS_SPECS
from silver.quality import run_continuous_checks
from silver.readers import continuous_prefix
from stations import STATIONS

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = build_spark_session(
        SimpleNamespace(
            s3_endpoint_url=None,
            s3_region="garage",
            access_key=None,
            secret_key=None,
        )
    )
    yield session
    session.stop()


def test_specs_cover_stage_and_discharge() -> None:
    by_code = {spec.parameter_code: spec for spec in CONTINUOUS_SPECS}
    assert set(by_code) == {"00065", "00060"}
    assert by_code["00065"].expected_unit == "ft"
    assert by_code["00060"].expected_unit == "ft^3/s"
    assert by_code["00060"].table == "usgs_continuous_00060"


def test_prefix_uses_the_requested_parameter() -> None:
    station = STATIONS[0]
    assert "parameter_code=00060" in continuous_prefix(station, "00060")
    assert "parameter_code=00065" in continuous_prefix(station, "00065")


def _frame(spark: SparkSession, unit: str):
    return spark.createDataFrame(
        [
            (
                "11150500",
                "2026-11-01",
                datetime(2026, 11, 1, 0, 0),
                1200.0,
                "Approved",
                unit,
            )
        ],
        "station_id string, ingest_date string, time_utc timestamp, "
        "value double, approval_status string, unit string",
    )


def _skeleton(spark: SparkSession):
    return spark.createDataFrame(
        [("11150500", "2026-11-01", 96, 24, 1)],
        "station_id string, ingest_date string, expected_samples int, "
        "expected_hours int, expected_daily int",
    )


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        usgs_completeness_min=0.90,
        meteo_completeness_min=0.95,
        max_gap_hours=3,
        freshness_days=2,
    )


def test_discharge_unit_is_cfs(spark: SparkSession) -> None:
    results = run_continuous_checks(
        _frame(spark, "ft^3/s"),
        _skeleton(spark),
        _settings(),
        "run",
        False,
        table="usgs_continuous_00060",
        expected_unit="ft^3/s",
    )
    unit = [row for row in results if row["check_name"] == "unit_consistency"][0]
    assert unit["passed"] is True
    assert all(row["table"] == "usgs_continuous_00060" for row in results)


def test_wrong_unit_fails(spark: SparkSession) -> None:
    results = run_continuous_checks(
        _frame(spark, "ft"),
        _skeleton(spark),
        _settings(),
        "run",
        False,
        table="usgs_continuous_00060",
        expected_unit="ft^3/s",
    )
    unit = [row for row in results if row["check_name"] == "unit_consistency"][0]
    assert unit["passed"] is False
    assert unit["failed_count"] == 1
