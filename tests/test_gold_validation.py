"""Validation must stop a bad dataset before anything is written."""

from __future__ import annotations

from types import SimpleNamespace
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from common.spark import build_spark_session
from gold.schemas import FEATURES_SCHEMA
from gold.validation import GoldValidationError, validate_features

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


def _row(station: str, timestamp: str) -> dict:
    row = {field.name: None for field in FEATURES_SCHEMA.fields}
    row["station_id"] = station
    row["time_utc"] = datetime.fromisoformat(timestamp)
    return row


def test_duplicate_keys_are_rejected(spark: SparkSession) -> None:
    frame = spark.createDataFrame(
        [
            _row("11525670", "2026-11-01 08:00:00"),
            _row("11525670", "2026-11-01 09:00:00"),
            _row("11525670", "2026-11-01 09:00:00"),
        ],
        FEATURES_SCHEMA,
    )
    with pytest.raises(GoldValidationError, match="duplicate"):
        validate_features(frame, "features")


def test_empty_frame_is_rejected(spark: SparkSession) -> None:
    frame = spark.createDataFrame([], FEATURES_SCHEMA)
    with pytest.raises(GoldValidationError, match="no rows"):
        validate_features(frame, "features")


def test_valid_frame_passes(spark: SparkSession) -> None:
    frame = spark.createDataFrame(
        [
            _row("11525670", "2026-11-01 08:00:00"),
            _row("11525670", "2026-11-01 09:00:00"),
        ],
        FEATURES_SCHEMA,
    )
    validate_features(frame, "features")
