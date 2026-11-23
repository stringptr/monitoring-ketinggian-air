"""Fast tests for the gold specification.

These do not need Spark, so they can run anywhere the package is importable.
"""

from __future__ import annotations

import inspect
from datetime import datetime, timezone

from gold import version
from gold.config import (
    FEATURE_LOOKBACK_HOURS,
    STAGE_COMPLETE_MIN_SAMPLES,
    TARGET_HORIZONS,
    GoldSettings,
    window_bounds,
)
from gold.schemas import FEATURE_COLS, TARGET_COLS
from gold.writer import write_table


def test_feature_contract_is_stable() -> None:
    assert len(FEATURE_COLS) == 48
    assert len(set(FEATURE_COLS)) == 48
    assert TARGET_COLS == ("y_h1", "y_h6", "y_h24")


def test_feature_names_match_notebook_prefixes() -> None:
    for column in FEATURE_COLS:
        assert column.islower()
    assert "stage_lag_168" in FEATURE_COLS
    assert "precip_sum_24_lag_24" in FEATURE_COLS
    assert "hour_sin" in FEATURE_COLS


def test_dataset_version_describes_the_definition() -> None:
    identifier = version.dataset_version()
    assert identifier.startswith("g1-")
    assert len(identifier) == len("g1-") + 8
    assert identifier == version.dataset_version()
    definition = version.definition()
    assert definition["features"] == list(FEATURE_COLS)
    assert definition["horizons"] == list(TARGET_HORIZONS)
    assert definition["stage_complete_min_samples"] == STAGE_COMPLETE_MIN_SAMPLES


def test_dataset_version_depends_on_discharge_source() -> None:
    assert version.dataset_version("hourly") != version.dataset_version("daily")
    assert version.definition("hourly")["discharge_source"] == "hourly"


def test_window_includes_feature_history_and_target_horizon() -> None:
    settings = GoldSettings(
        historic_start_date="2024-01-01",
        lookback_days=7,
        feature_lookback_hours=FEATURE_LOOKBACK_HOURS,
        min_trainable_ratio=0.85,
        discharge_source="auto",
        s3_endpoint_url=None,
        s3_region="garage",
        s3_bucket_silver="silver",
        s3_bucket_gold="gold",
        local_output_dir="/tmp",
        access_key=None,
        secret_key=None,
    )
    start, end = window_bounds(settings, "2026-11-01", "2026-11-07")

    assert start == datetime(2026, 10, 25, tzinfo=timezone.utc)
    assert end == datetime(2026, 11, 8, 23, tzinfo=timezone.utc)


def test_gold_is_partitioned_by_utc_date() -> None:
    default = inspect.signature(write_table).parameters["partition_cols"].default
    assert default == ("station_id", "date_utc")
