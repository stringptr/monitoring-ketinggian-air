"""Delta Lake gold tables: write, overwrite predicate, filters and history."""

from __future__ import annotations

import pandas as pd

from gold.config import GoldSettings
from gold.delta import history, read_delta, table_exists, write_delta


def _settings(tmp_path) -> GoldSettings:
    return GoldSettings(
        historic_start_date="2015-10-01",
        lookback_days=7,
        feature_lookback_hours=168,
        min_trainable_ratio=0.85,
        discharge_source="auto",
        s3_endpoint_url=None,
        s3_region="garage",
        s3_bucket_silver="silver",
        s3_bucket_gold="gold",
        local_output_dir=str(tmp_path),
        access_key=None,
        secret_key=None,
    )


def _frame(values: list[float], dates: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station_id": ["11150500"] * len(values),
            "date_utc": dates,
            "value": values,
        }
    )


def test_write_read_and_history(tmp_path) -> None:
    settings = _settings(tmp_path)
    write_delta(
        _frame([1.0, 2.0], ["2026-11-01", "2026-11-02"]),
        settings,
        "features",
        partition_by=["station_id", "date_utc"],
    )

    assert table_exists(settings, "features")
    frame = read_delta(settings, "features")
    assert len(frame) == 2
    assert sorted(frame["value"].tolist()) == [1.0, 2.0]
    assert len(history(settings, "features")) == 1


def test_predicate_overwrites_only_matching_rows(tmp_path) -> None:
    settings = _settings(tmp_path)
    write_delta(
        _frame([1.0, 2.0], ["2026-11-01", "2026-11-02"]),
        settings,
        "features",
        partition_by=["station_id", "date_utc"],
    )
    write_delta(
        _frame([99.0], ["2026-11-02"]),
        settings,
        "features",
        mode="overwrite",
        predicate="date_utc >= '2026-11-02' AND date_utc <= '2026-11-02'",
        partition_by=["station_id", "date_utc"],
    )

    frame = read_delta(settings, "features").sort_values("date_utc")
    assert frame["value"].tolist() == [1.0, 99.0]
    assert len(history(settings, "features")) == 2


def test_read_filters_and_columns(tmp_path) -> None:
    settings = _settings(tmp_path)
    write_delta(
        _frame([1.0, 2.0], ["2026-11-01", "2026-11-02"]),
        settings,
        "features",
        partition_by=["station_id", "date_utc"],
    )

    frame = read_delta(
        settings,
        "features",
        columns=["date_utc", "value"],
        filters=[("date_utc", ">=", "2026-11-02")],
    )
    assert len(frame) == 1
    assert frame["value"].iloc[0] == 2.0


def test_append_run_log(tmp_path) -> None:
    settings = _settings(tmp_path)
    write_delta(
        pd.DataFrame([{"run_id": "r1", "status": "committed"}]),
        settings,
        "dataset_versions",
        mode="append",
    )
    write_delta(
        pd.DataFrame([{"run_id": "r2", "status": "committed"}]),
        settings,
        "dataset_versions",
        mode="append",
    )

    frame = read_delta(settings, "dataset_versions")
    assert len(frame) == 2
    assert set(frame["run_id"]) == {"r1", "r2"}
