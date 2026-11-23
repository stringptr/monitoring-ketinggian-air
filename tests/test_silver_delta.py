"""Silver Delta tables use the silver bucket and local root."""

from __future__ import annotations

import pandas as pd

from silver.config import SilverSettings
from silver.delta import history, read_delta, table_exists, write_delta


def _settings(tmp_path) -> SilverSettings:
    return SilverSettings(
        historic_start_date="2015-10-01",
        lookback_days=14,
        s3_endpoint_url=None,
        s3_region="garage",
        s3_bucket_bronze="bronze",
        s3_bucket_silver="silver",
        local_output_dir=str(tmp_path),
        usgs_completeness_min=0.90,
        meteo_completeness_min=0.95,
        max_gap_hours=3,
        freshness_days=2,
        access_key=None,
        secret_key=None,
    )


def _frame(values: list[float], dates: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station_id": ["11150500"] * len(values),
            "ingest_date": dates,
            "time_utc": pd.to_datetime(dates, utc=True),
            "value": values,
        }
    )


def test_silver_delta_roundtrip(tmp_path) -> None:
    settings = _settings(tmp_path)
    path = "usgs/continuous/parameter_code=00065"
    write_delta(
        _frame([1.0, 2.0], ["2026-11-01", "2026-11-02"]),
        settings,
        path,
        partition_by=["station_id", "ingest_date"],
    )

    assert table_exists(settings, path)
    frame = read_delta(settings, path)
    assert len(frame) == 2
    assert len(history(settings, path)) == 1


def test_silver_delta_predicate_overwrite(tmp_path) -> None:
    settings = _settings(tmp_path)
    path = "usgs/continuous/parameter_code=00065"
    write_delta(
        _frame([1.0, 2.0], ["2026-11-01", "2026-11-02"]),
        settings,
        path,
        partition_by=["station_id", "ingest_date"],
    )
    write_delta(
        _frame([99.0], ["2026-11-02"]),
        settings,
        path,
        mode="overwrite",
        predicate="ingest_date >= '2026-11-02' AND ingest_date <= '2026-11-02'",
        partition_by=["station_id", "ingest_date"],
    )

    frame = read_delta(settings, path).sort_values("ingest_date")
    assert frame["value"].tolist() == [1.0, 99.0]
    assert len(history(settings, path)) == 2
