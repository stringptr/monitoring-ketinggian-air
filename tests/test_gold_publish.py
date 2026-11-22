"""Publishing copies a staged table into place and removes the staging area."""

from __future__ import annotations

from pathlib import Path

from gold.config import GoldSettings
from gold.writer import publish_table, staging_root


def _settings(tmp_path: Path) -> GoldSettings:
    return GoldSettings(
        historic_start_date="2024-01-01",
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


def _staged_file(tmp_path: Path, run_id: str, table: str) -> Path:
    path = (
        tmp_path
        / "gold"
        / "_staging"
        / f"run_id={run_id}"
        / table
        / "station_id=11525670"
        / "date_utc=2026-11-01"
    )
    path.mkdir(parents=True)
    target = path / "part-00000.parquet"
    target.write_bytes(b"parquet")
    return target


def test_publish_moves_staged_files(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    source = _staged_file(tmp_path, "r1", "station_hourly")

    assert staging_root(settings, "r1").endswith("_staging/run_id=r1")
    copied = publish_table(settings, "station_hourly", "r1")

    destination = (
        tmp_path
        / "gold"
        / "station_hourly"
        / "station_id=11525670"
        / "date_utc=2026-11-01"
        / "part-00000.parquet"
    )
    assert copied == 1
    assert destination.read_bytes() == b"parquet"
    assert not source.exists()


def test_publish_replaces_partition_instead_of_adding_files(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    existing = (
        tmp_path
        / "gold"
        / "station_hourly"
        / "station_id=11525670"
        / "date_utc=2026-11-01"
    )
    existing.mkdir(parents=True)
    (existing / "part-00000-old.parquet").write_bytes(b"old")

    _staged_file(tmp_path, "r2", "station_hourly")
    publish_table(settings, "station_hourly", "r2")

    files = list(existing.glob("*.parquet"))
    assert len(files) == 1
    assert files[0].read_bytes() == b"parquet"


def test_publish_without_staging_is_a_noop(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    assert publish_table(settings, "station_hourly", "missing") == 0
    assert not (tmp_path / "gold" / "station_hourly").exists()
