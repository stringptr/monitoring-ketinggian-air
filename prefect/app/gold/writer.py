"""Idempotent writers for the gold tables, the DQ report and the run log."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from gold.config import TABLE_DQ, GoldSettings, local_uri


def gold_root(settings: GoldSettings) -> str:
    if settings.s3_endpoint_url:
        return f"s3a://{settings.s3_bucket_gold}"
    return local_uri(Path(settings.local_output_dir) / "gold")


def table_path(settings: GoldSettings, table: str) -> str:
    return f"{gold_root(settings)}/{table}"


def _is_empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0


def write_table(
    spark: SparkSession,
    df: DataFrame,
    settings: GoldSettings,
    table: str,
    partition_cols: tuple[str, ...] = ("station_id", "date_utc"),
) -> str | None:
    """Overwrite only the partitions present in ``df``."""

    if _is_empty(df):
        return None

    path = table_path(settings, table)
    (
        df.repartition(*partition_cols)
        .write.mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(path)
    )
    return path


def write_dq_report(
    spark: SparkSession,
    df: DataFrame,
    settings: GoldSettings,
    table: str,
) -> str | None:
    """Write one report per run so window-level checks keep their range."""

    if _is_empty(df):
        return None

    path = table_path(settings, f"{TABLE_DQ}/table={table}")
    (
        df.repartition("run_id")
        .write.mode("overwrite")
        .partitionBy("run_id")
        .parquet(path)
    )
    return path


def write_dataset_version(
    spark: SparkSession,
    df: DataFrame,
    settings: GoldSettings,
    table: str,
) -> str | None:
    """Append one row per run; written last, so it marks a committed run."""

    if _is_empty(df):
        return None

    path = table_path(settings, table)
    df.write.mode("append").parquet(path)
    return path
