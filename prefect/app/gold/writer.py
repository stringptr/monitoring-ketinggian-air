"""Idempotent writers for the gold tables, the DQ report and the run log."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

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
    if _is_empty(df):
        return None

    prepared = df.withColumn(
        "date",
        F.when(
            F.col("date").isNull() | (F.col("date") == ""),
            F.lit("unknown"),
        ).otherwise(F.col("date")),
    )
    path = table_path(settings, f"{TABLE_DQ}/table={table}")
    (
        prepared.repartition("date")
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(path)
    )
    return path


def write_dataset_version(
    spark: SparkSession,
    df: DataFrame,
    settings: GoldSettings,
    table: str,
) -> str | None:
    """Write one row per dataset version, overwriting that version on rerun."""

    if _is_empty(df):
        return None

    path = table_path(settings, table)
    (
        df.write.mode("overwrite")
        .partitionBy("dataset_version")
        .parquet(path)
    )
    return path
