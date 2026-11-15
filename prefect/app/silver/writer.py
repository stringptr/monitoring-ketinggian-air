"""Idempotent writers for the silver tables, DQ reports and quarantine."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver.config import TABLE_DQ, TABLE_QUARANTINE, SilverSettings, local_uri


def silver_root(settings: SilverSettings) -> str:
    if settings.s3_endpoint_url:
        return f"s3a://{settings.s3_bucket_silver}"
    return local_uri(Path(settings.local_output_dir) / "silver")


def table_path(settings: SilverSettings, table: str) -> str:
    return f"{silver_root(settings)}/{table}"


def _is_empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0


def write_table(
    spark: SparkSession,
    df: DataFrame,
    settings: SilverSettings,
    table: str,
    partition_cols: tuple[str, ...] = ("station_id", "date"),
) -> str | None:
    """Overwrite the partitions present in ``df`` and leave the rest intact.

    ``spark.sql.sources.partitionOverwriteMode`` is set to ``dynamic`` in the
    session factory, so a daily run only rewrites the stations/dates it
    actually processed.
    """

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


def write_quarantine(
    spark: SparkSession,
    df: DataFrame,
    settings: SilverSettings,
    table: str,
) -> str | None:
    if _is_empty(df):
        return None

    prepared = df.withColumn("date", F.coalesce(F.col("date"), F.lit("unknown")))
    path = table_path(settings, f"{TABLE_QUARANTINE}/table={table}")
    (
        prepared.repartition("station_id", "date")
        .write.mode("overwrite")
        .partitionBy("station_id", "date")
        .parquet(path)
    )
    return path


def write_dq_report(
    spark: SparkSession,
    df: DataFrame,
    settings: SilverSettings,
    table: str,
    run_id: str,
) -> str | None:
    if _is_empty(df):
        return None

    path = table_path(
        settings,
        f"{TABLE_DQ}/table={table}/run_id={run_id}",
    )
    df.write.mode("overwrite").parquet(path)
    return path
