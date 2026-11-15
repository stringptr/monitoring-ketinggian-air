"""Idempotent writers for the native-resolution silver tables, DQ and quarantine."""

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
    table_path_name: str,
    partition_cols: tuple[str, ...] = ("station_id", "ingest_date"),
) -> str | None:
    """Overwrite only the station/ingest_date partitions present in ``df``."""

    if _is_empty(df):
        return None

    path = table_path(settings, table_path_name)
    (
        df.repartition(*partition_cols)
        .write.mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(path)
    )
    return path


def _valid_ingest_date(column: str = "ingest_date") -> F.Column:
    return F.when(
        F.col(column).isNull() | (F.col(column) == ""),
        F.lit("unknown"),
    ).otherwise(F.col(column))


def write_quarantine(
    spark: SparkSession,
    df: DataFrame,
    settings: SilverSettings,
    table: str,
) -> str | None:
    if _is_empty(df):
        return None

    prepared = df.withColumn("ingest_date", _valid_ingest_date("ingest_date"))
    path = table_path(settings, f"{TABLE_QUARANTINE}/table={table}")
    (
        prepared.repartition("station_id", "ingest_date")
        .write.mode("overwrite")
        .partitionBy("station_id", "ingest_date")
        .parquet(path)
    )
    return path


def write_dq_report(
    spark: SparkSession,
    df: DataFrame,
    settings: SilverSettings,
    table: str,
) -> str | None:
    if _is_empty(df):
        return None

    prepared = df.withColumn("ingest_date", _valid_ingest_date("ingest_date"))
    path = table_path(settings, f"{TABLE_DQ}/table={table}")
    (
        prepared.repartition("ingest_date")
        .write.mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(path)
    )
    return path
