"""Writers for the gold Delta tables, the DQ report and the run log.

Gold tables are Delta Lake. The DQ report remains Parquet because it is an
operational side table, not a serving table. Writes are atomic at table level,
so a failed run does not leave a half-published gold table.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from gold.config import TABLE_DQ, GoldSettings, local_uri
from gold.delta import write_delta


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
    predicate: str | None = None,
) -> str | None:
    """Write the window to a Delta table, replacing only matching rows."""

    if _is_empty(df):
        return None

    frame = df.toPandas()
    if frame.empty:
        return None

    return write_delta(
        frame,
        settings,
        table,
        mode="overwrite",
        predicate=predicate,
        partition_by=list(partition_cols),
    )


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

    frame = df.toPandas()
    if frame.empty:
        return None
    return write_delta(frame, settings, table, mode="append")
