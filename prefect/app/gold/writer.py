"""Writers for the gold tables, the DQ report and the run log.

Gold publishes in two steps. A run first writes both tables under
``gold/_staging/run_id=...``; only when both writes succeed are the files copied
to their final partitions and the staging prefix removed. The ``dataset_versions``
row is written last, so a run that fails at any point leaves no committed marker.

Object stores have no cross-object transactions. The staging step does not make
the publish atomic either, but it narrows the failure window to a copy of files
that already exist and keeps the marker honest.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from gold.config import TABLE_DQ, GoldSettings, local_uri

STAGING_DIR = "_staging"

_S3_CLIENTS: dict[tuple, object] = {}


def _s3_filesystem(settings: GoldSettings):
    key = (
        settings.s3_endpoint_url,
        settings.s3_region,
        settings.access_key,
        settings.secret_key,
    )
    filesystem = _S3_CLIENTS.get(key)
    if filesystem is None:
        import s3fs

        filesystem = s3fs.S3FileSystem(
            key=settings.access_key,
            secret=settings.secret_key,
            client_kwargs={
                "endpoint_url": settings.s3_endpoint_url,
                "region_name": settings.s3_region,
            },
        )
        _S3_CLIENTS[key] = filesystem
    return filesystem


def gold_root(settings: GoldSettings) -> str:
    if settings.s3_endpoint_url:
        return f"s3a://{settings.s3_bucket_gold}"
    return local_uri(Path(settings.local_output_dir) / "gold")


def table_path(settings: GoldSettings, table: str) -> str:
    return f"{gold_root(settings)}/{table}"


def staging_root(settings: GoldSettings, run_id: str) -> str:
    """Spark path of the staging area for one run."""

    return f"{gold_root(settings)}/{STAGING_DIR}/run_id={run_id}"


def _is_empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0


def write_table(
    spark: SparkSession,
    df: DataFrame,
    settings: GoldSettings,
    table: str,
    partition_cols: tuple[str, ...] = ("station_id", "date_utc"),
    base: str | None = None,
) -> str | None:
    """Write only the partitions present in ``df``.

    ``base`` points at a staging root; when omitted the table is written in
    place (used for small tables that do not need staging).
    """

    if _is_empty(df):
        return None

    path = f"{base or gold_root(settings)}/{table}"
    (
        df.repartition(*partition_cols)
        .write.mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(path)
    )
    return path


def publish_table(
    settings: GoldSettings,
    table: str,
    run_id: str,
) -> int:
    """Copy one staged table into its final partitions and remove the staging."""

    if settings.s3_endpoint_url:
        filesystem = _s3_filesystem(settings)
        source = f"{settings.s3_bucket_gold}/{STAGING_DIR}/run_id={run_id}/{table}"
        target = f"{settings.s3_bucket_gold}/{table}"
        try:
            entries = filesystem.find(source)
        except FileNotFoundError:
            return 0

        # Spark also writes zero-byte directory markers; treat only real
        # parquet files as data.
        entries = [entry for entry in entries if entry.endswith(".parquet")]
        if not entries:
            return 0

        # Replace whole partitions, not just add files: a previous run used
        # different part-file names, so copying without removing would leave
        # duplicates behind.
        partitions = {entry[len(source) + 1 :].rsplit("/", 1)[0] for entry in entries}
        for partition in partitions:
            destination = f"{target}/{partition}"
            if filesystem.exists(destination):
                filesystem.rm(destination, recursive=True)

        copied = 0
        for entry in entries:
            relative = entry[len(source) + 1 :]
            destination = f"{target}/{relative}"
            with filesystem.open(entry, "rb") as reader, filesystem.open(
                destination, "wb"
            ) as writer:
                shutil.copyfileobj(reader, writer)
            copied += 1

        # Remove station-level directory markers copied by earlier runs; they
        # make pyarrow treat a prefix as a zero-byte parquet file.
        stations = {partition.split("/")[0] for partition in partitions}
        for station in stations:
            marker = f"{target}/{station}"
            try:
                info = filesystem.info(marker)
            except FileNotFoundError:
                continue
            if info.get("type") == "file":
                filesystem.rm_file(marker)

        filesystem.rm(source, recursive=True)
        return copied

    base = Path(settings.local_output_dir) / "gold"
    source = base / STAGING_DIR / f"run_id={run_id}" / table
    target = base / table
    if not source.is_dir():
        return 0

    partitions = {
        entry.parent.relative_to(source) for entry in source.rglob("*.parquet")
    }
    for partition in partitions:
        destination = target / partition
        if destination.is_dir():
            shutil.rmtree(destination)

    copied = 0
    for entry in source.rglob("*.parquet"):
        destination = target / entry.relative_to(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(entry, destination)
        copied += 1
    shutil.rmtree(source)
    return copied


def cleanup_staging(settings: GoldSettings, run_id: str) -> None:
    """Remove an empty staging run directory, if any."""

    if settings.s3_endpoint_url:
        filesystem = _s3_filesystem(settings)
        directory = f"{settings.s3_bucket_gold}/{STAGING_DIR}/run_id={run_id}"
        if filesystem.exists(directory):
            filesystem.rm(directory, recursive=True)
        return

    directory = (
        Path(settings.local_output_dir) / "gold" / STAGING_DIR / f"run_id={run_id}"
    )
    if directory.is_dir():
        shutil.rmtree(directory)


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
