"""Delta Lake helpers for the gold layer.

Spark's Delta writer does not work against Garage because S3A turns a missing
``_last_checkpoint`` into HTTP 400 instead of 404. ``delta-rs`` handles the
object store correctly with unsafe rename, so the local pipeline uses it for
both writing and reading. On Azure, the same tables are Delta and can be read
by Spark or delta-rs.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def storage_options(settings) -> dict[str, str]:
    """Return delta-rs storage options for the configured backend."""

    if not settings.s3_endpoint_url:
        return {}
    return {
        "AWS_ENDPOINT_URL": settings.s3_endpoint_url.rstrip("/"),
        "AWS_ACCESS_KEY_ID": settings.access_key or "",
        "AWS_SECRET_ACCESS_KEY": settings.secret_key or "",
        "AWS_REGION": settings.s3_region,
        # Garage has no conditional put; single-writer local use is safe.
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_ALLOW_HTTP": "true",
    }


def table_uri(settings, table: str) -> str:
    if settings.s3_endpoint_url:
        return f"s3://{settings.s3_bucket_gold}/delta/{table}"
    return str(Path(settings.local_output_dir) / "gold" / "delta" / table)


def table_exists(settings, table: str) -> bool:
    from deltalake import DeltaTable

    try:
        DeltaTable(table_uri(settings, table), storage_options=storage_options(settings))
        return True
    except Exception:
        return False


def write_delta(
    frame: pd.DataFrame,
    settings,
    table: str,
    *,
    mode: str = "overwrite",
    predicate: str | None = None,
    partition_by: list[str] | None = None,
) -> str:
    """Write a pandas frame to a Delta table.

    ``predicate`` is a SQL expression (for example a date range). It is only
    applied when the table already exists; the first write creates the table.
    """

    from deltalake import write_deltalake

    uri = table_uri(settings, table)
    options: dict = {
        "storage_options": storage_options(settings),
        "mode": mode,
    }
    if partition_by:
        options["partition_by"] = list(partition_by)
    if predicate and table_exists(settings, table):
        options["predicate"] = predicate

    write_deltalake(uri, frame, **options)
    return uri


def read_delta(
    settings,
    table: str,
    *,
    columns: list[str] | None = None,
    filters: list[tuple[str, str, object]] | None = None,
) -> pd.DataFrame:
    from deltalake import DeltaTable

    dataset = DeltaTable(
        table_uri(settings, table),
        storage_options=storage_options(settings),
    )
    return dataset.to_pandas(columns=columns, filters=filters)


def history(settings, table: str) -> list[dict]:
    from deltalake import DeltaTable

    dataset = DeltaTable(
        table_uri(settings, table),
        storage_options=storage_options(settings),
    )
    return list(dataset.history())
