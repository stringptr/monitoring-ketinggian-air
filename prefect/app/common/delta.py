"""Shared Delta Lake helpers for the local medallion layers.

Spark Delta cannot be used against Garage: S3A turns a missing
``_last_checkpoint``/``_delta_log`` into HTTP 400 instead of 404. The local
pipeline therefore uses ``delta-rs`` for both writing and reading. On Azure the
same tables work with Spark Delta on ADLS.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

DEFAULT_BUCKET_ATTR = "s3_bucket_gold"
DEFAULT_ROOT = "gold"


def storage_options(settings) -> dict[str, str]:
    if not settings.s3_endpoint_url:
        return {}
    return {
        "AWS_ENDPOINT_URL": settings.s3_endpoint_url.rstrip("/"),
        "AWS_ACCESS_KEY_ID": settings.access_key or "",
        "AWS_SECRET_ACCESS_KEY": settings.secret_key or "",
        "AWS_REGION": settings.s3_region,
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_ALLOW_HTTP": "true",
    }


def table_uri(
    settings,
    table: str,
    *,
    bucket_attr: str = DEFAULT_BUCKET_ATTR,
    root: str = DEFAULT_ROOT,
) -> str:
    if settings.s3_endpoint_url:
        bucket = getattr(settings, bucket_attr)
        return f"s3://{bucket}/delta/{table}"
    return str(Path(settings.local_output_dir) / root / "delta" / table)


def table_exists(
    settings,
    table: str,
    *,
    bucket_attr: str = DEFAULT_BUCKET_ATTR,
    root: str = DEFAULT_ROOT,
) -> bool:
    from deltalake import DeltaTable

    try:
        DeltaTable(
            table_uri(settings, table, bucket_attr=bucket_attr, root=root),
            storage_options=storage_options(settings),
        )
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
    bucket_attr: str = DEFAULT_BUCKET_ATTR,
    root: str = DEFAULT_ROOT,
) -> str:
    from deltalake import write_deltalake

    uri = table_uri(settings, table, bucket_attr=bucket_attr, root=root)
    options: dict = {
        "storage_options": storage_options(settings),
        "mode": mode,
    }
    if partition_by:
        options["partition_by"] = list(partition_by)
    if predicate and table_exists(
        settings, table, bucket_attr=bucket_attr, root=root
    ):
        options["predicate"] = predicate

    write_deltalake(uri, frame, **options)
    return uri


def read_delta(
    settings,
    table: str,
    *,
    columns: list[str] | None = None,
    filters: list[tuple[str, str, object]] | None = None,
    bucket_attr: str = DEFAULT_BUCKET_ATTR,
    root: str = DEFAULT_ROOT,
) -> pd.DataFrame:
    from deltalake import DeltaTable

    dataset = DeltaTable(
        table_uri(settings, table, bucket_attr=bucket_attr, root=root),
        storage_options=storage_options(settings),
    )
    return dataset.to_pandas(columns=columns, filters=filters)


def history(
    settings,
    table: str,
    *,
    bucket_attr: str = DEFAULT_BUCKET_ATTR,
    root: str = DEFAULT_ROOT,
) -> list[dict]:
    from deltalake import DeltaTable

    dataset = DeltaTable(
        table_uri(settings, table, bucket_attr=bucket_attr, root=root),
        storage_options=storage_options(settings),
    )
    return list(dataset.history())
