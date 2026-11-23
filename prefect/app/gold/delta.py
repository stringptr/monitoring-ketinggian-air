"""Gold Delta helpers.

Thin wrappers over :mod:`common.delta` so callers keep the gold-specific API.
"""

from __future__ import annotations

import pandas as pd

from common.delta import (
    history as _history,
    read_delta as _read_delta,
    storage_options as _storage_options,
    table_exists as _table_exists,
    table_uri as _table_uri,
    write_delta as _write_delta,
)

GOLD_BUCKET_ATTR = "s3_bucket_gold"
GOLD_ROOT = "gold"


def storage_options(settings) -> dict[str, str]:
    return _storage_options(settings)


def table_uri(settings, table: str) -> str:
    return _table_uri(
        settings, table, bucket_attr=GOLD_BUCKET_ATTR, root=GOLD_ROOT
    )


def table_exists(settings, table: str) -> bool:
    return _table_exists(
        settings, table, bucket_attr=GOLD_BUCKET_ATTR, root=GOLD_ROOT
    )


def write_delta(frame: pd.DataFrame, settings, table: str, **kwargs) -> str:
    return _write_delta(
        frame,
        settings,
        table,
        bucket_attr=GOLD_BUCKET_ATTR,
        root=GOLD_ROOT,
        **kwargs,
    )


def read_delta(settings, table: str, **kwargs) -> pd.DataFrame:
    return _read_delta(
        settings,
        table,
        bucket_attr=GOLD_BUCKET_ATTR,
        root=GOLD_ROOT,
        **kwargs,
    )


def history(settings, table: str) -> list[dict]:
    return _history(
        settings, table, bucket_attr=GOLD_BUCKET_ATTR, root=GOLD_ROOT
    )
