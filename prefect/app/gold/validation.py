"""Hard validation that runs before anything is written.

A hard failure here means the transformed dataset must not be published, so
these checks are separate from the soft data-quality report: the pipeline calls
this module first and only writes after it returns.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gold.schemas import FEATURES_SCHEMA


class GoldValidationError(RuntimeError):
    """Raised when the transformed dataset violates a hard rule."""


def _empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0


def validate_features(df: DataFrame, table: str) -> None:
    """Validate the feature table before it is published to gold."""

    missing = [column for column in FEATURES_SCHEMA.names if column not in df.columns]
    if missing:
        raise GoldValidationError(
            f"{table}: missing columns {', '.join(sorted(missing))}"
        )

    if _empty(df):
        raise GoldValidationError(f"{table}: no rows produced")

    duplicates = (
        df.groupBy("station_id", "time_utc")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicates:
        raise GoldValidationError(
            f"{table}: duplicate (station_id, time_utc) rows"
        )
