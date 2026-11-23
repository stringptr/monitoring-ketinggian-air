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


#: Features whose window reaches the full 168-hour lookback. A gap in silver
#: makes these null and the resulting dataset is not trainable, even though the
#: hourly grid still looks complete.
LOOKBACK_FEATURES: tuple[str, ...] = (
    "stage_lag_168",
    "stage_mean_168",
    "stage_max_168",
    "precip_sum_168",
)


def validate_lookback(df: DataFrame, max_null_ratio: float = 0.20) -> None:
    """Reject a dataset whose 168-hour lookback is mostly missing.

    The daily chain used to publish a gold table with zero trainable rows when
    silver had a multi-day gap, and still wrote a ``committed`` marker. This
    check turns that silent failure into a failed run.
    """

    total = df.count()
    if total == 0:
        raise GoldValidationError("features: no rows produced")

    for column in LOOKBACK_FEATURES:
        missing = df.filter(F.col(column).isNull()).count()
        ratio = missing / total
        if ratio > max_null_ratio:
            raise GoldValidationError(
                f"features: {column} is {ratio:.1%} null "
                f"(limit {max_null_ratio:.0%}); silver lookback is incomplete"
            )


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
