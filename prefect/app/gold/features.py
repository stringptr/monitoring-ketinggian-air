"""Feature engineering for the gold layer.

The column names and windows mirror the modelling notebook, so a model trained
from this table is comparable with the notebook result. Spark's aggregate
functions ignore nulls, therefore every rolling window also counts its
non-null samples and returns ``null`` when fewer than 90 percent of the window
is present. Without that guard a window with a single value would look like a
complete one.
"""

from __future__ import annotations

import math
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from gold.schemas import FEATURE_COLS, TARGET_COLS

STAGE_LAGS: tuple[int, ...] = (1, 2, 3, 6, 12, 24, 48, 72, 168)
STAGE_WINDOWS: tuple[int, ...] = (3, 6, 12, 24, 72, 168)
STAGE_DIFFS: tuple[int, ...] = (1, 3, 6, 24)
DISCHARGE_LAGS: tuple[int, ...] = (1, 3, 6, 24, 72)
PRECIP_WINDOWS: tuple[int, ...] = (1, 3, 6, 24, 72, 168)

MIN_PERIOD_RATIO = 0.9
STAGE_COLUMN = "stage_mean_ft"
PRECIP_COLUMN = "precip_mm"
DISCHARGE_COLUMN = "discharge_model_cfs"

_PRECIP_24 = "_precip_sum_24"
_PRECIP_72 = "_precip_sum_72"


def _ordering() -> Window:
    return Window.partitionBy("station_id").orderBy("time_utc")


def _rolling(n: int) -> Window:
    return _ordering().rowsBetween(-(n - 1), 0)


def _min_periods(n: int) -> int:
    return max(1, math.ceil(MIN_PERIOD_RATIO * n))


def _rolling_value(column: str, n: int, how: str) -> F.Column:
    """Aggregate over the last ``n`` rows, refusing windows that are too sparse."""

    window = _rolling(n)
    aggregate = {"mean": F.avg, "max": F.max, "sum": F.sum}[how](
        F.col(column)
    ).over(window)
    samples = F.count(F.col(column)).over(window)
    return (
        F.when(samples >= _min_periods(n), aggregate)
        .otherwise(F.lit(None))
        .cast("double")
    )


def _stage_features() -> list[F.Column]:
    stage = F.col(STAGE_COLUMN)
    features: list[F.Column] = [stage.cast("double").alias("stage_now")]

    for lag in STAGE_LAGS:
        features.append(
            F.lag(stage, lag).over(_ordering()).cast("double").alias(f"stage_lag_{lag}")
        )
    for window in STAGE_WINDOWS:
        features.append(
            _rolling_value(STAGE_COLUMN, window, "mean").alias(f"stage_mean_{window}")
        )
        features.append(
            _rolling_value(STAGE_COLUMN, window, "max").alias(f"stage_max_{window}")
        )
    for diff in STAGE_DIFFS:
        features.append(
            (stage - F.lag(stage, diff).over(_ordering()))
            .cast("double")
            .alias(f"stage_diff_{diff}")
        )

    return features


def _discharge_features() -> list[F.Column]:
    discharge = F.col(DISCHARGE_COLUMN)
    return [
        F.lag(discharge, lag).over(_ordering()).cast("double").alias(
            f"discharge_lag_{lag}"
        )
        for lag in DISCHARGE_LAGS
    ]


def _precip_features() -> list[F.Column]:
    return [
        _rolling_value(PRECIP_COLUMN, window, "sum").alias(f"precip_sum_{window}")
        for window in PRECIP_WINDOWS
    ]


def _time_features() -> list[F.Column]:
    """Encode the local hour and day of year as sine/cosine pairs."""

    hour = F.hour(F.col("time_local"))
    day_of_year = F.dayofyear(F.col("time_local"))
    return [
        F.sin(2 * math.pi * hour / 24).cast("double").alias("hour_sin"),
        F.cos(2 * math.pi * hour / 24).cast("double").alias("hour_cos"),
        F.sin(2 * math.pi * day_of_year / 365.25).cast("double").alias("doy_sin"),
        F.cos(2 * math.pi * day_of_year / 365.25).cast("double").alias("doy_cos"),
    ]


def add_features(df: DataFrame, horizons: tuple[int, ...]) -> DataFrame:
    """Add the model features and the future targets.

    The two lagged precipitation columns are computed through temporary
    columns because Spark does not allow a window function inside another
    window function.
    """

    frame = df.select(
        "*",
        *_stage_features(),
        *_discharge_features(),
        *_precip_features(),
        *_time_features(),
    )

    frame = (
        frame.withColumn(_PRECIP_24, _rolling_value(PRECIP_COLUMN, 24, "sum"))
        .withColumn(_PRECIP_72, _rolling_value(PRECIP_COLUMN, 72, "sum"))
        .withColumn(
            "precip_sum_24_lag_24",
            F.lag(F.col(_PRECIP_24), 24).over(_ordering()).cast("double"),
        )
        .withColumn(
            "precip_sum_72_lag_24",
            F.lag(F.col(_PRECIP_72), 24).over(_ordering()).cast("double"),
        )
    )

    frame = frame.select(
        "*",
        *[
            F.lead(F.col(STAGE_COLUMN), horizon)
            .over(_ordering())
            .cast("double")
            .alias(f"y_h{horizon}")
            for horizon in horizons
        ],
    )
    return frame.drop(_PRECIP_24, _PRECIP_72)


def add_flags(df: DataFrame) -> DataFrame:
    """Add the water-year flag and the trainable-row flag."""

    max_date = df.agg(F.max("date_local")).collect()[0][0]
    current_year = 0
    if max_date:
        year = int(max_date[:4])
        month = int(max_date[5:7])
        current_year = year + (1 if month >= 10 else 0)

    return (
        df.withColumn("is_partial", F.col("water_year") >= F.lit(current_year))
        .withColumn(
            "is_trainable",
            ~reduce(
                lambda left, right: left | right,
                [F.col(column).isNull() for column in (*FEATURE_COLS, *TARGET_COLS)],
            ),
        )
    )


def build_features(df: DataFrame, horizons: tuple[int, ...]) -> DataFrame:
    """Return the feature table with one row per station and hour."""

    featured = add_features(df, horizons)
    flagged = add_flags(featured)
    ordered = [column for column in df.columns if column not in FEATURE_COLS]
    return flagged.select(
        *ordered,
        *FEATURE_COLS,
        *TARGET_COLS,
        "is_partial",
        "is_trainable",
    )
