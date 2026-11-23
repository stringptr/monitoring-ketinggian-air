"""Spark schemas and column contracts for the gold layer.

The feature names follow the modelling notebook exactly, so the model code can
be moved from the notebook into the pipeline without renaming anything. The
canonical weather names are the ones used by the notebook; the mapping from
the silver column names lives in ``METEO_SOURCE_MAP``.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

#: Canonical weather columns kept in gold, in a stable order.
METEO_COLUMNS: tuple[str, ...] = (
    "precip_mm",
    "rain_mm",
    "temp_c",
    "rh_pct",
    "dewpoint_c",
    "soil_moist_0_7",
    "soil_moist_7_28",
    "soil_moist_28_100",
    "et0_mm",
    "wind_kmh",
    "cloud_pct",
    "swrad_wm2",
    "pressure_hpa",
)

#: silver column -> canonical gold column.
METEO_SOURCE_MAP: dict[str, str] = {
    "precipitation": "precip_mm",
    "rain": "rain_mm",
    "temperature_2m": "temp_c",
    "relative_humidity_2m": "rh_pct",
    "dew_point_2m": "dewpoint_c",
    "soil_moisture_0_to_7cm": "soil_moist_0_7",
    "soil_moisture_7_to_28cm": "soil_moist_7_28",
    "soil_moisture_28_to_100cm": "soil_moist_28_100",
    "et0_fao_evapotranspiration": "et0_mm",
    "wind_speed_10m": "wind_kmh",
    "cloud_cover": "cloud_pct",
    "shortwave_radiation": "swrad_wm2",
    # The pipeline ingests ``surface_pressure``; the notebook names the
    # variable ``pressure_hpa``. The difference (surface vs mean sea level) is
    # recorded in GOLD.md and does not affect the current feature set.
    "surface_pressure": "pressure_hpa",
}

#: Feature columns, identical to ``FEATURE_COLS`` in the notebook.
FEATURE_COLS: tuple[str, ...] = (
    "stage_now",
    "stage_lag_1",
    "stage_lag_2",
    "stage_lag_3",
    "stage_lag_6",
    "stage_lag_12",
    "stage_lag_24",
    "stage_lag_48",
    "stage_lag_72",
    "stage_lag_168",
    "stage_mean_3",
    "stage_mean_6",
    "stage_mean_12",
    "stage_mean_24",
    "stage_mean_72",
    "stage_mean_168",
    "stage_max_3",
    "stage_max_6",
    "stage_max_12",
    "stage_max_24",
    "stage_max_72",
    "stage_max_168",
    "stage_diff_1",
    "stage_diff_3",
    "stage_diff_6",
    "stage_diff_24",
    "discharge_lag_1",
    "discharge_lag_3",
    "discharge_lag_6",
    "discharge_lag_24",
    "discharge_lag_72",
    "precip_sum_1",
    "precip_sum_3",
    "precip_sum_6",
    "precip_sum_24",
    "precip_sum_72",
    "precip_sum_168",
    "precip_sum_24_lag_24",
    "precip_sum_72_lag_24",
    "soil_moist_0_7",
    "soil_moist_7_28",
    "temp_c",
    "et0_mm",
    "rh_pct",
    "hour_sin",
    "hour_cos",
    "doy_sin",
    "doy_cos",
)

TARGET_COLS: tuple[str, ...] = ("y_h1", "y_h6", "y_h24")

STAGE_HOURLY_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("time_utc", TimestampType()),
        StructField("time_local", TimestampType()),
        StructField("date_local", StringType()),
        StructField("date_utc", StringType()),
        StructField("year", IntegerType()),
        StructField("water_year", IntegerType()),
        StructField("stage_mean_ft", DoubleType()),
        StructField("stage_max_ft", DoubleType()),
        StructField("stage_min_ft", DoubleType()),
        StructField("stage_n", LongType()),
        StructField("stage_complete", BooleanType()),
        StructField("discharge_mean_cfs", DoubleType()),
        StructField("discharge_n", LongType()),
        StructField("discharge_daily_cfs", DoubleType()),
        StructField("discharge_model_cfs", DoubleType()),
        StructField("discharge_grain", StringType()),
    ]
    + [StructField(column, DoubleType()) for column in METEO_COLUMNS]
    + [
        StructField("meteo_n", LongType()),
        StructField("meteo_variables_available", LongType()),
        StructField("dataset_version", StringType()),
    ]
)

STATION_HOURLY_SCHEMA = STAGE_HOURLY_SCHEMA

FEATURES_SCHEMA = StructType(
    [field for field in STATION_HOURLY_SCHEMA.fields if field.name not in FEATURE_COLS]
    + [StructField(column, DoubleType()) for column in FEATURE_COLS]
    + [StructField(column, DoubleType()) for column in TARGET_COLS]
    + [
        StructField("is_partial", BooleanType()),
        StructField("is_trainable", BooleanType()),
    ]
)

PREDICTIONS_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("issued_at", TimestampType()),
        StructField("target_time", TimestampType()),
        StructField("horizon", IntegerType()),
        StructField("y_pred", DoubleType()),
        StructField("y_obs", DoubleType()),
        StructField("model_version", StringType()),
        StructField("dataset_version", StringType()),
        StructField("created_at", TimestampType()),
    ]
)

MODEL_METRICS_SCHEMA = StructType(
    [
        StructField("model_version", StringType()),
        StructField("station_id", StringType()),
        StructField("horizon", IntegerType()),
        StructField("split", StringType()),
        StructField("n", LongType()),
    ]
    + [
        StructField(metric, DoubleType())
        for metric in ("mae", "rmse", "bias", "nse", "kge", "pod", "far", "csi")
    ]
    + [StructField("generated_at", TimestampType())]
)

DATASET_VERSIONS_SCHEMA = StructType(
    [
        StructField("dataset_version", StringType()),
        StructField("run_id", StringType()),
        StructField("status", StringType()),
        StructField("data_start_date", StringType()),
        StructField("data_end_date", StringType()),
        StructField("processed_at", TimestampType()),
        StructField("max_time_utc", TimestampType()),
        StructField("rows_hourly", LongType()),
        StructField("rows_features", LongType()),
        StructField("rows_trainable", LongType()),
        StructField("feature_count", IntegerType()),
        StructField("discharge_grain", StringType()),
        StructField("created_at", TimestampType()),
    ]
)

DQ_REPORT_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("table", StringType()),
        StructField("scope", StringType()),
        StructField("date", StringType()),
        StructField("window_start", StringType()),
        StructField("window_end", StringType()),
        StructField("station_id", StringType()),
        StructField("check_name", StringType()),
        StructField("severity", StringType()),
        StructField("passed", BooleanType()),
        StructField("failed_count", IntegerType()),
        StructField("total_count", IntegerType()),
        StructField("details", StringType()),
        StructField("generated_at", StringType()),
    ]
)

__all__ = [
    "DATASET_VERSIONS_SCHEMA",
    "DQ_REPORT_SCHEMA",
    "FEATURES_SCHEMA",
    "FEATURE_COLS",
    "METEO_COLUMNS",
    "METEO_SOURCE_MAP",
    "MODEL_METRICS_SCHEMA",
    "PREDICTIONS_SCHEMA",
    "STAGE_HOURLY_SCHEMA",
    "STATION_HOURLY_SCHEMA",
    "TARGET_COLS",
]
