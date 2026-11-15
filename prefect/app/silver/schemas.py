"""Spark schemas and pandera contracts for the silver layer.

Bronze stores the upstream payload verbatim, so the reader schemas describe the
raw USGS/Open-Meteo JSON exactly as published. The output schemas and the
pandera contracts describe the tidy daily tables produced by the transforms.
"""

from __future__ import annotations

import pandera.pandas as pa
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

#: The twelve Open-Meteo variables requested by the bronze flow.
METEO_VARIABLES: tuple[str, ...] = (
    "precipitation",
    "rain",
    "soil_moisture_0_to_7cm",
    "soil_moisture_7_to_28cm",
    "soil_moisture_28_to_100cm",
    "temperature_2m",
    "relative_humidity_2m",
    "surface_pressure",
    "et0_fao_evapotranspiration",
    "wind_speed_10m",
    "shortwave_radiation",
    "cloud_cover",
)

#: Plausibility ranges used to quarantine hourly Open-Meteo values. The
#: bounds are deliberately wide: this is a smoke detector, not a climate
#: quality-control system.
METEO_RANGES: dict[str, tuple[float, float]] = {
    "precipitation": (0.0, 1000.0),
    "rain": (0.0, 1000.0),
    "soil_moisture_0_to_7cm": (0.0, 1.5),
    "soil_moisture_7_to_28cm": (0.0, 1.5),
    "soil_moisture_28_to_100cm": (0.0, 1.5),
    "temperature_2m": (-60.0, 60.0),
    "relative_humidity_2m": (0.0, 100.0),
    "surface_pressure": (800.0, 1100.0),
    "et0_fao_evapotranspiration": (0.0, 1000.0),
    "wind_speed_10m": (0.0, 400.0),
    "shortwave_radiation": (0.0, 1500.0),
    "cloud_cover": (0.0, 100.0),
}

USGS_PROPERTIES_SCHEMA = StructType(
    [
        StructField("time_series_id", StringType()),
        StructField("monitoring_location_id", StringType()),
        StructField("parameter_code", StringType()),
        StructField("statistic_id", StringType()),
        StructField("time", StringType()),
        StructField("value", StringType()),
        StructField("unit_of_measure", StringType()),
        StructField("approval_status", StringType()),
        StructField("qualifier", StringType()),
        StructField("last_modified", StringType()),
        StructField("method_category", StringType()),
    ]
)

USGS_COLLECTION_SCHEMA = StructType(
    [
        StructField(
            "features",
            ArrayType(
                StructType(
                    [
                        StructField("id", StringType()),
                        StructField("properties", USGS_PROPERTIES_SCHEMA),
                    ]
                )
            ),
        )
    ]
)

METEO_COLLECTION_SCHEMA = StructType(
    [
        StructField(
            "hourly",
            StructType(
                [StructField("time", ArrayType(StringType()))]
                + [
                    StructField(variable, ArrayType(DoubleType()))
                    for variable in METEO_VARIABLES
                ]
            ),
        )
    ]
)

CONTINUOUS_SOURCE_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("time_series_id", StringType()),
        StructField("parameter_code", StringType()),
        StructField("statistic_id", StringType()),
        StructField("time_raw", StringType()),
        StructField("value_raw", StringType()),
        StructField("unit", StringType()),
        StructField("approval_status", StringType()),
        StructField("qualifier", StringType()),
        StructField("last_modified", StringType()),
        StructField("ts_utc", TimestampType()),
    ]
)

USGS_DAILY_SOURCE_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("date", StringType()),
        StructField("parameter_code", StringType()),
        StructField("value_raw", StringType()),
        StructField("unit", StringType()),
        StructField("statistic_id", StringType()),
        StructField("approval_status", StringType()),
        StructField("last_modified", StringType()),
    ]
)

METEO_SOURCE_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("time_raw", StringType()),
        StructField("ts_utc", TimestampType()),
    ]
    + [StructField(variable, DoubleType()) for variable in METEO_VARIABLES]
)

GAGE_DAILY_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("date", StringType()),
        StructField("gage_height_mean_ft", DoubleType()),
        StructField("gage_height_min_ft", DoubleType()),
        StructField("gage_height_max_ft", DoubleType()),
        StructField("sample_count", IntegerType()),
        StructField("expected_samples", IntegerType()),
        StructField("approved_samples", IntegerType()),
        StructField("provisional_samples", IntegerType()),
        StructField("max_gap_seconds", IntegerType()),
    ]
)

USGS_DAILY_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("date", StringType()),
        StructField("parameter_code", StringType()),
        StructField("value_cfs", DoubleType()),
        StructField("unit", StringType()),
        StructField("statistic_id", StringType()),
        StructField("approval_status", StringType()),
        StructField("last_modified", StringType()),
    ]
)

METEO_DAILY_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("date", StringType()),
        StructField("precipitation_mm", DoubleType()),
        StructField("rain_mm", DoubleType()),
        StructField("et0_mm", DoubleType()),
        StructField("temperature_2m_mean_c", DoubleType()),
        StructField("temperature_2m_min_c", DoubleType()),
        StructField("temperature_2m_max_c", DoubleType()),
        StructField("relative_humidity_2m_mean_pct", DoubleType()),
        StructField("surface_pressure_mean_hpa", DoubleType()),
        StructField("soil_moisture_0_7_mean", DoubleType()),
        StructField("soil_moisture_7_28_mean", DoubleType()),
        StructField("soil_moisture_28_100_mean", DoubleType()),
        StructField("wind_speed_10m_mean_kmh", DoubleType()),
        StructField("wind_speed_10m_max_kmh", DoubleType()),
        StructField("shortwave_radiation_mean_wm2", DoubleType()),
        StructField("shortwave_radiation_sum_whm2", DoubleType()),
        StructField("cloud_cover_mean_pct", DoubleType()),
        StructField("observed_hours", IntegerType()),
        StructField("expected_hours", IntegerType()),
    ]
)

STATION_DAILY_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("date", StringType()),
        StructField("discharge_cfs", DoubleType()),
        StructField("discharge_unit", StringType()),
        StructField("discharge_statistic_id", StringType()),
        StructField("discharge_approval_status", StringType()),
        StructField("gage_height_mean_ft", DoubleType()),
        StructField("gage_height_min_ft", DoubleType()),
        StructField("gage_height_max_ft", DoubleType()),
        StructField("gage_height_sample_count", IntegerType()),
        StructField("gage_height_expected_samples", IntegerType()),
        StructField("gage_height_provisional_samples", IntegerType()),
        StructField("gage_height_completeness", DoubleType()),
        StructField("gage_height_max_gap_seconds", IntegerType()),
        StructField("precipitation_mm", DoubleType()),
        StructField("rain_mm", DoubleType()),
        StructField("et0_mm", DoubleType()),
        StructField("temperature_2m_mean_c", DoubleType()),
        StructField("temperature_2m_min_c", DoubleType()),
        StructField("temperature_2m_max_c", DoubleType()),
        StructField("relative_humidity_2m_mean_pct", DoubleType()),
        StructField("surface_pressure_mean_hpa", DoubleType()),
        StructField("soil_moisture_0_7_mean", DoubleType()),
        StructField("soil_moisture_7_28_mean", DoubleType()),
        StructField("soil_moisture_28_100_mean", DoubleType()),
        StructField("wind_speed_10m_mean_kmh", DoubleType()),
        StructField("wind_speed_10m_max_kmh", DoubleType()),
        StructField("shortwave_radiation_mean_wm2", DoubleType()),
        StructField("shortwave_radiation_sum_whm2", DoubleType()),
        StructField("cloud_cover_mean_pct", DoubleType()),
        StructField("meteo_observed_hours", IntegerType()),
        StructField("meteo_expected_hours", IntegerType()),
        StructField("meteo_completeness", DoubleType()),
        StructField("has_discharge", BooleanType()),
        StructField("has_gage_height", BooleanType()),
        StructField("has_meteo", BooleanType()),
    ]
)

QUARANTINE_CONTINUOUS_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("date", StringType()),
        StructField("time_raw", StringType()),
        StructField("parameter_code", StringType()),
        StructField("value_raw", StringType()),
        StructField("unit", StringType()),
        StructField("reason", StringType()),
    ]
)

QUARANTINE_USGS_DAILY_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("date", StringType()),
        StructField("parameter_code", StringType()),
        StructField("value_raw", StringType()),
        StructField("reason", StringType()),
    ]
)

QUARANTINE_METEO_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("date", StringType()),
        StructField("time_raw", StringType()),
        StructField("variable", StringType()),
        StructField("value", DoubleType()),
        StructField("reason", StringType()),
    ]
)

DQ_REPORT_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("table", StringType()),
        StructField("check_name", StringType()),
        StructField("severity", StringType()),
        StructField("passed", BooleanType()),
        StructField("failed_count", IntegerType()),
        StructField("total_count", IntegerType()),
        StructField("details", StringType()),
        StructField("generated_at", StringType()),
    ]
)

_DATE_CHECK = pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")
_DATE_COLUMN = pa.Column(str, _DATE_CHECK, nullable=False, coerce=True)
_STATION_COLUMN = pa.Column(str, nullable=False, coerce=True)
_RUN_ID_COLUMN = pa.Column(str, nullable=True, coerce=True)
_STRING_COLUMN = pa.Column(str, nullable=True, coerce=True)


def _float(nullable: bool = True) -> pa.Column:
    return pa.Column(float, nullable=nullable, coerce=True)


def _int(nullable: bool = False) -> pa.Column:
    # ``Int64`` is the pandas nullable integer dtype. Spark emits int32/float64
    # through Arrow when a column contains nulls, and ``coerce=True`` brings
    # both shapes back to a single contract type.
    return pa.Column("Int64", nullable=nullable, coerce=True)


GAGE_DAILY_CONTRACT = pa.DataFrameSchema(
    {
        "station_id": _STATION_COLUMN,
        "date": _DATE_COLUMN,
        "gage_height_mean_ft": _float(),
        "gage_height_min_ft": _float(),
        "gage_height_max_ft": _float(),
        "sample_count": _int(),
        "expected_samples": _int(),
        "approved_samples": _int(),
        "provisional_samples": _int(),
        "max_gap_seconds": _float(),
    },
    strict=False,
    coerce=True,
)

USGS_DAILY_CONTRACT = pa.DataFrameSchema(
    {
        "station_id": _STATION_COLUMN,
        "date": _DATE_COLUMN,
        "parameter_code": pa.Column(str, nullable=False, coerce=True),
        "value_cfs": _float(),
        "unit": _STRING_COLUMN,
        "statistic_id": _STRING_COLUMN,
        "approval_status": _STRING_COLUMN,
        "last_modified": _STRING_COLUMN,
    },
    strict=False,
    coerce=True,
)

METEO_DAILY_CONTRACT = pa.DataFrameSchema(
    {
        "station_id": _STATION_COLUMN,
        "date": _DATE_COLUMN,
        "precipitation_mm": _float(),
        "rain_mm": _float(),
        "et0_mm": _float(),
        "temperature_2m_mean_c": _float(),
        "temperature_2m_min_c": _float(),
        "temperature_2m_max_c": _float(),
        "relative_humidity_2m_mean_pct": _float(),
        "surface_pressure_mean_hpa": _float(),
        "soil_moisture_0_7_mean": _float(),
        "soil_moisture_7_28_mean": _float(),
        "soil_moisture_28_100_mean": _float(),
        "wind_speed_10m_mean_kmh": _float(),
        "wind_speed_10m_max_kmh": _float(),
        "shortwave_radiation_mean_wm2": _float(),
        "shortwave_radiation_sum_whm2": _float(),
        "cloud_cover_mean_pct": _float(),
        "observed_hours": _int(),
        "expected_hours": _int(),
    },
    strict=False,
    coerce=True,
)

STATION_DAILY_CONTRACT = pa.DataFrameSchema(
    {
        "station_id": _STATION_COLUMN,
        "date": _DATE_COLUMN,
        "discharge_cfs": _float(),
        "discharge_unit": _STRING_COLUMN,
        "discharge_statistic_id": _STRING_COLUMN,
        "discharge_approval_status": _STRING_COLUMN,
        "gage_height_mean_ft": _float(),
        "gage_height_min_ft": _float(),
        "gage_height_max_ft": _float(),
        "gage_height_sample_count": _int(nullable=True),
        "gage_height_expected_samples": _int(nullable=True),
        "gage_height_provisional_samples": _int(nullable=True),
        "gage_height_completeness": _float(),
        "gage_height_max_gap_seconds": _float(),
        "precipitation_mm": _float(),
        "rain_mm": _float(),
        "et0_mm": _float(),
        "temperature_2m_mean_c": _float(),
        "temperature_2m_min_c": _float(),
        "temperature_2m_max_c": _float(),
        "relative_humidity_2m_mean_pct": _float(),
        "surface_pressure_mean_hpa": _float(),
        "soil_moisture_0_7_mean": _float(),
        "soil_moisture_7_28_mean": _float(),
        "soil_moisture_28_100_mean": _float(),
        "wind_speed_10m_mean_kmh": _float(),
        "wind_speed_10m_max_kmh": _float(),
        "shortwave_radiation_mean_wm2": _float(),
        "shortwave_radiation_sum_whm2": _float(),
        "cloud_cover_mean_pct": _float(),
        "meteo_observed_hours": _int(nullable=True),
        "meteo_expected_hours": _int(nullable=True),
        "meteo_completeness": _float(),
        "has_discharge": pa.Column(bool, nullable=False, coerce=True),
        "has_gage_height": pa.Column(bool, nullable=False, coerce=True),
        "has_meteo": pa.Column(bool, nullable=False, coerce=True),
    },
    strict=False,
    coerce=True,
)

DQ_REPORT_CONTRACT = pa.DataFrameSchema(
    {
        "run_id": _RUN_ID_COLUMN,
        "table": pa.Column(str, nullable=False, coerce=True),
        "check_name": pa.Column(str, nullable=False, coerce=True),
        "severity": pa.Column(str, nullable=False, coerce=True),
        "passed": pa.Column(bool, nullable=False, coerce=True),
        "failed_count": _int(),
        "total_count": _int(),
        "details": _STRING_COLUMN,
        "generated_at": pa.Column(str, nullable=False, coerce=True),
    },
    strict=False,
    coerce=True,
)
