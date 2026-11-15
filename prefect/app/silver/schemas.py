"""Spark schemas and pandera contracts for the native-resolution silver layer.

Bronze stores the upstream payload verbatim, so the reader schemas describe the
raw USGS/Open-Meteo JSON exactly as published. The output schemas and pandera
contracts describe the cleaned tables, which keep the original observation
resolution (15 minutes/hourly/daily).
"""

from __future__ import annotations

import pandera.pandas as pa
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("elevation", DoubleType()),
        StructField("timezone", StringType()),
        StructField("hourly_units", MapType(StringType(), StringType())),
        StructField(
            "hourly",
            StructType(
                [StructField("time", ArrayType(StringType()))]
                + [
                    StructField(variable, ArrayType(DoubleType()))
                    for variable in METEO_VARIABLES
                ]
            ),
        ),
    ]
)

CONTINUOUS_SOURCE_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("ingest_date", StringType()),
        StructField("time_series_id", StringType()),
        StructField("monitoring_location_id", StringType()),
        StructField("parameter_code", StringType()),
        StructField("statistic_id", StringType()),
        StructField("time_raw", StringType()),
        StructField("value_raw", StringType()),
        StructField("unit", StringType()),
        StructField("approval_status", StringType()),
        StructField("qualifier", StringType()),
        StructField("last_modified_raw", StringType()),
        StructField("method_category", StringType()),
        StructField("ts_utc", TimestampType()),
    ]
)

USGS_DAILY_SOURCE_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("ingest_date", StringType()),
        StructField("time_series_id", StringType()),
        StructField("monitoring_location_id", StringType()),
        StructField("parameter_code", StringType()),
        StructField("statistic_id", StringType()),
        StructField("date_raw", StringType()),
        StructField("value_raw", StringType()),
        StructField("unit", StringType()),
        StructField("approval_status", StringType()),
        StructField("qualifier", StringType()),
        StructField("last_modified_raw", StringType()),
    ]
)

METEO_SOURCE_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("ingest_date", StringType()),
        StructField("time_raw", StringType()),
        StructField("ts_utc", TimestampType()),
    ]
    + [StructField(variable, DoubleType()) for variable in METEO_VARIABLES]
    + [
        StructField("units_json", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("elevation", DoubleType()),
        StructField("timezone", StringType()),
    ]
)

USGS_CONTINUOUS_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("ingest_date", StringType()),
        StructField("time_utc", TimestampType()),
        StructField("time_local", TimestampType()),
        StructField("date_local", StringType()),
        StructField("time_series_id", StringType()),
        StructField("monitoring_location_id", StringType()),
        StructField("parameter_code", StringType()),
        StructField("statistic_id", StringType()),
        StructField("value", DoubleType()),
        StructField("unit", StringType()),
        StructField("approval_status", StringType()),
        StructField("qualifier", StringType()),
        StructField("last_modified", TimestampType()),
        StructField("method_category", StringType()),
    ]
)

USGS_DAILY_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("ingest_date", StringType()),
        StructField("time_series_id", StringType()),
        StructField("monitoring_location_id", StringType()),
        StructField("parameter_code", StringType()),
        StructField("statistic_id", StringType()),
        StructField("date_local", StringType()),
        StructField("value", DoubleType()),
        StructField("unit", StringType()),
        StructField("approval_status", StringType()),
        StructField("qualifier", StringType()),
        StructField("last_modified", TimestampType()),
    ]
)

METEO_HOURLY_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("ingest_date", StringType()),
        StructField("time_utc", TimestampType()),
        StructField("time_local", TimestampType()),
        StructField("date_local", StringType()),
    ]
    + [StructField(variable, DoubleType()) for variable in METEO_VARIABLES]
    + [
        StructField("units_json", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("elevation", DoubleType()),
        StructField("timezone", StringType()),
    ]
)

QUARANTINE_CONTINUOUS_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("ingest_date", StringType()),
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
        StructField("ingest_date", StringType()),
        StructField("date_raw", StringType()),
        StructField("parameter_code", StringType()),
        StructField("value_raw", StringType()),
        StructField("reason", StringType()),
    ]
)

QUARANTINE_METEO_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("ingest_date", StringType()),
        StructField("time_raw", StringType()),
        StructField("date_local", StringType()),
        StructField("variable", StringType()),
        StructField("value", DoubleType()),
        StructField("reason", StringType()),
    ]
)

DQ_REPORT_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("table", StringType()),
        StructField("ingest_date", StringType()),
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

_DATE_CHECK = pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")
_DATE_COLUMN = pa.Column(str, _DATE_CHECK, nullable=False, coerce=True)
_STATION_COLUMN = pa.Column(str, nullable=False, coerce=True)
_INGEST_COLUMN = pa.Column(str, _DATE_CHECK, nullable=False, coerce=True)
_STRING_COLUMN = pa.Column(str, nullable=True, coerce=True)


def _float(nullable: bool = True) -> pa.Column:
    return pa.Column(float, nullable=nullable, coerce=True)


def _timestamp(nullable: bool = True) -> pa.Column:
    return pa.Column(pa.DateTime, nullable=nullable, coerce=True)


USGS_CONTINUOUS_CONTRACT = pa.DataFrameSchema(
    {
        "station_id": _STATION_COLUMN,
        "ingest_date": _INGEST_COLUMN,
        "time_utc": _timestamp(nullable=False),
        "time_local": _timestamp(nullable=False),
        "date_local": _DATE_COLUMN,
        "time_series_id": _STRING_COLUMN,
        "monitoring_location_id": _STRING_COLUMN,
        "parameter_code": pa.Column(str, nullable=False, coerce=True),
        "statistic_id": _STRING_COLUMN,
        "value": _float(),
        "unit": _STRING_COLUMN,
        "approval_status": _STRING_COLUMN,
        "qualifier": _STRING_COLUMN,
        "last_modified": _timestamp(),
        "method_category": _STRING_COLUMN,
    },
    strict=False,
    coerce=True,
)

USGS_DAILY_CONTRACT = pa.DataFrameSchema(
    {
        "station_id": _STATION_COLUMN,
        "ingest_date": _INGEST_COLUMN,
        "time_series_id": _STRING_COLUMN,
        "monitoring_location_id": _STRING_COLUMN,
        "parameter_code": pa.Column(str, nullable=False, coerce=True),
        "statistic_id": _STRING_COLUMN,
        "date_local": _DATE_COLUMN,
        "value": _float(),
        "unit": _STRING_COLUMN,
        "approval_status": _STRING_COLUMN,
        "qualifier": _STRING_COLUMN,
        "last_modified": _timestamp(),
    },
    strict=False,
    coerce=True,
)

METEO_HOURLY_CONTRACT = pa.DataFrameSchema(
    {
        "station_id": _STATION_COLUMN,
        "ingest_date": _INGEST_COLUMN,
        "time_utc": _timestamp(nullable=False),
        "time_local": _timestamp(nullable=False),
        "date_local": _DATE_COLUMN,
        **{variable: _float() for variable in METEO_VARIABLES},
        "units_json": _STRING_COLUMN,
        "latitude": _float(),
        "longitude": _float(),
        "elevation": _float(),
        "timezone": _STRING_COLUMN,
    },
    strict=False,
    coerce=True,
)

DQ_REPORT_CONTRACT = pa.DataFrameSchema(
    {
        "run_id": _STRING_COLUMN,
        "table": pa.Column(str, nullable=False, coerce=True),
        "ingest_date": _INGEST_COLUMN,
        "station_id": _STATION_COLUMN,
        "check_name": pa.Column(str, nullable=False, coerce=True),
        "severity": pa.Column(str, nullable=False, coerce=True),
        "passed": pa.Column(bool, nullable=False, coerce=True),
        "failed_count": pa.Column(int, nullable=False, coerce=True),
        "total_count": pa.Column(int, nullable=False, coerce=True),
        "details": _STRING_COLUMN,
        "generated_at": pa.Column(str, nullable=False, coerce=True),
    },
    strict=False,
    coerce=True,
)
