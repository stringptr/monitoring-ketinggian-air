"""Orchestration of the daily silver transformation.

The flow layer stays thin; this module contains the work so a single Prefect
task owns the Spark session. The output is four partitioned tables plus DQ and
quarantine artefacts:

``silver/usgs_daily``
    Native USGS daily discharge (00060).
``silver/usgs_gage_height_daily``
    Gage height (00065) aggregated from the 15-minute continuous feed.
``silver/meteo_daily``
    Open-Meteo hourly data aggregated to the local day.
``silver/station_daily``
    The three sources joined on ``(station_id, date)``.
"""

from __future__ import annotations

import uuid
from contextlib import suppress
from functools import reduce

from datalake import date_range
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from stations import get_stations

from silver import readers
from silver.config import (
    QUARANTINE_GAGE,
    QUARANTINE_METEO,
    QUARANTINE_USGS_DAILY,
    TABLE_GAGE_DAILY,
    TABLE_METEO_DAILY,
    TABLE_STATION_DAILY,
    TABLE_USGS_DAILY,
    SilverSettings,
    next_day,
)
from silver.quality import (
    assert_no_duplicates,
    build_report,
    run_gage_checks,
    run_meteo_checks,
    run_station_checks,
    run_usgs_daily_checks,
    validate_contract,
)
from silver.schemas import (
    GAGE_DAILY_CONTRACT,
    GAGE_DAILY_SCHEMA,
    METEO_DAILY_CONTRACT,
    METEO_DAILY_SCHEMA,
    STATION_DAILY_CONTRACT,
    USGS_DAILY_CONTRACT,
    USGS_DAILY_SCHEMA,
)
from silver.spark import build_spark_session
from silver.transform import (
    aggregate_gage,
    aggregate_meteo,
    build_station_daily,
    clean_continuous,
    clean_meteo,
    clean_usgs_daily,
    day_dimension,
    with_local_date,
)
from silver.writer import write_dq_report, write_quarantine, write_table


def run_id() -> str:
    value = None
    with suppress(Exception):
        # Prefect is only available inside a worker; standalone runs fall back
        # to a random identifier.
        from prefect.runtime import flow_run

        value = flow_run.id
    return str(value) if value else uuid.uuid4().hex


def _is_empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0


def _union(frames: list[DataFrame], schema, spark: SparkSession) -> DataFrame:
    usable = [frame for frame in frames if not _is_empty(frame)]
    if not usable:
        return spark.createDataFrame([], schema)
    return reduce(DataFrame.unionByName, usable)


def _restrict(df: DataFrame, start: str, end: str) -> DataFrame:
    return df.filter(F.col("date").between(start, end))


def _restrict_quarantine(df: DataFrame, start: str, end: str) -> DataFrame:
    return df.filter(F.col("date").isNull() | F.col("date").between(start, end))


def _validate_and_write(
    spark: SparkSession,
    df: DataFrame,
    settings: SilverSettings,
    table: str,
    contract,
    keys: list[str],
    results: list[dict],
) -> tuple[int, int]:
    empty = _is_empty(df)
    if not empty:
        pdf = df.toPandas()
        if not pdf.empty:
            validate_contract(pdf, contract, table)
        assert_no_duplicates(df, keys, table)
        write_table(spark, df, settings, table)

    report = build_report(spark, results)
    identifier = results[0]["run_id"] if results else run_id()
    write_dq_report(spark, report, settings, table, identifier)

    rows = 0 if empty else df.count()
    failed = sum(1 for result in results if not result["passed"])
    return rows, failed


def run_silver(
    start_date: str,
    end_date: str,
    settings: SilverSettings,
    check_freshness: bool = True,
) -> dict:
    """Transform every bronze partition in ``start_date..end_date``."""

    spark = build_spark_session(settings)
    try:
        identifier = run_id()
        stations = get_stations()
        local_days = list(date_range(start_date, end_date))
        utc_days = list(date_range(start_date, next_day(end_date)))
        dimension = day_dimension(spark, local_days)

        gage_frames: list[DataFrame] = []
        meteo_frames: list[DataFrame] = []
        usgs_daily_frames: list[DataFrame] = []
        quarantine_gage: list[DataFrame] = []
        quarantine_meteo: list[DataFrame] = []
        quarantine_usgs_daily: list[DataFrame] = []

        for station in stations:
            gage_available = readers.available_dates(
                settings, readers.continuous_prefix(station)
            )
            meteo_available = readers.available_dates(
                settings, readers.meteo_prefix(station)
            )
            daily_available = readers.available_dates(
                settings, readers.daily_prefix(station)
            )

            gage_days = sorted(set(utc_days) & gage_available)
            meteo_days = sorted(set(utc_days) & meteo_available)
            daily_days = sorted(set(local_days) & daily_available)

            print(
                f"silver {station.site_no}: gage partitions={len(gage_days)} "
                f"meteo partitions={len(meteo_days)} "
                f"daily partitions={len(daily_days)}"
            )

            gage_raw = readers.read_usgs_continuous(spark, settings, station, gage_days)
            gage_valid, gage_quarantine = clean_continuous(spark, gage_raw)
            gage_agg = _restrict(
                aggregate_gage(gage_valid, dimension), start_date, end_date
            )
            if not _is_empty(gage_agg):
                gage_frames.append(gage_agg)
            quarantine_gage.append(
                _restrict_quarantine(gage_quarantine, start_date, end_date)
            )

            meteo_raw = readers.read_meteo(spark, settings, station, meteo_days)
            meteo_clean, meteo_quarantine = clean_meteo(spark, meteo_raw)
            meteo_agg = _restrict(
                aggregate_meteo(with_local_date(meteo_clean), dimension),
                start_date,
                end_date,
            )
            if not _is_empty(meteo_agg):
                meteo_frames.append(meteo_agg)
            quarantine_meteo.append(
                _restrict_quarantine(meteo_quarantine, start_date, end_date)
            )

            daily_raw = readers.read_usgs_daily(spark, settings, station, daily_days)
            daily_valid, daily_quarantine = clean_usgs_daily(spark, daily_raw)
            daily_valid = _restrict(daily_valid, start_date, end_date)
            if not _is_empty(daily_valid):
                usgs_daily_frames.append(daily_valid)
            quarantine_usgs_daily.append(
                _restrict_quarantine(daily_quarantine, start_date, end_date)
            )

        gage = _union(gage_frames, GAGE_DAILY_SCHEMA, spark)
        meteo = _union(meteo_frames, METEO_DAILY_SCHEMA, spark)
        usgs_daily = _union(usgs_daily_frames, USGS_DAILY_SCHEMA, spark)
        station_daily = build_station_daily(meteo, usgs_daily, gage)

        tables: dict[str, int] = {}
        dq: dict[str, dict[str, int]] = {}

        usgs_results = run_usgs_daily_checks(usgs_daily, settings, identifier)
        rows, failed = _validate_and_write(
            spark,
            usgs_daily,
            settings,
            TABLE_USGS_DAILY,
            USGS_DAILY_CONTRACT,
            ["station_id", "date", "parameter_code"],
            usgs_results,
        )
        tables[TABLE_USGS_DAILY] = rows
        dq[TABLE_USGS_DAILY] = {
            "checks": len(usgs_results),
            "failed": failed,
        }

        gage_results = run_gage_checks(gage, settings, identifier)
        rows, failed = _validate_and_write(
            spark,
            gage,
            settings,
            TABLE_GAGE_DAILY,
            GAGE_DAILY_CONTRACT,
            ["station_id", "date"],
            gage_results,
        )
        tables[TABLE_GAGE_DAILY] = rows
        dq[TABLE_GAGE_DAILY] = {
            "checks": len(gage_results),
            "failed": failed,
        }

        meteo_results = run_meteo_checks(meteo, settings, identifier)
        rows, failed = _validate_and_write(
            spark,
            meteo,
            settings,
            TABLE_METEO_DAILY,
            METEO_DAILY_CONTRACT,
            ["station_id", "date"],
            meteo_results,
        )
        tables[TABLE_METEO_DAILY] = rows
        dq[TABLE_METEO_DAILY] = {
            "checks": len(meteo_results),
            "failed": failed,
        }

        station_results = run_station_checks(
            station_daily, settings, identifier, check_freshness
        )
        rows, failed = _validate_and_write(
            spark,
            station_daily,
            settings,
            TABLE_STATION_DAILY,
            STATION_DAILY_CONTRACT,
            ["station_id", "date"],
            station_results,
        )
        tables[TABLE_STATION_DAILY] = rows
        dq[TABLE_STATION_DAILY] = {
            "checks": len(station_results),
            "failed": failed,
        }

        quarantine = {}
        for name, frames in (
            (QUARANTINE_GAGE, quarantine_gage),
            (QUARANTINE_METEO, quarantine_meteo),
            (QUARANTINE_USGS_DAILY, quarantine_usgs_daily),
        ):
            if not frames:
                continue
            merged = reduce(DataFrame.unionByName, frames)
            count = 0 if _is_empty(merged) else merged.count()
            quarantine[name] = count
            write_quarantine(spark, merged, settings, name)

        print(f"silver complete {start_date}..{end_date}: {tables}")

        return {
            "run_id": identifier,
            "start_date": start_date,
            "end_date": end_date,
            "tables": tables,
            "dq": dq,
            "quarantine": quarantine,
        }
    finally:
        spark.stop()
