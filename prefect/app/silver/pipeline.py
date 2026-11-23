"""Orchestration of the native-resolution silver transformation.

Silver mirrors the bronze hierarchy and keeps the original observation
resolution. Bronze partitions are deduplicated, cleaned, given local-time
columns, validated and checked for quality. There is no aggregation, no
resampling and no cross-source join.
"""

from __future__ import annotations

import uuid
from contextlib import suppress
from functools import partial, reduce

from datalake import date_range, year_chunks
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from stations import get_stations

from silver import readers
from silver.config import (
    CONTINUOUS_SPECS,
    EXPECTED_METEO_HOURS_PER_DAY,
    EXPECTED_USGS_DAILY_VALUES,
    EXPECTED_USGS_SAMPLES_PER_DAY,
    PATH_METEO_HOURLY,
    PATH_USGS_DAILY,
    TABLE_METEO_HOURLY,
    TABLE_USGS_DAILY,
    SilverSettings,
)
from silver.quality import (
    assert_no_duplicates,
    build_report,
    run_continuous_checks,
    run_meteo_checks,
    run_quarantine_check,
    run_usgs_daily_checks,
    validate_contract,
)
from silver.schemas import (
    METEO_HOURLY_CONTRACT,
    METEO_HOURLY_SCHEMA,
    QUARANTINE_CONTINUOUS_SCHEMA,
    QUARANTINE_METEO_SCHEMA,
    QUARANTINE_USGS_DAILY_SCHEMA,
    USGS_CONTINUOUS_CONTRACT,
    USGS_CONTINUOUS_SCHEMA,
    USGS_DAILY_CONTRACT,
    USGS_DAILY_SCHEMA,
)
from silver.spark import build_spark_session
from silver.transform import (
    clean_continuous,
    clean_meteo,
    clean_usgs_daily,
)
from silver.writer import write_dq_report, write_quarantine, write_table


def run_id() -> str:
    value = None
    with suppress(Exception):
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


def _skeleton(
    spark: SparkSession,
    stations,
    dates: list[str],
) -> DataFrame:
    rows = [(station.site_no, day) for station in stations for day in dates]
    schema = StructType(
        [
            StructField("station_id", StringType()),
            StructField("ingest_date", StringType()),
            StructField("expected_samples", IntegerType()),
            StructField("expected_hours", IntegerType()),
            StructField("expected_daily", IntegerType()),
        ]
    )
    return spark.createDataFrame(
        [
            (
                station_id,
                day,
                EXPECTED_USGS_SAMPLES_PER_DAY,
                EXPECTED_METEO_HOURS_PER_DAY,
                EXPECTED_USGS_DAILY_VALUES,
            )
            for station_id, day in rows
        ],
        schema,
    )


def _process_table(
    spark: SparkSession,
    df: DataFrame,
    quarantine: DataFrame,
    settings: SilverSettings,
    table: str,
    path: str,
    contract,
    keys: list[str],
    check_fn,
    skeleton: DataFrame,
    identifier: str,
    check_freshness: bool,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[int, int, int, int]:
    empty = _is_empty(df)
    if not empty:
        pdf = df.toPandas()
        if not pdf.empty:
            validate_contract(pdf, contract, table)
        assert_no_duplicates(df, keys, table)
        predicate = (
            f"ingest_date >= '{start_date}' AND ingest_date <= '{end_date}'"
            if start_date and end_date
            else None
        )
        write_table(spark, df, settings, path, predicate=predicate)

    results = check_fn(df, skeleton, settings, identifier, check_freshness)
    quarantine_results = run_quarantine_check(
        df,
        quarantine,
        table,
        identifier,
        skeleton,
    )
    results.extend(quarantine_results)

    report = build_report(spark, results)
    write_dq_report(spark, report, settings, table)
    write_quarantine(spark, quarantine, settings, table)

    rows = 0 if empty else df.count()
    failed = sum(1 for result in results if not result["passed"])
    return (
        rows,
        len(results),
        failed,
        sum(result["failed_count"] for result in quarantine_results),
    )


def run_silver(
    start_date: str,
    end_date: str,
    settings: SilverSettings,
    check_freshness: bool = True,
) -> dict:
    """Clean every bronze partition in ``start_date..end_date``.

    ``start_date`` and ``end_date`` are UTC ``ingest_date`` values. The range
    is processed in calendar-year chunks so a full backfill validates and
    writes a bounded amount of data at a time.
    """

    spark = build_spark_session(settings)
    try:
        identifier = run_id()
        stations = get_stations()

        availability: dict[tuple[str, ...], set[str]] = {}
        for station in stations:
            for spec in CONTINUOUS_SPECS:
                availability[(station.site_no, "continuous", spec.parameter_code)] = (
                    readers.available_dates(
                        settings,
                        readers.continuous_prefix(station, spec.parameter_code),
                    )
                )
            availability[(station.site_no, "daily")] = readers.available_dates(
                settings, readers.daily_prefix(station)
            )
            availability[(station.site_no, "meteo")] = readers.available_dates(
                settings, readers.meteo_prefix(station)
            )

        totals = {spec.table: 0 for spec in CONTINUOUS_SPECS}
        totals[TABLE_USGS_DAILY] = 0
        totals[TABLE_METEO_HOURLY] = 0
        dq = {table: {"checks": 0, "failed": 0} for table in totals}
        quarantine_totals = {table: 0 for table in totals}

        for chunk_start, chunk_end in year_chunks(start_date, end_date):
            chunk_dates = list(date_range(chunk_start, chunk_end))
            skeleton = _skeleton(spark, stations, chunk_dates)

            continuous_frames: dict[str, list[DataFrame]] = {
                spec.table: [] for spec in CONTINUOUS_SPECS
            }
            continuous_quarantine: dict[str, list[DataFrame]] = {
                spec.table: [] for spec in CONTINUOUS_SPECS
            }
            daily_frames: list[DataFrame] = []
            meteo_frames: list[DataFrame] = []
            daily_quarantine: list[DataFrame] = []
            meteo_quarantine: list[DataFrame] = []

            for station in stations:
                site_no = station.site_no
                continuous_counts: dict[str, int] = {}

                for spec in CONTINUOUS_SPECS:
                    continuous_days = sorted(
                        set(chunk_dates)
                        & availability[
                            (site_no, "continuous", spec.parameter_code)
                        ]
                    )
                    raw = readers.read_usgs_continuous(
                        spark,
                        settings,
                        station,
                        continuous_days,
                        spec.parameter_code,
                    )
                    valid, quarantined = clean_continuous(spark, raw)
                    continuous_frames[spec.table].append(valid)
                    continuous_quarantine[spec.table].append(quarantined)
                    continuous_counts[spec.parameter_code] = len(continuous_days)

                daily_days = sorted(set(chunk_dates) & availability[(site_no, "daily")])
                raw = readers.read_usgs_daily(spark, settings, station, daily_days)
                valid, quarantined = clean_usgs_daily(spark, raw)
                daily_frames.append(valid)
                daily_quarantine.append(quarantined)

                meteo_days = sorted(set(chunk_dates) & availability[(site_no, "meteo")])
                raw = readers.read_meteo(spark, settings, station, meteo_days)
                valid, quarantined = clean_meteo(spark, raw)
                meteo_frames.append(valid)
                meteo_quarantine.append(quarantined)

                continuous_label = " ".join(
                    f"continuous_{code}={count}"
                    for code, count in continuous_counts.items()
                )
                print(
                    f"silver {site_no}: {continuous_label} "
                    f"daily={len(daily_days)} meteo={len(meteo_days)} "
                    f"({chunk_start}..{chunk_end})"
                )

            for spec in CONTINUOUS_SPECS:
                continuous = _union(
                    continuous_frames[spec.table], USGS_CONTINUOUS_SCHEMA, spark
                )
                continuous_q = _union(
                    continuous_quarantine[spec.table],
                    QUARANTINE_CONTINUOUS_SCHEMA,
                    spark,
                )
                rows, checks, failed, quarantined = _process_table(
                    spark,
                    continuous,
                    continuous_q,
                    settings,
                    spec.table,
                    spec.path,
                    USGS_CONTINUOUS_CONTRACT,
                    ["station_id", "parameter_code", "time_utc"],
                    partial(
                        run_continuous_checks,
                        table=spec.table,
                        expected_unit=spec.expected_unit,
                    ),
                    skeleton,
                    identifier,
                    check_freshness,
                    start_date=chunk_start,
                    end_date=chunk_end,
                )
                totals[spec.table] += rows
                dq[spec.table]["checks"] += checks
                dq[spec.table]["failed"] += failed
                quarantine_totals[spec.table] += quarantined

            daily = _union(daily_frames, USGS_DAILY_SCHEMA, spark)
            meteo = _union(meteo_frames, METEO_HOURLY_SCHEMA, spark)
            daily_q = _union(daily_quarantine, QUARANTINE_USGS_DAILY_SCHEMA, spark)
            meteo_q = _union(meteo_quarantine, QUARANTINE_METEO_SCHEMA, spark)

            rows, checks, failed, quarantined = _process_table(
                spark,
                daily,
                daily_q,
                settings,
                TABLE_USGS_DAILY,
                PATH_USGS_DAILY,
                USGS_DAILY_CONTRACT,
                ["station_id", "parameter_code", "date_local"],
                run_usgs_daily_checks,
                skeleton,
                identifier,
                check_freshness,
                start_date=chunk_start,
                end_date=chunk_end,
            )
            totals[TABLE_USGS_DAILY] += rows
            dq[TABLE_USGS_DAILY]["checks"] += checks
            dq[TABLE_USGS_DAILY]["failed"] += failed
            quarantine_totals[TABLE_USGS_DAILY] += quarantined

            rows, checks, failed, quarantined = _process_table(
                spark,
                meteo,
                meteo_q,
                settings,
                TABLE_METEO_HOURLY,
                PATH_METEO_HOURLY,
                METEO_HOURLY_CONTRACT,
                ["station_id", "time_utc"],
                run_meteo_checks,
                skeleton,
                identifier,
                check_freshness,
                start_date=chunk_start,
                end_date=chunk_end,
            )
            totals[TABLE_METEO_HOURLY] += rows
            dq[TABLE_METEO_HOURLY]["checks"] += checks
            dq[TABLE_METEO_HOURLY]["failed"] += failed
            quarantine_totals[TABLE_METEO_HOURLY] += quarantined

        print(f"silver complete {start_date}..{end_date}: {totals}")

        return {
            "run_id": identifier,
            "start_date": start_date,
            "end_date": end_date,
            "tables": totals,
            "dq": dq,
            "quarantine": quarantine_totals,
        }
    finally:
        spark.stop()
