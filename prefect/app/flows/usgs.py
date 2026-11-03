"""USGS hydrology ingestion flow.

Refactor of ``usgs_batch_download.py``. The original hardcoded a single
Maryland station (``USGS-01646500``), fixed dates and an API key in the source
code. Here the station comes from the shared registry, the date range is a
parameter and the API key is read from ``USGS_API_KEY``.
"""

from __future__ import annotations

import dataretrieval
import pandas as pd
from dataretrieval import Configuration, nwis, waterdata
from prefect import flow, get_run_logger, task

from datalake import write_bronze
from settings import Settings, get_settings
from stations import Station, get_stations

#: USGS parameter codes.
DISCHARGE_PARAM = "00060"  # discharge, cubic feet per second (daily values)
GAGE_HEIGHT_PARAM = "00065"  # gage height, feet (instantaneous values)


def _daily_discharge(station: Station, time_range: str) -> pd.DataFrame:
    frame, _ = waterdata.get_daily(
        monitoring_location_id=station.id,
        parameter_code=DISCHARGE_PARAM,
        time=time_range,
    )

    if frame is None or frame.empty:
        return pd.DataFrame(columns=["date", "discharge_cfs"])

    frame = frame.rename(columns={"time": "date", "value": "discharge_cfs"})
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    return frame[["date", "discharge_cfs"]]


def _daily_gage_height(station: Station, start_date: str, end_date: str) -> pd.DataFrame:
    frame, _ = nwis.get_iv(
        sites=station.site_no,
        start=start_date,
        end=end_date,
        parameterCd=GAGE_HEIGHT_PARAM,
    )

    columns = [
        "date",
        "gage_height_mean_ft",
        "gage_height_max_ft",
        "gage_height_min_ft",
    ]
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)

    frame = frame.reset_index()
    datetime_col = next(
        column
        for column in frame.columns
        if "datetime" in column.lower() or "date" in column.lower()
    )
    frame[datetime_col] = pd.to_datetime(frame[datetime_col])

    return (
        frame.groupby(frame[datetime_col].dt.date)
        .agg(
            gage_height_mean_ft=(GAGE_HEIGHT_PARAM, "mean"),
            gage_height_max_ft=(GAGE_HEIGHT_PARAM, "max"),
            gage_height_min_ft=(GAGE_HEIGHT_PARAM, "min"),
        )
        .reset_index()
        .rename(columns={datetime_col: "date"})
    )


@task(retries=3, retry_delay_seconds=30, log_prints=True)
def fetch_station_usgs(
    station: Station, start_date: str, end_date: str
) -> pd.DataFrame:
    """Fetch daily discharge and daily-aggregated gage height for one station."""

    settings = get_settings(start_date, end_date)
    if not settings.usgs_api_key:
        raise ValueError("USGS_API_KEY is not set")

    time_range = f"{start_date}/{end_date}"

    with dataretrieval.configure(Configuration(api_key=settings.usgs_api_key)):
        discharge = _daily_discharge(station, time_range)

    gage_height = _daily_gage_height(station, start_date, end_date)

    frame = (
        discharge.merge(gage_height, on="date", how="outer")
        .sort_values("date")
        .reset_index(drop=True)
    )

    frame["station_id"] = station.id
    frame["source"] = "usgs"
    frame["ingested_at"] = pd.Timestamp.utcnow()
    return frame


@task(retries=3, retry_delay_seconds=30, log_prints=True)
def store_station_usgs(
    frame: pd.DataFrame, station: Station, settings: Settings
) -> list[str]:
    return write_bronze(
        frame,
        dataset="usgs",
        station=station.site_no,
        settings=settings,
        time_column="date",
    )


@flow(name="ingest-usgs", log_prints=True)
def ingest_usgs(
    start_date: str | None = None, end_date: str | None = None
) -> list[str]:
    """Ingest USGS hydrology for every active station."""

    logger = get_run_logger()
    settings = get_settings(start_date, end_date)
    stations = get_stations()

    logger.info(
        "Ingesting USGS %s..%s for %d station(s)",
        settings.start_date,
        settings.end_date,
        len(stations),
    )

    written: list[str] = []
    for station in stations:
        frame = fetch_station_usgs(station, settings.start_date, settings.end_date)
        locations = store_station_usgs(frame, station, settings)
        logger.info(
            "usgs %s: %d rows -> %s",
            station.site_no,
            len(frame),
            ", ".join(locations),
        )
        written.extend(locations)

    return written
