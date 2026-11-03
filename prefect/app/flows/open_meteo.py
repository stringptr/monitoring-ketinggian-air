"""Open-Meteo ingestion flow.

Refactor of ``penarikan.py`` into a parameterised Prefect flow. The original
hardcoded Indonesian coordinates and the ``Asia/Bangkok`` timezone; here the
location is taken from the USGS station registry and every station uses
``America/Los_Angeles``.
"""

from __future__ import annotations

import pandas as pd
import requests
from prefect import flow, get_run_logger, task

from datalake import write_bronze
from settings import get_settings
from stations import Station, get_stations

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

#: The twelve hourly variables requested by the project (see Notes.md).
HOURLY_VARIABLES: tuple[str, ...] = (
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


@task(retries=3, retry_delay_seconds=30, log_prints=True)
def fetch_station_weather(
    station: Station, start_date: str, end_date: str
) -> pd.DataFrame:
    """Fetch the hourly weather archive for a single station.

    Errors are raised (not swallowed) so Prefect can surface a failed task and
    apply the configured retries.
    """

    params = {
        "latitude": station.latitude,
        "longitude": station.longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": station.timezone,
    }

    response = requests.get(ARCHIVE_URL, params=params, timeout=120)
    response.raise_for_status()
    payload = response.json()

    hourly = payload.get("hourly")
    if not hourly or "time" not in hourly:
        raise ValueError(
            f"Open-Meteo returned no hourly data for {station.id} "
            f"({start_date}..{end_date}): {payload.get('reason', payload)}"
        )

    frame = pd.DataFrame(hourly)
    frame["time"] = pd.to_datetime(frame["time"])
    frame["station_id"] = station.id
    frame["source"] = "open_meteo"
    frame["ingested_at"] = pd.Timestamp.utcnow()

    return frame


@task(retries=3, retry_delay_seconds=30, log_prints=True)
def store_station_weather(
    frame: pd.DataFrame, station: Station, start_date: str, end_date: str
) -> list[str]:
    settings = get_settings(start_date, end_date)
    return write_bronze(
        frame,
        dataset="open_meteo",
        station=station.site_no,
        settings=settings,
        time_column="time",
    )


@flow(name="ingest-open-meteo", log_prints=True)
def ingest_open_meteo(
    start_date: str | None = None, end_date: str | None = None
) -> list[str]:
    """Ingest hourly Open-Meteo meteorology for every active station."""

    logger = get_run_logger()
    settings = get_settings(start_date, end_date)
    stations = get_stations()

    logger.info(
        "Ingesting Open-Meteo %s..%s for %d station(s)",
        settings.start_date,
        settings.end_date,
        len(stations),
    )

    written: list[str] = []
    for station in stations:
        frame = fetch_station_weather(station, settings.start_date, settings.end_date)
        locations = store_station_weather(
            frame, station, settings.start_date, settings.end_date
        )
        logger.info(
            "open_meteo %s: %d rows -> %s",
            station.site_no,
            len(frame),
            ", ".join(locations),
        )
        written.extend(locations)

    return written
