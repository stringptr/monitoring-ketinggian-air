"""Open-Meteo meteorology ingestion flow.

Fetches the raw JSON archive response for each station and lands it in bronze
verbatim (no column renames, no metadata columns). The first run per station
fetches the full history from ``HYDROLOGY_START_DATE``; once an
``ingest_date=*`` partition exists only the rolling last 7 days are fetched.
"""

from __future__ import annotations

import requests
from prefect import flow, get_run_logger, task

from datalake import (
    incremental_start,
    open_meteo_prefix,
    partition_exists,
    write_payload,
)
from settings import Settings, get_settings
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
def ingest_station_weather(
    station: Station, settings: Settings, ingest_date: str
) -> str:
    """Fetch and store the raw Open-Meteo JSON for a single station."""

    prefix = open_meteo_prefix(station.site_no)
    if partition_exists(settings, prefix):
        start_date = incremental_start(ingest_date)
    else:
        start_date = settings.start_date

    params = {
        "latitude": station.latitude,
        "longitude": station.longitude,
        "start_date": start_date,
        "end_date": settings.end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": station.timezone,
        "format": "json",
    }

    response = requests.get(ARCHIVE_URL, params=params, timeout=120)
    response.raise_for_status()
    payload = response.json()

    hourly = payload.get("hourly")
    if not hourly or "time" not in hourly:
        raise ValueError(
            f"Open-Meteo returned no hourly data for {station.site_no} "
            f"({start_date}..{settings.end_date}): "
            f"{payload.get('reason', payload)}"
        )

    location = write_payload(settings, prefix, ingest_date, payload)
    print(
        f"open-meteo {station.site_no}: {len(hourly['time'])} hour(s) "
        f"({start_date}..{settings.end_date}) -> {location}"
    )
    return location


@flow(name="ingest-open-meteo", log_prints=True)
def ingest_open_meteo(
    start_date: str | None = None, end_date: str | None = None
) -> list[str]:
    """Ingest raw Open-Meteo JSON for every active station."""

    logger = get_run_logger()
    settings = get_settings(start_date, end_date)
    ingest_date = settings.end_date
    stations = get_stations()

    logger.info(
        "Ingesting Open-Meteo into bronze for %d station(s), ingest_date=%s",
        len(stations),
        ingest_date,
    )

    written: list[str] = []
    for station in stations:
        written.append(ingest_station_weather(station, settings, ingest_date))

    return written
