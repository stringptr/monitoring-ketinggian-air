"""Open-Meteo meteorology ingestion flow.

Fetches the raw JSON archive response for each station and lands it in bronze
verbatim (no column renames, no metadata columns). Data is partitioned by
observation day (UTC): every date from ``HYDROLOGY_START_DATE`` (default
``2023-01-01``) through today gets its own ``ingest_date=YYYY-MM-DD``
partition. Each file contains that day plus the previous ``LOOKBACK_DAYS``
days (default 7). Existing partitions are skipped, so a long backfill can be
stopped and resumed without repeating completed days.

Missing dates are fetched in calendar-year batches and split into one file per
day. Requests are throttled to the documented Open-Meteo quota.
"""

from __future__ import annotations

from datetime import date, timedelta

from prefect import flow, get_run_logger, task

from datalake import (
    contiguous_runs,
    date_range,
    existing_dates,
    open_meteo_prefix,
    window_start,
    write_payload,
    year_chunks,
)
from ratelimit import OPEN_METEO_LIMITER, get_json
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


def fetch_chunk(
    station: Station, start_date: str, end_date: str
) -> tuple[dict, list[str]]:
    """Fetch one (<= 1 year) window and return ``(payload, variable_keys)``."""

    payload = get_json(
        OPEN_METEO_LIMITER,
        ARCHIVE_URL,
        params={
            "latitude": station.latitude,
            "longitude": station.longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(HOURLY_VARIABLES),
            "timezone": "UTC",
            "format": "json",
        },
    )

    hourly = payload.get("hourly")
    if not hourly or "time" not in hourly:
        raise ValueError(
            f"Open-Meteo returned no hourly data for {station.site_no} "
            f"({start_date}..{end_date}): {payload.get('reason', payload)}"
        )

    variable_keys = [key for key in hourly if key != "time"]
    return payload, variable_keys


def split_hourly_by_date(
    payload: dict, variable_keys: list[str]
) -> dict[str, dict]:
    """Slice a multi-day hourly payload into per-day ``hourly`` objects."""

    hourly = payload["hourly"]
    times = hourly["time"]

    buckets: dict[str, list[int]] = {}
    for index, timestamp in enumerate(times):
        buckets.setdefault(timestamp[:10], []).append(index)

    grouped: dict[str, dict] = {}
    for day, indices in buckets.items():
        day_hourly: dict[str, list] = {"time": [times[index] for index in indices]}
        for key in variable_keys:
            values = hourly.get(key) or []
            day_hourly[key] = [values[index] for index in indices]
        grouped[day] = day_hourly
    return grouped


def _day_payload(payload: dict, hourly: dict) -> dict:
    day_payload = {key: value for key, value in payload.items() if key != "hourly"}
    day_payload["hourly"] = hourly
    return day_payload


@task(retries=3, retry_delay_seconds=30, log_prints=True)
def ingest_station_weather(station: Station, settings: Settings) -> list[str]:
    """Fetch every missing day of Open-Meteo data for a single station."""

    prefix = open_meteo_prefix(station.site_no)
    existing = existing_dates(settings, prefix)
    missing = [
        day
        for day in date_range(settings.start_date, settings.end_date)
        if day not in existing
    ]

    label = f"open-meteo {station.site_no}"
    if not missing:
        print(f"{label}: up to date (nothing to fetch)")
        return []

    missing_set = set(missing)
    grace_cutoff = (
        date.fromisoformat(settings.end_date)
        - timedelta(days=settings.empty_grace_days)
    ).isoformat()

    written: list[str] = []
    lookback = settings.lookback_days
    for run_start, run_end in contiguous_runs(missing):
        for chunk_start, chunk_end in year_chunks(run_start, run_end):
            # Fetch a lookback lead-in so each day's window is complete even
            # across year boundaries.
            fetch_start = window_start(chunk_start, settings.start_date, lookback)
            payload, variable_keys = fetch_chunk(station, fetch_start, chunk_end)
            grouped = split_hourly_by_date(payload, variable_keys)

            for day in date_range(chunk_start, chunk_end):
                if day not in missing_set:
                    continue

                first_day = window_start(day, settings.start_date, lookback)
                combined_time: list[str] = []
                combined: dict[str, list] = {key: [] for key in variable_keys}
                for window_day in date_range(first_day, day):
                    hourly = grouped.get(window_day)
                    if not hourly:
                        continue
                    combined_time.extend(hourly["time"])
                    for key in variable_keys:
                        combined[key].extend(hourly[key])

                if not combined_time and day > grace_cutoff:
                    # Recent days may not be published yet; retry next run.
                    continue

                hourly = {"time": combined_time, **combined}
                written.append(
                    write_payload(
                        settings, prefix, day, _day_payload(payload, hourly)
                    )
                )

    print(
        f"{label}: wrote {len(written)} day partition(s) "
        f"({len(missing)} missing, {len(existing)} existing)"
    )
    return written


@flow(name="ingest-open-meteo", log_prints=True)
def ingest_open_meteo(
    start_date: str | None = None, end_date: str | None = None
) -> list[str]:
    """Ingest raw Open-Meteo JSON for every active station."""

    logger = get_run_logger()
    settings = get_settings(start_date, end_date)
    stations = get_stations()

    logger.info(
        "Ingesting Open-Meteo into bronze for %d station(s), %s..%s (UTC)",
        len(stations),
        settings.start_date,
        settings.end_date,
    )

    written: list[str] = []
    for station in stations:
        written.extend(ingest_station_weather(station, settings))

    return written