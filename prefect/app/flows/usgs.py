"""USGS hydrology ingestion flow.

Pulls raw JSON straight from the USGS Water Data OGC API and lands it in the
bronze bucket without any transformation:

* ``00060`` (discharge) is ingested from the ``daily`` collection.
* ``00065`` (gage height) is ingested from the ``continuous`` collection.

The first run for a station/parameter fetches the full history from
``HYDROLOGY_START_DATE`` (default ``2023-01-01``). Once an ``ingest_date=*``
partition exists, only the rolling last 7 days are fetched. Long ranges are
downloaded in calendar-year chunks (the API caps the continuous collection at
1100 days) and every chunk/page is merged into a single ``part-000.json``.
"""

from __future__ import annotations

from datetime import date, timedelta

import requests
from prefect import flow, get_run_logger, task

from datalake import (
    incremental_start,
    partition_exists,
    usgs_prefix,
    write_payload,
)
from settings import Settings, get_settings
from stations import Station, get_stations

API_BASE = "https://api.waterdata.usgs.gov/ogcapi/v1/collections"
DAILY_ITEMS = f"{API_BASE}/daily/items"
CONTINUOUS_ITEMS = f"{API_BASE}/continuous/items"

#: USGS parameter codes and the collection each one is fetched from.
USGS_PARAMETERS: tuple[tuple[str, str, str], ...] = (
    ("daily", "00060", DAILY_ITEMS),  # discharge, daily values
    ("continuous", "00065", CONTINUOUS_ITEMS),  # gage height, continuous values
)

#: Page size used while walking the paginated response.
PAGE_LIMIT = 50_000
#: Safety guard so a malformed ``next`` link cannot loop forever.
MAX_PAGES = 1_000


def _next_link(payload: dict) -> str | None:
    for link in payload.get("links", []):
        if link.get("rel") == "next" and link.get("href"):
            return link["href"]
    return None


def fetch_collection(url: str, params: dict, api_key: str) -> dict:
    """Fetch every page of an OGC API collection and merge the features.

    The returned object keeps the source FeatureCollection envelope (minus the
    now-stale pagination links) with all features concatenated in order.
    """

    headers = {"X-Api-Key": api_key}
    features: list[dict] = []
    merged: dict | None = None

    next_url: str | None = url
    next_params: dict | None = params
    pages = 0

    while next_url:
        pages += 1
        if pages > MAX_PAGES:
            raise RuntimeError(f"Pagination exceeded {MAX_PAGES} pages for {url}")

        response = requests.get(
            next_url, params=next_params, headers=headers, timeout=180
        )
        response.raise_for_status()
        payload = response.json()

        if merged is None:
            merged = {key: value for key, value in payload.items() if key != "links"}

        features.extend(payload.get("features", []))
        next_url = _next_link(payload)
        next_params = None  # the next link already carries the query string

    if merged is None:
        merged = {"type": "FeatureCollection", "features": []}

    merged["type"] = "FeatureCollection"
    merged["features"] = features
    merged["numberReturned"] = len(features)
    merged.pop("links", None)
    return merged


def _chunks(start_date: str, end_date: str):
    """Yield non-overlapping calendar-year windows covering the range."""

    cursor = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    while cursor <= end:
        chunk_end = min(date(cursor.year, 12, 31), end)
        yield cursor.isoformat(), chunk_end.isoformat()
        cursor = chunk_end + timedelta(days=1)


def _feature_key(feature: dict) -> str:
    properties = feature.get("properties", {})
    return (
        feature.get("id")
        or f"{properties.get('time_series_id')}|{properties.get('time')}"
        f"|{properties.get('statistic_id')}"
    )


def fetch_range(
    collection_url: str,
    base_params: dict,
    api_key: str,
    start_date: str,
    end_date: str,
) -> dict:
    """Fetch ``start_date..end_date`` in year chunks and merge into one payload.

    Features are de-duplicated (adjacent windows never overlap, but the API can
    be inconsistent) and sorted chronologically before being returned.
    """

    merged: dict | None = None
    features: list[dict] = []
    seen: set[str] = set()

    for chunk_start, chunk_end in _chunks(start_date, end_date):
        params = dict(base_params)
        params["datetime"] = f"{chunk_start}/{chunk_end}"
        payload = fetch_collection(collection_url, params, api_key)

        if merged is None:
            merged = {
                key: value
                for key, value in payload.items()
                if key not in ("features", "numberReturned")
            }

        for feature in payload.get("features", []):
            key = _feature_key(feature)
            if key in seen:
                continue
            seen.add(key)
            features.append(feature)

    if merged is None:
        merged = {"type": "FeatureCollection"}

    features.sort(key=lambda feature: feature.get("properties", {}).get("time", ""))

    merged["type"] = "FeatureCollection"
    merged["features"] = features
    merged["numberReturned"] = len(features)
    merged.pop("links", None)
    return merged


@task(retries=3, retry_delay_seconds=30, log_prints=True)
def ingest_usgs_parameter(
    station: Station,
    data_type: str,
    parameter_code: str,
    collection_url: str,
    settings: Settings,
    ingest_date: str,
) -> str:
    """Ingest one USGS parameter for one station into a bronze partition."""

    if not settings.usgs_api_key:
        raise ValueError("USGS_API_KEY is not set")

    prefix = usgs_prefix(station.site_no, data_type, parameter_code)
    if partition_exists(settings, prefix):
        start_date = incremental_start(ingest_date)
    else:
        start_date = settings.start_date

    payload = fetch_range(
        collection_url,
        {
            "monitoring_location_id": station.id,
            "parameter_code": parameter_code,
            "limit": PAGE_LIMIT,
            "f": "json",
        },
        settings.usgs_api_key,
        start_date,
        settings.end_date,
    )

    location = write_payload(settings, prefix, ingest_date, payload)
    print(
        f"usgs {data_type}/{parameter_code} {station.site_no}: "
        f"{payload['numberReturned']} feature(s) "
        f"({start_date}..{settings.end_date}) -> {location}"
    )
    return location


@flow(name="ingest-usgs", log_prints=True)
def ingest_usgs(
    start_date: str | None = None, end_date: str | None = None
) -> list[str]:
    """Ingest raw USGS hydrology JSON for every active station/parameter."""

    logger = get_run_logger()
    settings = get_settings(start_date, end_date)
    ingest_date = settings.end_date
    stations = get_stations()

    logger.info(
        "Ingesting USGS into bronze for %d station(s), ingest_date=%s",
        len(stations),
        ingest_date,
    )

    written: list[str] = []
    for station in stations:
        for data_type, parameter_code, collection_url in USGS_PARAMETERS:
            written.append(
                ingest_usgs_parameter(
                    station,
                    data_type,
                    parameter_code,
                    collection_url,
                    settings,
                    ingest_date,
                )
            )

    return written
