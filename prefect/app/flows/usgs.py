"""USGS hydrology ingestion flow.

Pulls raw JSON straight from the USGS Water Data OGC API and lands it in the
bronze bucket without any transformation:

* ``00060`` (discharge) is ingested from the ``daily`` collection.
* ``00065`` (gage height) is ingested from the ``continuous`` collection.

Data is partitioned by observation day (UTC): every date from
``HYDROLOGY_START_DATE`` (default ``2023-01-01``) through today gets its own
``ingest_date=YYYY-MM-DD/part-000.json``. Existing partitions are skipped, so a
long backfill can be stopped and resumed without repeating completed days.

Missing dates are fetched in batches: contiguous ranges are downloaded in
calendar-year chunks (the API caps the continuous collection at 1100 days) and
split into one file per day. Requests are throttled to the documented quota.
"""

from __future__ import annotations

from datetime import date, timedelta

from prefect import flow, get_run_logger, task

from datalake import (
    contiguous_runs,
    date_range,
    existing_dates,
    usgs_prefix,
    write_payload,
    year_chunks,
)
from ratelimit import USGS_LIMITER, get_json
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
    """Fetch every page of an OGC API collection and merge the features."""

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

        payload = get_json(
            USGS_LIMITER, next_url, params=next_params, headers=headers
        )

        if merged is None:
            merged = {key: value for key, value in payload.items() if key != "links"}

        features.extend(payload.get("features", []))
        next_url = _next_link(payload)
        next_params = None  # the next link already carries the query string

    if merged is None:
        merged = {"type": "FeatureCollection", "features": []}

    features.sort(key=lambda feature: feature.get("properties", {}).get("time", ""))

    merged["type"] = "FeatureCollection"
    merged["features"] = features
    merged["numberReturned"] = len(features)
    merged.pop("links", None)
    return merged


def fetch_chunk(
    collection_url: str,
    base_params: dict,
    api_key: str,
    start_date: str,
    end_date: str,
) -> dict:
    """Fetch a single (<= 1 year) date window."""

    params = dict(base_params)
    params["datetime"] = f"{start_date}/{end_date}"
    return fetch_collection(collection_url, params, api_key)


def split_features_by_date(features: list[dict]) -> dict[str, list[dict]]:
    """Group raw features by the UTC date in ``properties.time``."""

    grouped: dict[str, list[dict]] = {}
    seen: set[str] = set()

    for feature in features:
        properties = feature.get("properties", {})
        timestamp = properties.get("time") or ""
        day = timestamp[:10]
        if not day:
            continue

        key = feature.get("id") or (
            f"{properties.get('time_series_id')}|{timestamp}"
            f"|{properties.get('statistic_id')}"
        )
        if key in seen:
            continue
        seen.add(key)
        grouped.setdefault(day, []).append(feature)

    for day_features in grouped.values():
        day_features.sort(
            key=lambda feature: feature.get("properties", {}).get("time", "")
        )
    return grouped


def _empty_collection() -> dict:
    return {"type": "FeatureCollection", "features": [], "numberReturned": 0}


@task(retries=3, retry_delay_seconds=30, log_prints=True)
def ingest_usgs_parameter(
    station: Station,
    data_type: str,
    parameter_code: str,
    collection_url: str,
    settings: Settings,
) -> list[str]:
    """Fetch every missing day for one USGS parameter/station."""

    if not settings.usgs_api_key:
        raise ValueError("USGS_API_KEY is not set")

    prefix = usgs_prefix(station.site_no, data_type, parameter_code)
    existing = existing_dates(settings, prefix)
    missing = [
        day
        for day in date_range(settings.start_date, settings.end_date)
        if day not in existing
    ]

    label = f"usgs {data_type}/{parameter_code} {station.site_no}"
    if not missing:
        print(f"{label}: up to date (nothing to fetch)")
        return []

    missing_set = set(missing)
    grace_cutoff = (
        date.fromisoformat(settings.end_date)
        - timedelta(days=settings.empty_grace_days)
    ).isoformat()

    base_params = {
        "monitoring_location_id": station.id,
        "parameter_code": parameter_code,
        "limit": PAGE_LIMIT,
        "f": "json",
    }

    written: list[str] = []
    for run_start, run_end in contiguous_runs(missing):
        for chunk_start, chunk_end in year_chunks(run_start, run_end):
            payload = fetch_chunk(
                collection_url,
                base_params,
                settings.usgs_api_key,
                chunk_start,
                chunk_end,
            )
            grouped = split_features_by_date(payload.get("features", []))

            for day in date_range(chunk_start, chunk_end):
                if day not in missing_set:
                    continue

                features = grouped.get(day, [])
                if not features and day > grace_cutoff:
                    # Recent days may not be published yet; retry next run.
                    continue

                day_payload = {
                    "type": "FeatureCollection",
                    "features": features,
                    "numberReturned": len(features),
                }
                written.append(write_payload(settings, prefix, day, day_payload))

    print(
        f"{label}: wrote {len(written)} day partition(s) "
        f"({len(missing)} missing, {len(existing)} existing)"
    )
    return written


@flow(name="ingest-usgs", log_prints=True)
def ingest_usgs(
    start_date: str | None = None, end_date: str | None = None
) -> list[str]:
    """Ingest raw USGS hydrology JSON for every active station/parameter."""

    logger = get_run_logger()
    settings = get_settings(start_date, end_date)
    stations = get_stations()

    logger.info(
        "Ingesting USGS into bronze for %d station(s), %s..%s (UTC)",
        len(stations),
        settings.start_date,
        settings.end_date,
    )

    written: list[str] = []
    for station in stations:
        for data_type, parameter_code, collection_url in USGS_PARAMETERS:
            written.extend(
                ingest_usgs_parameter(
                    station,
                    data_type,
                    parameter_code,
                    collection_url,
                    settings,
                )
            )

    return written