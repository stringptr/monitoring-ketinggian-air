"""USGS monitoring locations used by the hydrology pipeline.

Coordinates are the official NAD83 decimal lat/lon published by the USGS Site
Service for each monitoring location. Every station in this project is in
California and shares the ``America/Los_Angeles`` timezone.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_TIMEZONE = "America/Los_Angeles"


@dataclass(frozen=True)
class Station:
    """A USGS monitoring location enriched with the coordinates needed by
    Open-Meteo."""

    #: Identifier in the USGS Water Data (OGC) API, e.g. ``USGS-11525670``.
    id: str
    #: Bare site number as used by the legacy NWIS services, e.g. ``11525670``.
    site_no: str
    name: str
    latitude: float
    longitude: float
    timezone: str = DEFAULT_TIMEZONE


STATIONS: tuple[Station, ...] = (
    Station(
        id="USGS-11525670",
        site_no="11525670",
        name="Indian C near Douglas City, CA",
        latitude=40.65178087,
        longitude=-122.9144343,
    ),
    Station(
        id="USGS-11150500",
        site_no="11150500",
        name="Salinas River near Bradley, CA",
        latitude=35.9302434,
        longitude=-120.8687915,
    ),
    Station(
        id="USGS-11454000",
        site_no="11454000",
        name="Putah Creek near Winters, CA",
        latitude=38.5151562,
        longitude=-122.0818793,
    ),
)


def get_stations() -> tuple[Station, ...]:
    """Return the active stations.

    ``STATIONS`` (comma separated) can restrict the selection to a subset,
    matching either the ``USGS-`` prefixed id or the bare site number. When the
    variable is unset or matches nothing, all three project stations are used.
    """

    raw = os.getenv("STATIONS")
    if not raw:
        return STATIONS

    wanted = {token.strip() for token in raw.split(",") if token.strip()}
    selected = tuple(
        station
        for station in STATIONS
        if station.id in wanted or station.site_no in wanted
    )
    return selected or STATIONS
