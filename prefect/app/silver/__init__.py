"""Daily silver layer for the hydrology project.

The package turns the raw bronze JSON written by the ingestion flows into
tidy, typed daily tables, runs basic data-quality checks and records the
results. Gold, modelling and dashboards are intentionally out of scope.

Everything the layer produces is keyed by ``(station_id, date)`` where
``date`` is the **local** calendar day of the station (America/Los_Angeles).
The USGS daily values use that same water-day convention, so the whole
silver layer stays aligned with the discharge series.
"""

from __future__ import annotations

from silver.config import SilverSettings

__all__ = ["SilverSettings"]
