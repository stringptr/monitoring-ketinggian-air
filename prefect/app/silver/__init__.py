"""Native-resolution silver layer for the hydrology project.

The package turns the raw bronze JSON written by the ingestion flows into
cleaned, typed tables at the original observation resolution (15 minutes,
hourly, and native daily). It only deduplicates, cleans, adds local-time
columns, validates contracts and records data quality. Gold, modelling and
dashboards are intentionally out of scope.

Rows stay in the same ``ingest_date`` partition as bronze. ``time_utc`` is
preserved and ``time_local`` / ``date_local`` are added for consumers that
need the station's local calendar day.
"""

from __future__ import annotations

from silver.config import SilverSettings

__all__ = ["SilverSettings"]
