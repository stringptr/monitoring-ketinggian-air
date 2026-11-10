"""Top level bronze ingestion pipeline.

Orchestrates the two source flows. Silver/gold transformations are intentionally
deferred; this stage only populates the bronze bucket.
"""

from __future__ import annotations

from prefect import flow, get_run_logger

from flows.open_meteo import ingest_open_meteo
from flows.usgs import ingest_usgs


@flow(name="bronze-pipeline", log_prints=True)
def bronze_pipeline(start_date: str | None = None) -> dict[str, list[str]]:
    """Ingest Open-Meteo and USGS data into bronze for all active stations."""

    logger = get_run_logger()

    open_meteo_locations = ingest_open_meteo(start_date=start_date)
    usgs_locations = ingest_usgs(start_date=start_date)

    logger.info(
        "Bronze ingestion complete: %d Open-Meteo partition(s), %d USGS partition(s)",
        len(open_meteo_locations),
        len(usgs_locations),
    )

    return {"open_meteo": open_meteo_locations, "usgs": usgs_locations}


if __name__ == "__main__":
    bronze_pipeline()
