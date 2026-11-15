"""Daily end-to-end pipeline: bronze ingestion followed by silver transform.

This is an additive wrapper around the existing ``bronze_pipeline``; the
bronze code itself is not modified. Deploy this entrypoint and deactivate the
old ``bronze-ingest`` deployment in the Prefect UI so a day is only processed
once.
"""

from __future__ import annotations

import sys
from pathlib import Path

_APP_ROOT = Path(__file__).resolve().parents[1]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from flows.pipeline import bronze_pipeline
from flows.silver_flow import silver_pipeline
from prefect import flow, get_run_logger


@flow(name="daily-bronze-silver", log_prints=True)
def daily_pipeline() -> dict:
    logger = get_run_logger()

    bronze = bronze_pipeline()
    logger.info(
        "Bronze ingestion finished: %d Open-Meteo partition(s), %d USGS partition(s)",
        len(bronze.get("open_meteo", [])),
        len(bronze.get("usgs", [])),
    )

    silver = silver_pipeline(backfill=False)
    logger.info("Silver transform finished: %s", silver.get("tables", {}))

    return {"bronze": bronze, "silver": silver}


if __name__ == "__main__":
    daily_pipeline()
