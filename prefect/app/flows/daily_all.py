"""One daily run: bronze, then silver, then gold.

The three layers used to be scheduled independently at 02:00, 02:30 and 03:00,
so gold could start while silver was still writing. This flow chains them in one
run and only continues when the previous layer completes, which removes the
dependency on the clock.
"""

from __future__ import annotations

import sys
from pathlib import Path

_APP_ROOT = Path(__file__).resolve().parents[1]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from flows.gold_flow import gold_pipeline
from flows.pipeline import bronze_pipeline
from flows.silver_flow import silver_pipeline

from prefect import flow, get_run_logger


@flow(name="daily-all", log_prints=True)
def daily_all() -> dict:
    """Run bronze, silver and gold in order; stop if a layer fails."""

    logger = get_run_logger()

    bronze = bronze_pipeline()
    logger.info(
        "Bronze finished: %d Open-Meteo partition(s), %d USGS partition(s)",
        len(bronze.get("open_meteo", [])),
        len(bronze.get("usgs", [])),
    )

    silver = silver_pipeline(backfill=False)
    logger.info("Silver finished: %s", silver.get("tables", {}))

    gold = gold_pipeline(backfill=False)
    logger.info("Gold finished: %s", gold.get("rows", {}))

    return {"bronze": bronze, "silver": silver, "gold": gold}


if __name__ == "__main__":
    daily_all()
