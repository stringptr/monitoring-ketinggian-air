"""Prefect flow for the gold layer."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

_APP_ROOT = Path(__file__).resolve().parents[1]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from gold.config import GoldSettings, default_end_date, rolling_window
from gold.pipeline import run_gold

from prefect import flow, get_run_logger, task


@task(retries=2, retry_delay_seconds=60, log_prints=True)
def transform_gold(start_date: str, end_date: str) -> dict:
    settings = GoldSettings.from_env()
    return run_gold(start_date, end_date, settings)


@flow(name="gold-transform", log_prints=True)
def gold_pipeline(
    start_date: str | None = None,
    end_date: str | None = None,
    backfill: bool = False,
) -> dict:
    """Build the hourly gold dataset and the model features.

    ``backfill=False`` processes the rolling UTC date window configured by
    ``GOLD_LOOKBACK_DAYS`` (default 7 days). ``backfill=True`` processes
    ``start_date`` (default: the historic start date) through ``end_date``
    (default: yesterday UTC).
    """

    logger = get_run_logger()
    settings = GoldSettings.from_env()

    if backfill:
        window_start = start_date or settings.historic_start_date
        window_end = end_date or default_end_date()
    else:
        window_start, window_end = rolling_window(settings, end_date)

    if date.fromisoformat(window_start) > date.fromisoformat(window_end):
        raise ValueError(f"start_date {window_start} is after end_date {window_end}")

    logger.info(
        "Gold transform %s..%s UTC dates (backfill=%s, lookback=%d days)",
        window_start,
        window_end,
        backfill,
        settings.lookback_days,
    )

    result = transform_gold(window_start, window_end)
    logger.info("Gold result: %s", result)
    return result


if __name__ == "__main__":
    gold_pipeline(backfill=True)
