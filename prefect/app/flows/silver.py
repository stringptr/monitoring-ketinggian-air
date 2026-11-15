"""Prefect flow for the daily silver transformation."""

from __future__ import annotations

from datetime import date

from silver.config import SilverSettings, default_end_date, rolling_window
from silver.pipeline import run_silver

from prefect import flow, get_run_logger, task


@task(retries=2, retry_delay_seconds=60, log_prints=True)
def transform_silver(
    start_date: str,
    end_date: str,
    backfill: bool,
) -> dict:
    settings = SilverSettings.from_env()
    return run_silver(
        start_date,
        end_date,
        settings,
        check_freshness=not backfill,
    )


@flow(name="silver-transform", log_prints=True)
def silver_pipeline(
    start_date: str | None = None,
    end_date: str | None = None,
    backfill: bool = False,
) -> dict:
    """Build the daily silver tables.

    ``backfill=False`` processes the rolling window configured by
    ``SILVER_LOOKBACK_DAYS`` (default 7 local days). ``backfill=True`` processes
    ``start_date`` (default: the historic start date) through ``end_date``
    (default: yesterday local time) in a single Spark job.
    """

    logger = get_run_logger()
    settings = SilverSettings.from_env()

    if backfill:
        window_start = start_date or settings.historic_start_date
        window_end = end_date or default_end_date()
    else:
        window_start, window_end = rolling_window(settings, end_date)

    if date.fromisoformat(window_start) > date.fromisoformat(window_end):
        raise ValueError(f"start_date {window_start} is after end_date {window_end}")

    logger.info(
        "Silver transform %s..%s (backfill=%s, lookback=%d days)",
        window_start,
        window_end,
        backfill,
        settings.lookback_days,
    )

    result = transform_silver(window_start, window_end, backfill)
    logger.info("Silver result: %s", result)
    return result


if __name__ == "__main__":
    silver_pipeline(backfill=True)
