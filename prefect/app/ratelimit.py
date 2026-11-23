"""Request throttling for the upstream APIs.

USGS allows at most 1000 requests per hour and Open-Meteo at most 5000. A
``RateLimiter`` enforces a minimum spacing between calls which guarantees the
rolling-hour quota is never exceeded. ``get_json`` retries HTTP 429 through the
``Retry-After`` header, and retries 5xx plus transient network failures with
exponential backoff, because the free upstream APIs have no uptime guarantee.
"""

from __future__ import annotations

import random
import threading
import time
from typing import Any

import requests

#: Published per-hour request quotas.
USGS_REQUESTS_PER_HOUR = 1_000
OPEN_METEO_REQUESTS_PER_HOUR = 5_000

DEFAULT_TIMEOUT = 180
DEFAULT_MAX_RETRIES = 5
RETRYABLE_STATUS = frozenset({500, 502, 503, 504})
RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def backoff_seconds(attempt: int) -> float:
    """Exponential backoff with jitter, capped at one minute."""

    return min(60.0, 2.0**attempt) + random.uniform(0.0, 1.0)


class RateLimiter:
    """Space requests so no more than ``requests_per_hour`` are sent."""

    def __init__(self, requests_per_hour: int) -> None:
        if requests_per_hour <= 0:
            raise ValueError("requests_per_hour must be positive")
        self.min_interval = 3600.0 / requests_per_hour
        self._last_request = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = self.min_interval - (now - self._last_request)
            if delay > 0:
                time.sleep(delay)
            self._last_request = time.monotonic()


def get_json(
    limiter: RateLimiter,
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> Any:
    """GET ``url`` (throttled) and return the decoded JSON body.

    Retries throttling (429), upstream overload (5xx) and transient network
    failures. The upstream APIs are free services without an uptime guarantee,
    so a single 503 must not fail a whole daily run.
    """

    attempt = 0
    while True:
        limiter.wait()
        try:
            response = requests.get(
                url, params=params, headers=headers, timeout=timeout
            )
        except RETRYABLE_EXCEPTIONS:
            if attempt >= max_retries:
                raise
            attempt += 1
            time.sleep(backoff_seconds(attempt))
            continue

        if response.status_code == 429 and attempt < max_retries:
            attempt += 1
            retry_after = response.headers.get("Retry-After")
            try:
                delay = float(retry_after)
            except (TypeError, ValueError):
                delay = 60.0
            time.sleep(max(delay, 1.0))
            continue

        if response.status_code in RETRYABLE_STATUS and attempt < max_retries:
            attempt += 1
            time.sleep(backoff_seconds(attempt))
            continue

        response.raise_for_status()
        return response.json()


USGS_LIMITER = RateLimiter(USGS_REQUESTS_PER_HOUR)
OPEN_METEO_LIMITER = RateLimiter(OPEN_METEO_REQUESTS_PER_HOUR)