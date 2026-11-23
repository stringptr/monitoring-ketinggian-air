"""The partial flag must follow the real current water year."""

from __future__ import annotations

from datetime import datetime, timezone

from gold.features import current_water_year


def test_water_year_starts_in_october() -> None:
    assert current_water_year(datetime(2026, 9, 30, 12, tzinfo=timezone.utc)) == 2026
    assert current_water_year(datetime(2026, 10, 1, 12, tzinfo=timezone.utc)) == 2027


def test_local_time_decides_the_boundary() -> None:
    # 2026-10-01 04:00 UTC is still 2026-09-30 in California.
    assert current_water_year(datetime(2026, 10, 1, 4, tzinfo=timezone.utc)) == 2026


def test_january_stays_in_the_same_water_year() -> None:
    assert current_water_year(datetime(2027, 1, 15, 12, tzinfo=timezone.utc)) == 2027
