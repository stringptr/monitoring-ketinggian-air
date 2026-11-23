"""Completeness thresholds and severity semantics for silver DQ."""

from __future__ import annotations

from silver.quality import _result, completeness_failure


def test_full_day_passes() -> None:
    failed, details = completeness_failure(96, 96, 0.90)
    assert failed == 0
    assert details["ratio"] == 1.0


def test_day_above_threshold_passes() -> None:
    failed, details = completeness_failure(96, 90, 0.90)
    assert failed == 0
    assert details["ratio"] == 0.9375


def test_day_below_threshold_fails() -> None:
    failed, _ = completeness_failure(96, 86, 0.90)
    assert failed == 10


def test_meteo_threshold_is_applied() -> None:
    assert completeness_failure(24, 20, 0.95)[0] == 4
    assert completeness_failure(24, 23, 0.95)[0] == 0


def test_zero_expected_is_not_a_failure() -> None:
    assert completeness_failure(0, 0, 0.90)[0] == 0


def test_informational_findings_are_not_failures() -> None:
    row = _result(
        "run",
        "table",
        "2026-11-01",
        "11150500",
        "provisional_values",
        "info",
        12,
        96,
    )
    assert row["passed"] is True
    assert row["failed_count"] == 12
