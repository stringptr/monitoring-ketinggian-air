"""Shared pytest configuration.

The marker is registered here so the Spark tests are easy to skip with
``-m "not slow"`` without turning the repository into a package.
"""

from __future__ import annotations

import sys
from pathlib import Path


def pytest_configure(config) -> None:
    config.addinivalue_line(
        "markers", "slow: needs pyspark and a Java runtime"
    )


def _add_app_path() -> None:
    candidates = [
        Path("/prefect/app"),
        Path(__file__).resolve().parents[1] / "prefect" / "app",
    ]
    for candidate in candidates:
        if (candidate / "gold").is_dir():
            if str(candidate) not in sys.path:
                sys.path.insert(0, str(candidate))
            break


_add_app_path()
