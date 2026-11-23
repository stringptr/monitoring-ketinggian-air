"""Compatibility wrapper for the shared Spark session.

The implementation lives in ``common/spark.py`` so silver and gold can both use
it without importing each other. This module stays so existing silver imports
keep working.
"""

from __future__ import annotations

from common.spark import build_spark_session

__all__ = ["build_spark_session"]
