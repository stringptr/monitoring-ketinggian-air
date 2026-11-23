"""Gold layer: the joined hourly dataset used by the model and the dashboard.

Silver keeps three separate tables at their native resolution. Gold turns them
into one row per station per hour, then derives the model features, the
prediction log and the data-quality report. Nothing here fetches from the
internet: gold only reads silver.
"""

__all__ = [
    "config",
    "features",
    "hourly",
    "pipeline",
    "quality",
    "readers",
    "schemas",
    "writer",
]
