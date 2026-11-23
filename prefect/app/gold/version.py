"""Identity of the gold dataset definition.

``dataset_version`` describes the definition, not the data range: it changes
only when the feature list, the windows, the timezone or the completeness rule
changes. When the data range changes, the pipeline records ``data_start_date``,
``data_end_date`` and ``processed_at`` separately in ``gold/dataset_versions``.
"""

from __future__ import annotations

import hashlib
import json

from gold.config import STAGE_COMPLETE_MIN_SAMPLES, TARGET_HORIZONS, TIMEZONE_NAME
from gold.features import (
    DISCHARGE_LAGS,
    MIN_PERIOD_RATIO,
    PRECIP_WINDOWS,
    STAGE_DIFFS,
    STAGE_LAGS,
    STAGE_WINDOWS,
)
from gold.schemas import FEATURE_COLS, TARGET_COLS

PREFIX = "g1"


def definition(discharge_source: str = "auto") -> dict:
    return {
        "features": list(FEATURE_COLS),
        "targets": list(TARGET_COLS),
        "stage_lags": list(STAGE_LAGS),
        "stage_windows": list(STAGE_WINDOWS),
        "stage_diffs": list(STAGE_DIFFS),
        "discharge_lags": list(DISCHARGE_LAGS),
        "discharge_source": discharge_source,
        "precip_windows": list(PRECIP_WINDOWS),
        "min_period_ratio": MIN_PERIOD_RATIO,
        "stage_complete_min_samples": STAGE_COMPLETE_MIN_SAMPLES,
        "horizons": list(TARGET_HORIZONS),
        "timezone": TIMEZONE_NAME,
    }


def dataset_version(discharge_source: str = "auto") -> str:
    """Return a stable identifier for the current dataset definition.

    The discharge source is part of the identity: hourly and daily discharge
    produce different ``discharge_lag_*`` values even though the column names
    and windows are identical.
    """

    payload = json.dumps(
        definition(discharge_source), sort_keys=True, separators=(",", ":")
    )
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:8]
    return f"{PREFIX}-{digest}"
