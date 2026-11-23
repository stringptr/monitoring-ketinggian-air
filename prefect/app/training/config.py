"""Configuration for training and scheduled retraining."""

from __future__ import annotations

import os
from dataclasses import dataclass


DEFAULT_TRAIN_START = "2015-10-01"
DEFAULT_TRAIN_END = "2023-09-30"
DEFAULT_VAL_START = "2023-10-01"
DEFAULT_VAL_END = "2024-09-30"
DEFAULT_TEST_START = "2024-10-01"
DEFAULT_TEST_END = "2026-09-30"
DEFAULT_GATE_IMPROVEMENT = 0.02
DEFAULT_MODEL_DIR = "/prefect/models"
DEFAULT_MIN_TRAIN_ROWS = 5_000

#: Same hyperparameters as the modelling experiment.
LGB_PARAMS: dict = {
    "objective": "regression_l1",
    "metric": "mae",
    "learning_rate": 0.03,
    "num_leaves": 63,
    "min_child_samples": 50,
    "subsample": 0.8,
    "subsample_freq": 1,
    "colsample_bytree": 0.8,
    "reg_lambda": 1.0,
    "n_estimators": 3000,
    "importance_type": "gain",
    "random_state": 42,
    "n_jobs": -1,
    "verbosity": -1,
}


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    return float(raw) if raw is not None else default


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    return int(raw) if raw is not None else default


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class TrainingSettings:
    train_start: str
    train_end: str
    val_start: str
    val_end: str
    test_start: str
    test_end: str
    gate_improvement: float
    min_train_rows: int
    model_dir: str
    s3_endpoint_url: str | None
    s3_region: str
    s3_bucket_gold: str
    local_output_dir: str
    access_key: str | None
    secret_key: str | None
    #: Local smoke tests may train on the running water year. Production keeps
    #: this false so incomplete water years never enter training.
    include_partial: bool = False

    @classmethod
    def from_env(cls) -> TrainingSettings:
        return cls(
            train_start=_env("TRAIN_START", DEFAULT_TRAIN_START) or DEFAULT_TRAIN_START,
            train_end=_env("TRAIN_END", DEFAULT_TRAIN_END) or DEFAULT_TRAIN_END,
            val_start=_env("TRAIN_VAL_START", DEFAULT_VAL_START) or DEFAULT_VAL_START,
            val_end=_env("TRAIN_VAL_END", DEFAULT_VAL_END) or DEFAULT_VAL_END,
            test_start=_env("TRAIN_TEST_START", DEFAULT_TEST_START)
            or DEFAULT_TEST_START,
            test_end=_env("TRAIN_TEST_END", DEFAULT_TEST_END) or DEFAULT_TEST_END,
            gate_improvement=_env_float(
                "TRAIN_GATE_IMPROVEMENT", DEFAULT_GATE_IMPROVEMENT
            ),
            min_train_rows=_env_int("TRAIN_MIN_ROWS", DEFAULT_MIN_TRAIN_ROWS),
            model_dir=_env("TRAIN_MODEL_DIR", DEFAULT_MODEL_DIR) or DEFAULT_MODEL_DIR,
            s3_endpoint_url=_env("S3_ENDPOINT_URL"),
            s3_region=_env("S3_REGION", "garage") or "garage",
            s3_bucket_gold=_env("S3_BUCKET_GOLD", "gold") or "gold",
            local_output_dir=_env("BRONZE_LOCAL_DIR", "/prefect/data")
            or "/prefect/data",
            access_key=_env("GARAGE_DEFAULT_ACCESS_KEY"),
            secret_key=_env("GARAGE_DEFAULT_SECRET_KEY"),
            include_partial=_env_bool("TRAIN_INCLUDE_PARTIAL", False),
        )
