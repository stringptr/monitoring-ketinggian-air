"""Prefect flow for training and scheduled retraining."""

from __future__ import annotations

import sys
from pathlib import Path

_APP_ROOT = Path(__file__).resolve().parents[1]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from prefect import flow, get_run_logger, task

from training.config import TrainingSettings
from training.pipeline import run_training


@task(retries=2, retry_delay_seconds=60, log_prints=True)
def train_models(settings: TrainingSettings) -> dict:
    return run_training(settings)


@flow(name="training-pipeline", log_prints=True)
def training_pipeline() -> dict:
    """Train all station/horizon models from gold/features."""

    logger = get_run_logger()
    settings = TrainingSettings.from_env()
    logger.info(
        "Training from gold %s..%s (val %s..%s, test %s..%s, gate %.1f%%)",
        settings.train_start,
        settings.train_end,
        settings.val_start,
        settings.val_end,
        settings.test_start,
        settings.test_end,
        settings.gate_improvement * 100,
    )
    result = train_models(settings)
    logger.info("Training result: %s", result)
    return result


if __name__ == "__main__":
    training_pipeline()
