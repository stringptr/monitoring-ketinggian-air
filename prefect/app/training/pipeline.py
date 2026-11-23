"""Orchestration of one training run."""

from __future__ import annotations

import pandas as pd

from training.config import TrainingSettings
from training.dataset import read_features, split_features
from training.tracking import log_run
from training.train import train_all
from training.writer import write_metrics, write_models


def run_training(settings: TrainingSettings) -> dict:
    """Read gold, train 9 models, evaluate and write metrics plus artifacts."""

    frame = read_features(settings)
    splits = split_features(frame, settings)

    if not len(splits["train"]):
        raise RuntimeError("the train split is empty; check gold coverage and dates")

    result = train_all(splits, settings)
    model_version = result.metadata[0]["model_version"]

    metrics_location = write_metrics(result.metrics, settings)
    models_location = write_models(result.models, result.metadata, settings)

    used = pd.concat(splits.values(), ignore_index=True)
    dataset_version = ",".join(
        sorted(used["dataset_version"].dropna().astype(str).unique())
    )
    discharge_grain = ",".join(
        sorted(used["discharge_grain"].dropna().astype(str).unique())
    )

    summary = {
        "model_version": model_version,
        "dataset_version": dataset_version,
        "discharge_grain": discharge_grain,
        "rows": {name: int(len(part)) for name, part in splits.items()},
        "promoted": {
            f"{site}_h{horizon}": accepted
            for (site, horizon), accepted in result.promoted.items()
        },
        "models": len(result.models),
        "metrics": metrics_location,
        "artifacts": models_location,
    }
    summary["mlflow"] = log_run(result, summary, settings)
    print(f"training complete: {summary}")
    return summary
