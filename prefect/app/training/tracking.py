"""Optional MLflow tracking for a training run.

The pipeline stays runnable without MLflow. Tracking is enabled only when
``MLFLOW_TRACKING_URI`` is set, which keeps local smoke tests independent from
the tracking server.
"""

from __future__ import annotations

import math
import os

from training.config import TrainingSettings


def tracking_uri() -> str | None:
    value = os.getenv("MLFLOW_TRACKING_URI", "").strip()
    return value or None


def _log_model(mlflow, model, site: str, horizon: int) -> None:
    name = f"hydrology-{site}-h{horizon}"
    try:
        mlflow.lightgbm.log_model(
            model,
            name=f"{site}_h{horizon}",
            registered_model_name=name,
        )
    except TypeError:
        # MLflow 2.x uses artifact_path.
        mlflow.lightgbm.log_model(
            model,
            artifact_path=f"{site}_h{horizon}",
            registered_model_name=name,
        )


def log_run(result, summary: dict, settings: TrainingSettings) -> dict:
    """Log parameters, metrics, models and lineage to MLflow."""

    uri = tracking_uri()
    if not uri:
        return {"enabled": False}

    import mlflow

    mlflow.set_tracking_uri(uri)
    experiment = os.getenv("MLFLOW_EXPERIMENT", "hydrology-gold")
    mlflow.set_experiment(experiment)

    with mlflow.start_run(run_name=summary["model_version"]) as run:
        mlflow.log_params(
            {
                "train_start": settings.train_start,
                "train_end": settings.train_end,
                "val_start": settings.val_start,
                "val_end": settings.val_end,
                "test_start": settings.test_start,
                "test_end": settings.test_end,
                "gate_improvement": settings.gate_improvement,
                "include_partial": settings.include_partial,
                "feature_count": len(result.metadata[0]["features"]),
                "dataset_version": summary.get("dataset_version", ""),
                "discharge_grain": summary.get("discharge_grain", ""),
            }
        )
        mlflow.set_tags(
            {
                "dataset_version": summary.get("dataset_version", ""),
                "discharge_grain": summary.get("discharge_grain", ""),
                "model_version": summary["model_version"],
            }
        )

        for row in result.metrics.itertuples(index=False):
            label = "baseline" if row.model_version == "persistence" else "candidate"
            prefix = f"{label}_{row.split}_{row.station_id}_h{row.horizon}"
            for metric in ("mae", "rmse", "bias", "nse", "kge", "pod", "far", "csi"):
                value = getattr(row, metric, None)
                if value is None:
                    continue
                number = float(value)
                if math.isfinite(number):
                    mlflow.log_metric(f"{prefix}_{metric}", number)

        for (site, horizon), model in result.models.items():
            _log_model(mlflow, model, site, horizon)

        mlflow.log_dict(summary, "summary.json")
        return {
            "enabled": True,
            "run_id": run.info.run_id,
            "experiment": experiment,
            "tracking_uri": uri,
        }
