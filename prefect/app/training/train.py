"""Train one LightGBM model per station and horizon."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from gold.schemas import FEATURE_COLS, TARGET_COLS
from stations import get_stations
from training.config import LGB_PARAMS, TrainingSettings
from training.metrics import event_metrics, promotion_passes, regression_metrics


@dataclass
class TrainingResult:
    metrics: pd.DataFrame
    models: dict
    metadata: list[dict]
    promoted: dict


def _model_version() -> str:
    return f"m-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def _result_row(
    model_version: str,
    station: str,
    horizon: int,
    split: str,
    y_true,
    y_pred,
    threshold: float,
) -> dict:
    row = regression_metrics(y_true, y_pred)
    row.update(event_metrics(y_true, y_pred, threshold))
    row.update(
        {
            "model_version": model_version,
            "station_id": station,
            "horizon": horizon,
            "split": split,
            "generated_at": datetime.now(timezone.utc),
        }
    )
    return row


def train_all(
    splits: dict[str, pd.DataFrame],
    settings: TrainingSettings,
) -> TrainingResult:
    """Train 3 stations x 3 horizons and evaluate candidate vs persistence."""

    import lightgbm as lgb

    for name in ("train", "val", "test"):
        if splits[name].empty:
            raise RuntimeError(f"the {name} split is empty; check gold coverage")

    train = splits["train"]
    if len(train) < settings.min_train_rows:
        raise RuntimeError(
            f"only {len(train)} trainable rows; need {settings.min_train_rows}"
        )

    version = _model_version()
    metrics: list[dict] = []
    models: dict = {}
    metadata: list[dict] = []
    promoted: dict = {}

    for station in get_stations():
        site = station.site_no
        station_splits = {
            name: frame[frame["station_id"] == site].copy()
            for name, frame in splits.items()
        }
        if station_splits["train"].empty:
            raise RuntimeError(f"{site}: no trainable rows in the train split")

        for horizon, target_column in zip((1, 6, 24), TARGET_COLS):
            target_train = station_splits["train"][target_column]
            threshold = float(np.nanpercentile(target_train, 95))

            train_delta = (
                station_splits["train"][target_column]
                - station_splits["train"]["stage_now"]
            )
            val_delta = (
                station_splits["val"][target_column]
                - station_splits["val"]["stage_now"]
            )

            model = lgb.LGBMRegressor(**LGB_PARAMS)
            model.fit(
                station_splits["train"][list(FEATURE_COLS)],
                train_delta,
                eval_X=station_splits["val"][list(FEATURE_COLS)],
                eval_y=val_delta,
                eval_metric="mae",
                callbacks=[
                    lgb.early_stopping(100, verbose=False),
                    lgb.log_evaluation(0),
                ],
            )

            candidate_val = (
                model.predict(station_splits["val"][list(FEATURE_COLS)])
                + station_splits["val"]["stage_now"].to_numpy()
            )
            candidate_test = (
                model.predict(station_splits["test"][list(FEATURE_COLS)])
                + station_splits["test"]["stage_now"].to_numpy()
            )

            for split_name, prediction in (
                ("val", candidate_val),
                ("test", candidate_test),
            ):
                metrics.append(
                    _result_row(
                        version,
                        site,
                        horizon,
                        split_name,
                        station_splits[split_name][target_column].to_numpy(),
                        prediction,
                        threshold,
                    )
                )

            for split_name in ("val", "test"):
                part = station_splits[split_name]
                metrics.append(
                    _result_row(
                        "persistence",
                        site,
                        horizon,
                        split_name,
                        part[target_column].to_numpy(),
                        part["stage_now"].to_numpy(),
                        threshold,
                    )
                )

            baseline_val = [
                row
                for row in metrics
                if row["model_version"] == "persistence"
                and row["station_id"] == site
                and row["horizon"] == horizon
                and row["split"] == "val"
            ][0]["mae"]
            candidate_val_mae = [
                row
                for row in metrics
                if row["model_version"] == version
                and row["station_id"] == site
                and row["horizon"] == horizon
                and row["split"] == "val"
            ][0]["mae"]
            accepted = promotion_passes(
                candidate_val_mae,
                baseline_val,
                settings.gate_improvement,
            )
            promoted[(site, horizon)] = accepted

            models[(site, horizon)] = model
            metadata.append(
                {
                    "station": site,
                    "horizon": horizon,
                    "file": f"lightgbm_{site}_h{horizon}.txt",
                    "target": "y_h - stage_now",
                    "base_feature": "stage_now",
                    "features": list(FEATURE_COLS),
                    "best_iteration": int(getattr(model, "best_iteration_", 0) or 0),
                    "model_version": version,
                    "event_threshold": threshold,
                    "promoted": accepted,
                }
            )

    frame = pd.DataFrame(metrics)
    for column in ("mae", "rmse", "bias", "nse", "kge", "pod", "far", "csi"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return TrainingResult(frame, models, metadata, promoted)
