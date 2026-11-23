"""Training metrics, split and a small end-to-end training smoke test."""

from __future__ import annotations

import numpy as np
import pandas as pd

from gold.schemas import FEATURE_COLS
from stations import STATIONS
from training.config import TrainingSettings
from training.dataset import selected_columns, split_features
from training.metrics import (
    event_metrics,
    promotion_passes,
    regression_metrics,
)
from training.tracking import log_run, tracking_uri
from training.train import train_all


def _settings(**overrides) -> TrainingSettings:
    values = dict(
        train_start="2020-10-01",
        train_end="2021-09-30",
        val_start="2021-10-01",
        val_end="2022-09-30",
        test_start="2022-10-01",
        test_end="2023-09-30",
        gate_improvement=0.02,
        min_train_rows=10,
        model_dir="/tmp/models",
        s3_endpoint_url=None,
        s3_region="garage",
        s3_bucket_gold="gold",
        local_output_dir="/tmp",
        access_key=None,
        secret_key=None,
    )
    values.update(overrides)
    return TrainingSettings(**values)


def test_regression_metrics_perfect() -> None:
    result = regression_metrics([1.0, 2.0, 3.0], [1.0, 2.0, 3.0])
    assert result["mae"] == 0.0
    assert result["nse"] == 1.0
    assert result["kge"] == 1.0


def test_regression_metrics_known_error() -> None:
    result = regression_metrics([1.0, 2.0, 3.0], [1.0, 2.0, 4.0])
    assert result["n"] == 3
    assert round(result["mae"], 4) == round(1 / 3, 4)
    assert round(result["bias"], 4) == round(1 / 3, 4)


def test_event_metrics() -> None:
    result = event_metrics([1.0, 2.0, 3.0], [1.0, 3.0, 3.0], threshold=2.0)
    assert result["pod"] == 1.0
    assert result["far"] == 0.0
    assert result["csi"] == 1.0


def test_promotion_gate() -> None:
    assert promotion_passes(0.9, 1.0, 0.02) is True
    assert promotion_passes(0.99, 1.0, 0.02) is False
    assert promotion_passes(0.9, 0.0, 0.02) is False


def _split_frame() -> pd.DataFrame:
    rows = []
    for start in ("2021-01-01", "2022-01-01", "2023-01-01"):
        rows.append(
            {
                "time_utc": pd.Timestamp(start, tz="UTC"),
                "is_trainable": True,
                "is_partial": False,
            }
        )
    rows.append(
        {
            "time_utc": pd.Timestamp("2021-02-01", tz="UTC"),
            "is_trainable": True,
            "is_partial": True,
        }
    )
    return pd.DataFrame(rows)


def test_split_filters_and_dates() -> None:
    splits = split_features(_split_frame(), _settings())
    assert len(splits["train"]) == 1
    assert len(splits["val"]) == 1
    assert len(splits["test"]) == 1


def test_split_can_include_partial_for_smoke() -> None:
    splits = split_features(
        _split_frame(), _settings(include_partial=True)
    )
    assert len(splits["train"]) == 2


def _synthetic_frame() -> pd.DataFrame:
    frames = []
    for site_index, station in enumerate(STATIONS):
        for split, start in (
            ("train", "2020-11-01"),
            ("val", "2021-11-01"),
            ("test", "2022-11-01"),
        ):
            count = 120
            times = pd.date_range(start, periods=count, freq="h", tz="UTC")
            stage = 5.0 + site_index + np.linspace(0, 2, count)
            data = {
                "station_id": station.site_no,
                "time_utc": times,
                "water_year": times.year + (times.month >= 10).astype(int),
                "is_trainable": True,
                "is_partial": False,
                "stage_now": stage,
            }
            for column in FEATURE_COLS:
                data[column] = np.random.default_rng(hash(column) % 2**32).random(count)
            for horizon in (1, 6, 24):
                data[f"y_h{horizon}"] = stage + 0.05 * horizon
            frames.append(pd.DataFrame(data))
    return pd.concat(frames, ignore_index=True)


def test_selected_columns_are_unique() -> None:
    columns = selected_columns()
    assert len(columns) == len(set(columns))
    assert columns.count("stage_now") == 1


def test_tracking_is_optional(monkeypatch) -> None:
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    assert tracking_uri() is None
    assert log_run(None, {}, _settings()) == {"enabled": False}


def test_train_all_produces_nine_models(monkeypatch) -> None:
    monkeypatch.setattr(
        "training.train.LGB_PARAMS",
        {
            "objective": "regression_l1",
            "metric": "mae",
            "n_estimators": 30,
            "learning_rate": 0.1,
            "num_leaves": 7,
            "min_child_samples": 5,
            "verbosity": -1,
            "random_state": 42,
        },
    )
    splits = split_features(_synthetic_frame(), _settings())
    result = train_all(splits, _settings())

    assert len(result.models) == 9
    assert len(result.metadata) == 9
    assert set(result.promoted) == {
        (station.site_no, horizon)
        for station in STATIONS
        for horizon in (1, 6, 24)
    }
    # 9 models x 2 splits x (candidate + persistence)
    assert len(result.metrics) == 36
    assert set(result.metrics["model_version"]) == {
        result.metadata[0]["model_version"],
        "persistence",
    }
