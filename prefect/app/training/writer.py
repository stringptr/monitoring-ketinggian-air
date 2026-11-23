"""Write model metrics and model artifacts to the gold bucket."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from gold.delta import write_delta
from training.config import TrainingSettings


def _s3(settings: TrainingSettings):
    import s3fs

    return s3fs.S3FileSystem(
        key=settings.access_key,
        secret=settings.secret_key,
        client_kwargs={
            "endpoint_url": settings.s3_endpoint_url,
            "region_name": settings.s3_region,
        },
    )


def write_metrics(frame: pd.DataFrame, settings: TrainingSettings) -> str:
    """Write the current metrics snapshot as a Delta table.

    MLflow keeps the historical runs; this table is the latest snapshot used by
    the serving side.
    """

    return write_delta(frame, settings, "model_metrics", mode="overwrite")


def write_models(
    models: dict,
    metadata: list[dict],
    settings: TrainingSettings,
) -> str:
    """Save LightGBM text models and metadata under ``gold/models``."""

    version = metadata[0]["model_version"]
    prefix = f"model_version={version}"

    if settings.s3_endpoint_url:
        filesystem = _s3(settings)
        root = f"{settings.s3_bucket_gold}/models"
        base = f"{root}/{prefix}"

        for (site, horizon), model in models.items():
            body = model.booster_.model_to_string()
            path = f"{base}/station_id={site}/horizon={horizon}/model.txt"
            with filesystem.open(path, "wb") as handle:
                handle.write(body.encode("utf-8"))

        metadata_json = json.dumps(metadata, indent=2, default=str)
        with filesystem.open(f"{base}/metadata.json", "wb") as handle:
            handle.write(metadata_json.encode("utf-8"))
        return base

    base = (
        Path(settings.local_output_dir)
        / "gold"
        / "models"
        / prefix
    )
    for (site, horizon), model in models.items():
        path = base / f"station_id={site}" / f"horizon={horizon}"
        path.mkdir(parents=True, exist_ok=True)
        model.booster_.save_model(str(path / "model.txt"))
    (base / "metadata.json").write_text(
        json.dumps(metadata, indent=2, default=str), encoding="utf-8"
    )
    return str(base)
