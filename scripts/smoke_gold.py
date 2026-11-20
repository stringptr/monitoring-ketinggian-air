"""Synthetic smoke test for the gold layer.

Creates two weeks of fake silver Parquet in a temporary directory, runs the
gold pipeline against it, and prints the row counts. No network and no API key
are needed, so the gold logic can be tested before the real backfill runs.

Run inside the worker container:

    docker compose exec prefect-worker python scripts/smoke_gold.py
"""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

APP_CANDIDATES = (
    Path("/prefect/app"),
    Path(__file__).resolve().parents[1] / "prefect" / "app",
)
for candidate in APP_CANDIDATES:
    if (candidate / "gold").is_dir():
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
        break

STATION = "11525670"
START = datetime(2024, 1, 1, tzinfo=timezone.utc)
DAYS = 12


def _hourly_rows() -> list[tuple]:
    rows = []
    total_hours = DAYS * 24
    for hour in range(total_hours):
        moment = START + timedelta(hours=hour)
        rows.append((moment, round(2.0 + 0.3 * (hour % 24) / 24, 4)))
    return rows


def _write_silver(spark, base: Path) -> None:
    from pyspark.sql import functions as F

    hourly = _hourly_rows()
    frame = spark.createDataFrame(hourly, "time_utc timestamp, stage double")
    frame = (
        frame.withColumn(
            "time_local", F.from_utc_timestamp(F.col("time_utc"), "America/Los_Angeles")
        )
        .withColumn("date_local", F.date_format("time_local", "yyyy-MM-dd"))
        .withColumn("station_id", F.lit(STATION))
        .withColumn("ingest_date", F.date_format("time_utc", "yyyy-MM-dd"))
        .withColumn("value", F.col("stage"))
    )

    continuous = (
        frame.select("station_id", "ingest_date", "time_utc", "time_local", "date_local", "value")
        .withColumn("time_utc", F.date_trunc("hour", F.col("time_utc")))
    )
    # Four samples per hour, like the real 15-minute series.
    samples = continuous
    for offset in (15, 30, 45):
        shifted = continuous.withColumn(
            "time_utc", F.col("time_utc") + F.expr(f"INTERVAL {offset} MINUTES")
        ).withColumn("time_local", F.from_utc_timestamp("time_utc", "America/Los_Angeles"))
        samples = samples.unionByName(shifted)

    samples.write.mode("overwrite").partitionBy("station_id", "ingest_date").parquet(
        str(base / "silver" / "usgs" / "continuous" / "parameter_code=00065")
    )

    daily = (
        frame.groupBy("station_id", "date_local")
        .agg(F.first("ingest_date").alias("ingest_date"), F.avg("value").alias("value"))
        .withColumn("value", F.col("value") * 10)
    )
    daily.write.mode("overwrite").partitionBy("station_id", "ingest_date").parquet(
        str(base / "silver" / "usgs" / "daily" / "parameter_code=00060")
    )

    meteo = frame.select("station_id", "ingest_date", "time_utc", "time_local", "date_local")
    for name, factor in (
        ("precipitation", 0.2),
        ("rain", 0.15),
        ("temperature_2m", 15.0),
        ("relative_humidity_2m", 70.0),
        ("dew_point_2m", 8.0),
        ("soil_moisture_0_to_7cm", 0.2),
        ("soil_moisture_7_to_28cm", 0.25),
        ("soil_moisture_28_to_100cm", 0.3),
        ("et0_fao_evapotranspiration", 0.1),
        ("wind_speed_10m", 8.0),
        ("cloud_cover", 40.0),
        ("shortwave_radiation", 120.0),
        ("surface_pressure", 1010.0),
    ):
        meteo = meteo.withColumn(name, F.lit(factor))

    meteo.write.mode("overwrite").partitionBy("station_id", "ingest_date").parquet(
        str(base / "silver" / "open-meteo")
    )


def main() -> int:
    os.environ.pop("S3_ENDPOINT_URL", None)
    base = Path(tempfile.mkdtemp(prefix="gold-smoke-"))
    os.environ["BRONZE_LOCAL_DIR"] = str(base)

    from silver.spark import build_spark_session

    from gold.config import GoldSettings
    from gold.pipeline import run_gold

    settings = GoldSettings.from_env()
    spark = build_spark_session(settings)
    try:
        _write_silver(spark, base)
    finally:
        spark.stop()

    result = run_gold("2024-01-08", "2024-01-10", GoldSettings.from_env())

    rows = result["rows"]
    print("\nsmoke result:", result)
    print("output dir:", base)

    expected = 3 * 24 * 3
    if rows["features"] != expected:
        print(f"expected {expected} feature rows, got {rows['features']}")
        return 1
    if rows["trainable"] == 0:
        print("expected some trainable rows, got none")
        return 1
    print("smoke OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
