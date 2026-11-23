"""Shared Spark session factory.

The session knows nothing about bronze, silver or gold data; it only configures
Spark and object storage. Each layer imports it from here instead of borrowing
another layer's implementation.
"""

from __future__ import annotations

import os
from typing import Protocol


class SparkSettings(Protocol):
    s3_endpoint_url: str | None
    s3_region: str
    access_key: str | None
    secret_key: str | None


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def build_spark_session(settings: SparkSettings):
    """Create (or reuse) the Spark session shared by all layers."""

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("hydrology")
        .master(_env("SPARK_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", _env("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
            "2",
        )
    )

    extra_jars = os.getenv("SPARK_EXTRA_JARS")
    if extra_jars:
        builder = builder.config("spark.jars", extra_jars)

    local_ip = os.getenv("SPARK_LOCAL_IP")
    if local_ip:
        builder = builder.config("spark.driver.host", local_ip)

    if settings.s3_endpoint_url:
        if not settings.access_key or not settings.secret_key:
            raise RuntimeError(
                "GARAGE_DEFAULT_ACCESS_KEY and GARAGE_DEFAULT_SECRET_KEY must be "
                "set to read the S3 data lake"
            )

        secure = settings.s3_endpoint_url.startswith("https://")
        builder = (
            builder.config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint_url)
            .config("spark.hadoop.fs.s3a.endpoint.region", settings.s3_region)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                "true" if secure else "false",
            )
            .config("spark.hadoop.fs.s3a.access.key", settings.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", settings.secret_key)
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
            .config("spark.hadoop.fs.s3a.change.detection.source", "etag")
            .config("spark.hadoop.fs.s3a.committer.name", "directory")
            .config(
                "spark.hadoop.fs.s3a.committer.staging.conflict-mode",
                "replace",
            )
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
        )

    return builder.getOrCreate()
