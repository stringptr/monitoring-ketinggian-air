"""Spark session factory for the silver layer.

The same code runs three ways:

* against Garage through the S3A connector (the default in Docker Compose),
* against a local directory when ``S3_ENDPOINT_URL`` is unset,
* against Azure Data Lake Storage later, by replacing the filesystem prefix
  and the credential block.

The S3A connector jars (``hadoop-aws`` and the AWS SDK v2 bundle) are baked
into the worker image by ``config/prefect/Dockerfile`` so the container never
has to download Maven artifacts at runtime.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from silver.config import SilverSettings


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def build_spark_session(settings: SilverSettings) -> SparkSession:
    """Create (or reuse) the Spark session used by the silver flows."""

    builder = (
        SparkSession.builder.appName("hydrology-silver")
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
        # Local mode still starts Python workers that connect back to the
        # driver. Pinning the driver host avoids containers/Windows resolving
        # a hostname the worker cannot reach.
        builder = builder.config("spark.driver.host", local_ip)

    if settings.s3_endpoint_url:
        if not settings.access_key or not settings.secret_key:
            raise RuntimeError(
                "GARAGE_DEFAULT_ACCESS_KEY and GARAGE_DEFAULT_SECRET_KEY must be "
                "set to read bronze from the S3 data lake"
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
            # Garage is not AWS; disable S3 change detection and use the
            # directory committer, which is the safest option for small
            # partitioned writes on non-AWS object stores.
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
