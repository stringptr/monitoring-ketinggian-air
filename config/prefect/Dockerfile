FROM docker.io/prefecthq/prefect:3.9.1-python3.13

RUN uv pip install --no-cache \
    requests \
    s3fs \
    boto3

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl \
        ca-certificates \
        procps \
    && rm -rf /var/lib/apt/lists/*

RUN uv pip install --no-cache \
    "pyspark==4.0.2" \
    "pandas>=2.2,<3" \
    "pyarrow>=17" \
    "pandera>=0.26,<0.37" \
    "tzdata"

ARG HADOOP_AWS_VERSION=3.4.1
ARG AWS_SDK_BUNDLE_VERSION=2.24.6

RUN set -eux; \
    jars="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    curl -fsSL -o "${jars}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSL -o "${jars}/aws-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
        "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_SUBMIT_ARGS="--driver-memory 2g pyspark-shell"

