"""Shared SparkSession builder configured for Apache Hudi and MinIO (S3A)."""

from __future__ import annotations

import os
from typing import Dict

from pyspark.sql import SparkSession

DEFAULT_HUDI_BUNDLE = "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"
DEFAULT_HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"
DEFAULT_AWS_SDK = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
DEFAULT_SPARK_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def get_lakehouse_paths(bucket: str | None = None) -> Dict[str, str]:
    """Return canonical S3A URIs for Bronze, Silver, Gold, and MLflow/MLlib models."""
    resolved_bucket = bucket or os.getenv("MINIO_BUCKET", "lakehouse")
    base = f"s3a://{resolved_bucket}"
    return {
        "bronze_tickets": f"{base}/bronze/tickets",
        "bronze_updates": f"{base}/bronze/updates",
        "silver_tickets": f"{base}/silver/tickets_hudi",
        "gold_sla_by_category": f"{base}/gold/sla_by_category",
        "gold_daily_volumes": f"{base}/gold/daily_volumes",
        "models_nlp": f"{base}/models/nlp_pipeline",
    }


LAKEHOUSE_PATHS: Dict[str, str] = get_lakehouse_paths()


def _build_packages_list(with_kafka: bool) -> str:
    """Resolve Maven package coordinates for Spark + Hudi + S3A (+ optional Kafka)."""
    custom_packages = os.getenv("SPARK_JARS_PACKAGES")
    if custom_packages:
        return custom_packages

    packages = [
        os.getenv("HUDI_SPARK_BUNDLE", DEFAULT_HUDI_BUNDLE),
        os.getenv("HADOOP_AWS_PACKAGE", DEFAULT_HADOOP_AWS),
        os.getenv("AWS_SDK_PACKAGE", DEFAULT_AWS_SDK),
    ]
    if with_kafka:
        packages.append(os.getenv("SPARK_KAFKA_PACKAGE", DEFAULT_SPARK_KAFKA))
    return ",".join(packages)


def get_spark_session(
    app_name: str = "IL07-Lakehouse",
    master: str | None = None,
    with_kafka: bool = False,
    extra_configs: Dict[str, str] | None = None,
) -> SparkSession:
    """Build or reuse a SparkSession pre-configured with Apache Hudi and MinIO S3A."""
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
    ssl_enabled = "true" if endpoint.lower().startswith("https://") else "false"

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    configs = {
        "spark.jars.packages": _build_packages_list(with_kafka=with_kafka),
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator": "org.apache.spark.HoodieSparkKryoRegistrar",
        "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        "spark.sql.hive.convertMetastoreParquet": "false",
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": ssl_enabled,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
    }

    if extra_configs:
        configs.update(extra_configs)

    for key, value in configs.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


if __name__ == "__main__":
    session = get_spark_session(app_name="IL07-Session-SmokeTest")
    try:
        paths = get_lakehouse_paths()
        print(f"Spark {session.version} initialized successfully.")
        print(f"Silver Hudi target: {paths['silver_tickets']}")
    finally:
        session.stop()
