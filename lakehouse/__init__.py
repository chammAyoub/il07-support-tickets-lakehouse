"""IL-07 Lakehouse layer: MinIO S3 storage, Apache Hudi tables, and maintenance."""

from typing import Any


def __getattr__(name: str) -> Any:
    """Lazily expose Spark/Hudi helpers so boto3-only scripts do not require PySpark."""
    if name in {"LAKEHOUSE_PATHS", "get_lakehouse_paths", "get_spark_session"}:
        from lakehouse import spark_hudi_session

        return getattr(spark_hudi_session, name)
    raise AttributeError(f"module 'lakehouse' has no attribute {name!r}")


__all__ = [
    "LAKEHOUSE_PATHS",
    "get_lakehouse_paths",
    "get_spark_session",
]
