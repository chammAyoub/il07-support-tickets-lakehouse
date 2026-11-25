"""Initialize MinIO bucket and Medallion zone prefixes for Project IL-07."""

from __future__ import annotations

import argparse
import os
import sys

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

ZONES = ("bronze", "silver", "gold", "models")


def get_minio_client() -> boto3.client:
    """Create a boto3 S3 client configured for the MinIO endpoint."""
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ROOT_USER", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(client: boto3.client, bucket_name: str) -> None:
    """Create an S3/MinIO bucket if it does not already exist."""
    try:
        client.head_bucket(Bucket=bucket_name)
        print(f"[MinIO] Bucket '{bucket_name}' already exists.")
    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", ""))
        if error_code in {"404", "NoSuchBucket", "NotFound"}:
            client.create_bucket(Bucket=bucket_name)
            print(f"[MinIO] Created bucket '{bucket_name}'.")
        else:
            raise


def init_lakehouse_storage(also_zone_buckets: bool = False) -> None:
    """Ensure the canonical 'lakehouse' bucket and its Medallion prefixes exist."""
    bucket = os.getenv("MINIO_BUCKET", "lakehouse")
    client = get_minio_client()

    ensure_bucket(client, bucket)
    for zone in ZONES:
        key = f"{zone}/.keep"
        client.put_object(Bucket=bucket, Key=key, Body=b"")
        print(f"[MinIO] Initialized prefix s3a://{bucket}/{zone}/")

    if also_zone_buckets:
        for zone in ZONES:
            ensure_bucket(client, zone)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Initialize IL-07 MinIO lakehouse buckets.")
    parser.add_argument(
        "--also-zone-buckets",
        action="store_true",
        help="Also create standalone bronze/silver/gold/models buckets in addition to lakehouse/.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    try:
        init_lakehouse_storage(also_zone_buckets=args.also_zone_buckets)
    except Exception as exc:
        print(f"[MinIO] Initialization failed: {exc}", file=sys.stderr)
        sys.exit(1)
