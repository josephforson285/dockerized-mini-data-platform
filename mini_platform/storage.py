"""MinIO access. Thin wrapper over boto3 so callers never build clients."""

from __future__ import annotations

from pathlib import Path

import boto3
import pandas as pd

from mini_platform.config import MinioSettings


def client(settings: MinioSettings | None = None):
    s = settings or MinioSettings.from_env()
    return boto3.client(
        "s3",
        endpoint_url=s.endpoint,
        aws_access_key_id=s.access_key,
        aws_secret_access_key=s.secret_key,
        region_name=s.region,
    )


def upload(local_path: str | Path, key: str, settings: MinioSettings | None = None) -> str:
    s = settings or MinioSettings.from_env()
    client(s).upload_file(str(local_path), s.bucket, key)
    return f"s3://{s.bucket}/{key}"


def list_keys(prefix: str = "", settings: MinioSettings | None = None) -> list[str]:
    s = settings or MinioSettings.from_env()
    pages = client(s).get_paginator("list_objects_v2").paginate(Bucket=s.bucket, Prefix=prefix)
    return [obj["Key"] for page in pages for obj in page.get("Contents", [])]


def read_csv(key: str, settings: MinioSettings | None = None) -> pd.DataFrame:
    s = settings or MinioSettings.from_env()
    body = client(s).get_object(Bucket=s.bucket, Key=key)["Body"]
    # Everything is read as text; typing is the transform layer's job.
    return pd.read_csv(body, dtype="string")
