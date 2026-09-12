"""Settings resolved from env. Defaults target the host; containers override
the endpoints via env so the same code runs in both places."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def load_env(path: str | Path = ".env") -> None:
    p = Path(path)
    if p.exists():
        load_dotenv(p, override=False)


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    region: str

    @classmethod
    def from_env(cls) -> MinioSettings:
        port = os.environ.get("MINIO_API_PORT", "9002")
        return cls(
            endpoint=os.environ.get("MINIO_ENDPOINT", f"http://127.0.0.1:{port}"),
            access_key=os.environ["MINIO_ROOT_USER"],
            secret_key=os.environ["MINIO_ROOT_PASSWORD"],
            bucket=os.environ.get("MINIO_BUCKET", "raw"),
            region=os.environ.get("MINIO_REGION", "us-east-1"),
        )


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    user: str
    password: str
    dbname: str

    @classmethod
    def from_env(cls) -> PostgresSettings:
        return cls(
            host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
            port=int(os.environ.get("POSTGRES_PORT", "5435")),
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            dbname=os.environ.get("POSTGRES_DB", "analytics"),
        )

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
