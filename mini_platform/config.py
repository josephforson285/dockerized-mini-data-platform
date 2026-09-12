"""Environment-specific settings: hosts, ports, credentials.

Values are required rather than defaulted, so a missing variable fails loudly
at startup instead of silently connecting somewhere unintended. Business rules
live in config/pipeline.yml — see mini_platform.settings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from mini_platform.settings import REPO_ROOT


class MissingSetting(RuntimeError):
    pass


def load_env(path: str | Path | None = None) -> None:
    p = Path(path) if path else REPO_ROOT / ".env"
    if p.exists():
        load_dotenv(p, override=False)


def _env(key: str) -> str:
    try:
        return os.environ[key]
    except KeyError:
        raise MissingSetting(
            f"{key} is not set; copy .env.example and run `make secrets`"
        ) from None


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    region: str

    @classmethod
    def from_env(cls) -> MinioSettings:
        # Containers set MINIO_ENDPOINT (http://minio:9000); on the host it is
        # derived from the published port so there is no second literal.
        endpoint = os.environ.get("MINIO_ENDPOINT") or f"http://127.0.0.1:{_env('MINIO_API_PORT')}"
        return cls(
            endpoint=endpoint,
            access_key=_env("MINIO_ROOT_USER"),
            secret_key=_env("MINIO_ROOT_PASSWORD"),
            bucket=_env("MINIO_BUCKET"),
            region=_env("MINIO_REGION"),
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
        # Containers reach Postgres by service name on the internal port.
        in_container = "POSTGRES_HOST" in os.environ
        return cls(
            host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
            port=5432 if in_container else int(_env("POSTGRES_PORT")),
            user=_env("POSTGRES_USER"),
            password=_env("POSTGRES_PASSWORD"),
            dbname=_env("POSTGRES_DB"),
        )

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
