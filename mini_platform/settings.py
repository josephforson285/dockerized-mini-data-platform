"""Loads config/pipeline.yml once. Business rules come from here; hosts,
ports and credentials come from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "config" / "pipeline.yml"


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class Contract:
    raw_columns: tuple[str, ...]
    required_columns: tuple[str, ...]
    derived_columns: tuple[str, ...]
    text_columns: tuple[str, ...]

    @property
    def clean_columns(self) -> tuple[str, ...]:
        return self.raw_columns + self.derived_columns


@dataclass(frozen=True)
class Rules:
    allowed_currencies: tuple[str, ...]
    min_quantity: int
    min_unit_price: float
    revenue_precision: int
    dedupe_key: str
    uppercase_columns: tuple[str, ...]
    lowercase_columns: tuple[str, ...]


@dataclass(frozen=True)
class GeneratorConfig:
    rows: int
    seed: int
    corrupt: int
    duplicates: int
    start_date: str
    interval_minutes: int
    categories: tuple[str, ...]
    countries: tuple[str, ...]
    payment_methods: tuple[str, ...]


@dataclass(frozen=True)
class PipelineConfig:
    contract: Contract
    rules: Rules
    generator: GeneratorConfig
    raw_prefix: str
    fact_table: str
    reject_table: str
    run_table: str
    source: Path = field(compare=False, default=DEFAULT_CONFIG)


def _require(mapping: dict, key: str, where: str):
    if key not in mapping:
        raise ConfigError(f"missing '{key}' under {where}")
    return mapping[key]


def load(path: str | Path | None = None) -> PipelineConfig:
    p = Path(path or os.environ.get("MINI_PLATFORM_CONFIG") or DEFAULT_CONFIG)
    if not p.exists():
        raise ConfigError(f"pipeline config not found: {p}")

    raw = yaml.safe_load(p.read_text()) or {}
    contract = _require(raw, "contract", "root")
    rules = _require(raw, "rules", "root")
    gen = _require(raw, "generator", "root")
    storage = raw.get("storage", {})
    warehouse = raw.get("warehouse", {})

    cfg = PipelineConfig(
        contract=Contract(
            raw_columns=tuple(_require(contract, "raw_columns", "contract")),
            required_columns=tuple(_require(contract, "required_columns", "contract")),
            derived_columns=tuple(_require(contract, "derived_columns", "contract")),
            text_columns=tuple(_require(contract, "text_columns", "contract")),
        ),
        rules=Rules(
            allowed_currencies=tuple(_require(rules, "allowed_currencies", "rules")),
            min_quantity=int(_require(rules, "min_quantity", "rules")),
            min_unit_price=float(_require(rules, "min_unit_price", "rules")),
            revenue_precision=int(rules.get("revenue_precision", 2)),
            dedupe_key=str(rules.get("dedupe_key", "order_id")),
            uppercase_columns=tuple(rules.get("uppercase_columns", ())),
            lowercase_columns=tuple(rules.get("lowercase_columns", ())),
        ),
        generator=GeneratorConfig(
            rows=int(_require(gen, "rows", "generator")),
            seed=int(_require(gen, "seed", "generator")),
            corrupt=int(_require(gen, "corrupt", "generator")),
            duplicates=int(_require(gen, "duplicates", "generator")),
            start_date=str(_require(gen, "start_date", "generator")),
            interval_minutes=int(gen.get("interval_minutes", 3)),
            categories=tuple(_require(gen, "categories", "generator")),
            countries=tuple(_require(gen, "countries", "generator")),
            payment_methods=tuple(_require(gen, "payment_methods", "generator")),
        ),
        raw_prefix=str(storage.get("raw_prefix", "sales/")),
        fact_table=str(warehouse.get("fact_table", "fact_sales")),
        reject_table=str(warehouse.get("reject_table", "rejected_sales")),
        run_table=str(warehouse.get("run_table", "pipeline_runs")),
        source=p,
    )
    _validate(cfg)
    return cfg


def _validate(cfg: PipelineConfig) -> None:
    """Catch a broken config at load time rather than mid-DAG."""
    unknown = set(cfg.contract.required_columns) - set(cfg.contract.raw_columns)
    if unknown:
        raise ConfigError(f"required_columns not in raw_columns: {sorted(unknown)}")

    unknown = set(cfg.contract.text_columns) - set(cfg.contract.raw_columns)
    if unknown:
        raise ConfigError(f"text_columns not in raw_columns: {sorted(unknown)}")

    if cfg.rules.dedupe_key not in cfg.contract.raw_columns:
        raise ConfigError(f"dedupe_key '{cfg.rules.dedupe_key}' is not a raw column")

    if not cfg.rules.allowed_currencies:
        raise ConfigError("allowed_currencies must not be empty")

    overlap = set(cfg.contract.raw_columns) & set(cfg.contract.derived_columns)
    if overlap:
        raise ConfigError(f"derived_columns overlap raw_columns: {sorted(overlap)}")


@lru_cache(maxsize=1)
def get() -> PipelineConfig:
    return load()
