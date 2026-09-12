"""Data contract, sourced from config/pipeline.yml. One definition of a valid
row, shared by the transforms, the tests and the DAG."""

from __future__ import annotations

from mini_platform.settings import get


class RejectReason:
    MISSING_REQUIRED = "missing_required_field"
    BAD_TIMESTAMP = "unparseable_timestamp"
    BAD_QUANTITY = "quantity_below_minimum"
    BAD_PRICE = "price_below_minimum"
    UNKNOWN_CURRENCY = "unknown_currency"
    DUPLICATE = "duplicate_key"


class SchemaError(ValueError):
    """Frame is structurally wrong — a pipeline bug or a changed upstream
    contract, not a bad row."""


def raw_columns() -> tuple[str, ...]:
    return get().contract.raw_columns


def clean_columns() -> tuple[str, ...]:
    return get().contract.clean_columns


def assert_raw_schema(columns: list[str]) -> None:
    missing = [c for c in raw_columns() if c not in columns]
    if missing:
        raise SchemaError(f"missing columns: {', '.join(missing)}")
