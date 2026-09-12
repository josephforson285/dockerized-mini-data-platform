"""Data contract. Imported by the transforms, the tests and the DAG, so there
is exactly one definition of what a valid row looks like."""

from __future__ import annotations

RAW_COLUMNS: tuple[str, ...] = (
    "order_id",
    "order_ts",
    "customer_id",
    "product_id",
    "product_category",
    "quantity",
    "unit_price",
    "currency",
    "country",
    "payment_method",
)

# Null in any of these makes the row unusable.
REQUIRED_COLUMNS: tuple[str, ...] = (
    "order_id",
    "order_ts",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
)

DERIVED_COLUMNS: tuple[str, ...] = ("revenue", "batch_id", "ingested_at")

CLEAN_COLUMNS: tuple[str, ...] = RAW_COLUMNS + DERIVED_COLUMNS

CURRENCIES: tuple[str, ...] = ("USD", "EUR", "GBP", "GHS")


class RejectReason:
    MISSING_REQUIRED = "missing_required_field"
    BAD_TIMESTAMP = "unparseable_timestamp"
    BAD_QUANTITY = "quantity_not_positive"
    BAD_PRICE = "price_not_positive"
    UNKNOWN_CURRENCY = "unknown_currency"
    DUPLICATE = "duplicate_order_id"


class SchemaError(ValueError):
    """Raised when the frame is structurally wrong — a pipeline bug or a
    changed upstream contract, not a bad row."""


def assert_raw_schema(columns: list[str]) -> None:
    missing = [c for c in RAW_COLUMNS if c not in columns]
    if missing:
        raise SchemaError(f"missing columns: {', '.join(missing)}")
