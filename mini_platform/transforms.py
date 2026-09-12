"""Pure transforms: DataFrame in, DataFrames out. No Airflow, no I/O, no env —
so the whole cleaning contract is unit-testable without a running stack."""

from __future__ import annotations

import datetime as dt

import pandas as pd

from mini_platform.schema import (
    CLEAN_COLUMNS,
    CURRENCIES,
    RAW_COLUMNS,
    REQUIRED_COLUMNS,
    RejectReason,
    assert_raw_schema,
)

_TEXT_COLUMNS = (
    "order_id",
    "customer_id",
    "product_id",
    "product_category",
    "currency",
    "country",
    "payment_method",
)


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Tidy column names and text values. Does not drop anything."""
    out = df.rename(columns=lambda c: str(c).strip().lower())
    assert_raw_schema(list(out.columns))
    out = out.loc[:, list(RAW_COLUMNS)].copy()

    for col in _TEXT_COLUMNS:
        out[col] = out[col].astype("string").str.strip()
        # Empty strings are nulls; upstream CSVs express them both ways.
        out[col] = out[col].replace("", pd.NA)

    out["currency"] = out["currency"].str.upper()
    out["country"] = out["country"].str.upper()
    out["product_category"] = out["product_category"].str.lower()
    return out


def _coerce(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["order_ts"] = pd.to_datetime(out["order_ts"], errors="coerce", format="mixed", utc=True)
    out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce")
    out["unit_price"] = pd.to_numeric(out["unit_price"], errors="coerce")
    return out


def _reject_reason(df: pd.DataFrame) -> pd.Series:
    """First failing rule per row; empty string means the row is good."""
    reason = pd.Series("", index=df.index, dtype="string")

    def mark(mask: pd.Series, label: str) -> None:
        reason.loc[(reason == "") & mask.fillna(True)] = label

    required = [c for c in REQUIRED_COLUMNS if c != "order_ts"]
    mark(df[required].isna().any(axis=1), RejectReason.MISSING_REQUIRED)
    mark(df["order_ts"].isna(), RejectReason.BAD_TIMESTAMP)
    mark(df["quantity"] <= 0, RejectReason.BAD_QUANTITY)
    mark(df["unit_price"] <= 0, RejectReason.BAD_PRICE)
    mark(~df["currency"].isin(CURRENCIES), RejectReason.UNKNOWN_CURRENCY)
    return reason


def clean(
    df: pd.DataFrame,
    *,
    batch_id: str,
    ingested_at: dt.datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a raw frame into (clean, rejects).

    Rejects are returned rather than dropped so bad rows stay auditable.
    """
    ingested_at = ingested_at or dt.datetime.now(dt.UTC)

    frame = _coerce(normalise(df))
    reason = _reject_reason(frame)

    rejects = frame.loc[reason != ""].copy()
    rejects["reject_reason"] = reason.loc[reason != ""]

    good = frame.loc[reason == ""].copy()

    # Retries and replays re-deliver the same file; keep the first occurrence.
    dupes = good.duplicated(subset="order_id", keep="first")
    if dupes.any():
        dropped = good.loc[dupes].copy()
        dropped["reject_reason"] = RejectReason.DUPLICATE
        rejects = pd.concat([rejects, dropped], ignore_index=False)
        good = good.loc[~dupes].copy()

    good["quantity"] = good["quantity"].astype("int64")
    good["unit_price"] = good["unit_price"].astype("float64")
    good["revenue"] = (good["quantity"] * good["unit_price"]).round(2)
    good["batch_id"] = batch_id
    good["ingested_at"] = ingested_at

    good = good.loc[:, list(CLEAN_COLUMNS)].sort_values("order_ts").reset_index(drop=True)
    rejects["batch_id"] = batch_id
    return good, rejects.reset_index(drop=True)
