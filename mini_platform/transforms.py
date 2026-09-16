"""Pure transforms: DataFrame in, DataFrames out. No Airflow, no I/O, so the
cleaning contract is testable without a stack. Thresholds come from config."""

from __future__ import annotations

import datetime as dt

import pandas as pd

from mini_platform.schema import RejectReason, assert_raw_schema
from mini_platform.settings import PipelineConfig, get


class QualityGateFailed(RuntimeError):
    """Too much of the batch was unusable to treat as ordinary quarantine."""


def assert_quality(
    clean: pd.DataFrame, rejects: pd.DataFrame, cfg: PipelineConfig | None = None
) -> float:
    """Raise above the configured reject ratio: a mostly-bad batch is an
    upstream incident, not rows to quarantine."""
    cfg = cfg or get()
    total = len(clean) + len(rejects)
    if total == 0:
        return 0.0

    ratio = len(rejects) / total
    if ratio > cfg.rules.max_reject_ratio:
        raise QualityGateFailed(
            f"{len(rejects)}/{total} rows rejected ({ratio:.1%}); "
            f"limit is {cfg.rules.max_reject_ratio:.1%}"
        )
    return ratio


def normalise(df: pd.DataFrame, cfg: PipelineConfig | None = None) -> pd.DataFrame:
    """Tidy column names and text values. Does not drop anything."""
    cfg = cfg or get()
    out = df.rename(columns=lambda c: str(c).strip().lower())
    assert_raw_schema(list(out.columns))
    out = out.loc[:, list(cfg.contract.raw_columns)].copy()

    for col in cfg.contract.text_columns:
        out[col] = out[col].astype("string").str.strip()
        # Upstream CSVs express null both ways.
        out[col] = out[col].replace("", pd.NA)

    for col in cfg.rules.uppercase_columns:
        out[col] = out[col].str.upper()
    for col in cfg.rules.lowercase_columns:
        out[col] = out[col].str.lower()
    return out


def _coerce(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["order_ts"] = pd.to_datetime(out["order_ts"], errors="coerce", format="mixed", utc=True)
    out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce")
    out["unit_price"] = pd.to_numeric(out["unit_price"], errors="coerce")
    return out


def _reject_reason(df: pd.DataFrame, cfg: PipelineConfig) -> pd.Series:
    """First failing rule per row; empty string means the row is good."""
    reason = pd.Series("", index=df.index, dtype="string")

    def mark(mask: pd.Series, label: str) -> None:
        reason.loc[(reason == "") & mask.fillna(True)] = label

    required = [c for c in cfg.contract.required_columns if c != "order_ts"]
    mark(df[required].isna().any(axis=1), RejectReason.MISSING_REQUIRED)
    mark(df["order_ts"].isna(), RejectReason.BAD_TIMESTAMP)
    mark(df["quantity"] < cfg.rules.min_quantity, RejectReason.BAD_QUANTITY)
    mark(df["unit_price"] < cfg.rules.min_unit_price, RejectReason.BAD_PRICE)
    mark(~df["currency"].isin(cfg.rules.allowed_currencies), RejectReason.UNKNOWN_CURRENCY)
    return reason


def clean(
    df: pd.DataFrame,
    *,
    batch_id: str,
    ingested_at: dt.datetime | None = None,
    cfg: PipelineConfig | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split into (clean, rejects); rejects are kept auditable, not dropped."""
    cfg = cfg or get()
    ingested_at = ingested_at or dt.datetime.now(dt.UTC)

    frame = _coerce(normalise(df, cfg))
    reason = _reject_reason(frame, cfg)

    rejects = frame.loc[reason != ""].copy()
    rejects["reject_reason"] = reason.loc[reason != ""]

    good = frame.loc[reason == ""].copy()

    # Retries and replays re-deliver the same file; keep the first occurrence.
    dupes = good.duplicated(subset=cfg.rules.dedupe_key, keep="first")
    if dupes.any():
        dropped = good.loc[dupes].copy()
        dropped["reject_reason"] = RejectReason.DUPLICATE
        rejects = pd.concat([rejects, dropped], ignore_index=False)
        good = good.loc[~dupes].copy()

    good["quantity"] = good["quantity"].astype("int64")
    good["unit_price"] = good["unit_price"].astype("float64")
    good["revenue"] = (good["quantity"] * good["unit_price"]).round(cfg.rules.revenue_precision)
    good["batch_id"] = batch_id
    good["ingested_at"] = ingested_at

    good = good.loc[:, list(cfg.contract.clean_columns)]
    good = good.sort_values("order_ts").reset_index(drop=True)
    rejects["batch_id"] = batch_id
    return good, rejects.reset_index(drop=True)
