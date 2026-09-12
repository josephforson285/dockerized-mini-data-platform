from __future__ import annotations

import pandas as pd
import pytest

from mini_platform.schema import CLEAN_COLUMNS, RAW_COLUMNS, RejectReason, SchemaError
from mini_platform.transforms import clean, normalise

BASE = {
    "order_id": "A1",
    "order_ts": "2026-01-01T10:00:00+00:00",
    "customer_id": "C1",
    "product_id": "P1",
    "product_category": "Electronics",
    "quantity": "2",
    "unit_price": "10.50",
    "currency": "usd",
    "country": "gh",
    "payment_method": "card",
}


def frame(*overrides: dict[str, str]) -> pd.DataFrame:
    rows = [BASE | o for o in (overrides or ({},))]
    return pd.DataFrame(rows, columns=list(RAW_COLUMNS)).astype("string")


def test_normalise_trims_and_normalises_case():
    out = normalise(frame({"country": "  gh ", "currency": " usd", "product_category": "HOME"}))
    assert out.loc[0, "country"] == "GH"
    assert out.loc[0, "currency"] == "USD"
    assert out.loc[0, "product_category"] == "home"


def test_missing_column_is_a_schema_error_not_a_reject():
    df = frame().drop(columns=["unit_price"])
    with pytest.raises(SchemaError, match="unit_price"):
        normalise(df)


def test_empty_string_counts_as_null():
    out = normalise(frame({"customer_id": "   "}))
    assert pd.isna(out.loc[0, "customer_id"])


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"customer_id": ""}, RejectReason.MISSING_REQUIRED),
        ({"order_id": ""}, RejectReason.MISSING_REQUIRED),
        ({"order_ts": "not-a-date"}, RejectReason.BAD_TIMESTAMP),
        ({"quantity": "0"}, RejectReason.BAD_QUANTITY),
        ({"quantity": "-3"}, RejectReason.BAD_QUANTITY),
        ({"unit_price": "0"}, RejectReason.BAD_PRICE),
        ({"unit_price": "-1.5"}, RejectReason.BAD_PRICE),
        ({"currency": "XXX"}, RejectReason.UNKNOWN_CURRENCY),
    ],
)
def test_each_rule_rejects_with_its_own_reason(override, reason):
    good, bad = clean(frame(override), batch_id="b1")
    assert len(good) == 0
    assert bad.loc[0, "reject_reason"] == reason


def test_valid_row_survives():
    good, bad = clean(frame(), batch_id="b1")
    assert len(good) == 1
    assert len(bad) == 0


def test_duplicates_are_quarantined_not_silently_dropped():
    good, bad = clean(frame({}, {}), batch_id="b1")
    assert len(good) == 1
    assert bad["reject_reason"].tolist() == [RejectReason.DUPLICATE]


def test_revenue_is_quantity_times_price():
    good, _ = clean(frame({"quantity": "3", "unit_price": "9.99"}), batch_id="b1")
    assert good.loc[0, "revenue"] == pytest.approx(29.97)


def test_clean_output_has_exactly_the_contract_columns():
    good, _ = clean(frame(), batch_id="b1")
    assert list(good.columns) == list(CLEAN_COLUMNS)


def test_batch_id_is_stamped_on_both_outputs():
    good, bad = clean(frame({}, {"quantity": "-1"}), batch_id="batch-xyz")
    assert set(good["batch_id"]) == {"batch-xyz"}
    assert set(bad["batch_id"]) == {"batch-xyz"}


def test_rows_are_ordered_by_timestamp():
    df = frame(
        {"order_id": "A", "order_ts": "2026-03-01T00:00:00+00:00"},
        {"order_id": "B", "order_ts": "2026-01-01T00:00:00+00:00"},
    )
    good, _ = clean(df, batch_id="b1")
    assert good["order_id"].tolist() == ["B", "A"]


def test_one_row_yields_one_reason_even_with_several_faults():
    good, bad = clean(
        frame({"quantity": "-1", "unit_price": "-1", "currency": "XXX"}), batch_id="b"
    )
    assert len(good) == 0
    assert len(bad) == 1
