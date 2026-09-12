from __future__ import annotations

import textwrap

import pytest
import yaml

from data_generator.generate import generate
from mini_platform.schema import RejectReason
from mini_platform.settings import DEFAULT_CONFIG, ConfigError, load
from mini_platform.transforms import clean

VALID = yaml.safe_load(DEFAULT_CONFIG.read_text())


def write(tmp_path, mutate=None):
    doc = yaml.safe_load(yaml.safe_dump(VALID))
    if mutate:
        mutate(doc)
    p = tmp_path / "pipeline.yml"
    p.write_text(yaml.safe_dump(doc))
    return p


def test_shipped_config_loads():
    cfg = load(DEFAULT_CONFIG)
    assert cfg.contract.raw_columns
    assert cfg.rules.allowed_currencies


def test_missing_file_is_a_clear_error(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load(tmp_path / "nope.yml")


def test_missing_section_names_the_section(tmp_path):
    p = tmp_path / "bad.yml"
    p.write_text(textwrap.dedent("contract: {}\n"))
    with pytest.raises(ConfigError, match="rules"):
        load(p)


def test_required_column_outside_raw_columns_is_rejected(tmp_path):
    p = write(tmp_path, lambda d: d["contract"]["required_columns"].append("nonexistent"))
    with pytest.raises(ConfigError, match="required_columns"):
        load(p)


def test_dedupe_key_must_be_a_real_column(tmp_path):
    p = write(tmp_path, lambda d: d["rules"].update(dedupe_key="not_a_column"))
    with pytest.raises(ConfigError, match="dedupe_key"):
        load(p)


def test_empty_currency_list_is_rejected(tmp_path):
    p = write(tmp_path, lambda d: d["rules"].update(allowed_currencies=[]))
    with pytest.raises(ConfigError, match="allowed_currencies"):
        load(p)


def test_derived_column_cannot_shadow_a_raw_column(tmp_path):
    p = write(tmp_path, lambda d: d["contract"]["derived_columns"].append("quantity"))
    with pytest.raises(ConfigError, match="overlap"):
        load(p)


def test_generator_and_validator_cannot_drift_on_currency():
    """Regression: currencies were once declared in two modules."""
    cfg = load(DEFAULT_CONFIG)
    df, _ = generate(300, seed=11, corrupt=0, duplicates=0, cfg=cfg)
    assert set(df["currency"]) <= set(cfg.rules.allowed_currencies)


def test_adding_a_currency_to_config_changes_what_is_accepted(tmp_path):
    p = write(tmp_path, lambda d: d["rules"]["allowed_currencies"].append("XXX"))
    cfg = load(p)
    df, _ = generate(50, seed=3, corrupt=6, duplicates=0, cfg=cfg)
    good, bad = clean(df, batch_id="b1", cfg=cfg)
    # The currency corruption is no longer a violation, so one fewer reject kind.
    assert "unknown_currency" not in set(bad["reject_reason"])


def test_raising_min_quantity_rejects_previously_valid_rows(tmp_path):
    """Same data, stricter threshold — the rule must come from config alone."""
    lenient = load(DEFAULT_CONFIG)
    df, _ = generate(200, seed=5, corrupt=0, duplicates=0, cfg=lenient)

    good_before, bad_before = clean(df, batch_id="b", cfg=lenient)
    assert len(good_before) == 200
    assert bad_before.empty

    strict = load(write(tmp_path, lambda d: d["rules"].update(min_quantity=5)))
    good_after, bad_after = clean(df, batch_id="b", cfg=strict)

    assert len(good_after) < len(good_before)
    assert len(good_after) + len(bad_after) == 200
    assert set(bad_after["reject_reason"]) == {RejectReason.BAD_QUANTITY}
