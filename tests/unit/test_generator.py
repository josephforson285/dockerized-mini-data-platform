from __future__ import annotations

import pandas as pd
import pytest

from data_generator.generate import generate
from mini_platform.transforms import clean


@pytest.mark.parametrize(
    ("rows", "seed", "corrupt", "duplicates"),
    [(200, 1, 20, 10), (500, 7, 30, 20), (1000, 99, 0, 0), (300, 5, 50, 0)],
)
def test_manifest_predicts_the_cleaning_outcome(rows, seed, corrupt, duplicates):
    """The manifest is the E2E test's source of truth, so it must be exact."""
    df, manifest = generate(rows, seed, corrupt, duplicates)
    good, bad = clean(df, batch_id="b1")

    assert manifest["rows_written"] == len(df) == rows + duplicates
    assert manifest["expected_clean"] == len(good)
    assert manifest["expected_rejects"] == len(bad)


def test_same_seed_gives_identical_output():
    a, _ = generate(100, 42, 10, 5)
    b, _ = generate(100, 42, 10, 5)
    pd.testing.assert_frame_equal(a, b)


def test_different_seeds_differ():
    a, _ = generate(100, 1, 0, 0)
    b, _ = generate(100, 2, 0, 0)
    assert not a.equals(b)


def test_corrupt_plus_duplicates_cannot_exceed_rows():
    with pytest.raises(ValueError, match="exceed"):
        generate(rows=10, seed=1, corrupt=8, duplicates=5)


def test_every_corruption_kind_is_exercised():
    _, manifest = generate(600, 3, 60, 0)
    assert len(manifest["corruption_breakdown"]) == 6
