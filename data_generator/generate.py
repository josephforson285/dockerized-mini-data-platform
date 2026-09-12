"""Synthetic sales batches.

Deterministic for a given seed, and every corrupted row is corrupted on
purpose. The manifest records how many rows *should* survive cleaning, so the
end-to-end test asserts against known numbers instead of against the
transform's own output.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import random
from pathlib import Path

import pandas as pd
from faker import Faker

from mini_platform.config import load_env
from mini_platform.schema import RAW_COLUMNS

CATEGORIES = ("electronics", "grocery", "apparel", "home", "toys")
CURRENCIES = ("USD", "EUR", "GBP", "GHS")
COUNTRIES = ("GH", "NG", "KE", "ZA", "US", "GB")
PAYMENTS = ("card", "mobile_money", "bank_transfer", "cash")

# Each corruption must trip exactly one rule in transforms._reject_reason.
CORRUPTIONS = (
    ("customer_id", lambda rnd: ""),
    ("order_id", lambda rnd: ""),
    ("order_ts", lambda rnd: "not-a-date"),
    ("quantity", lambda rnd: str(-rnd.randint(1, 5))),
    ("unit_price", lambda rnd: "0"),
    ("currency", lambda rnd: "XXX"),
)


def _row(fake: Faker, rnd: random.Random, ts: dt.datetime) -> dict[str, str]:
    return {
        "order_id": fake.uuid4(),
        "order_ts": ts.isoformat(),
        "customer_id": f"C{rnd.randint(1000, 9999)}",
        "product_id": f"P{rnd.randint(100, 999)}",
        "product_category": rnd.choice(CATEGORIES),
        "quantity": str(rnd.randint(1, 8)),
        "unit_price": f"{rnd.uniform(2.5, 450.0):.2f}",
        "currency": rnd.choice(CURRENCIES),
        "country": rnd.choice(COUNTRIES),
        "payment_method": rnd.choice(PAYMENTS),
    }


def generate(rows: int, seed: int, corrupt: int, duplicates: int) -> tuple[pd.DataFrame, dict]:
    if corrupt + duplicates > rows:
        raise ValueError("corrupt + duplicates must not exceed rows")

    fake = Faker()
    Faker.seed(seed)
    rnd = random.Random(seed)

    start = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    records = [_row(fake, rnd, start + dt.timedelta(minutes=3 * i)) for i in range(rows)]

    # Corrupt the first block, duplicate from the untouched tail, so the two
    # never overlap and the expected counts stay exact.
    breakdown: dict[str, int] = {}
    for i in range(corrupt):
        field, make = CORRUPTIONS[i % len(CORRUPTIONS)]
        records[i][field] = make(rnd)
        breakdown[field] = breakdown.get(field, 0) + 1

    copies = [dict(records[corrupt + i]) for i in range(duplicates)]
    all_records = records + copies
    rnd.shuffle(all_records)

    df = pd.DataFrame(all_records, columns=list(RAW_COLUMNS)).astype("string")
    manifest = {
        "seed": seed,
        "rows_written": len(all_records),
        "expected_clean": rows - corrupt,
        "expected_rejects": corrupt + duplicates,
        "corrupted": corrupt,
        "duplicates": duplicates,
        "corruption_breakdown": breakdown,
    }
    return df, manifest


def main() -> None:
    p = argparse.ArgumentParser(description="Generate a synthetic sales batch")
    p.add_argument("--rows", type=int, default=2000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--corrupt", type=int, default=60)
    p.add_argument("--duplicates", type=int, default=40)
    p.add_argument("--batch-id", default=None)
    p.add_argument("--outdir", default="out/batches")
    p.add_argument("--upload", action="store_true", help="upload to MinIO")
    args = p.parse_args()

    load_env()
    batch_id = args.batch_id or dt.datetime.now(dt.UTC).strftime("batch_%Y%m%dT%H%M%SZ")
    df, manifest = generate(args.rows, args.seed, args.corrupt, args.duplicates)
    manifest["batch_id"] = batch_id

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    csv_path = outdir / f"{batch_id}.csv"
    df.to_csv(csv_path, index=False)
    (outdir / f"{batch_id}.manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"wrote {csv_path} ({manifest['rows_written']} rows)")

    if args.upload:
        from mini_platform import storage

        uri = storage.upload(csv_path, f"sales/{batch_id}.csv")
        print(f"uploaded {uri}")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
