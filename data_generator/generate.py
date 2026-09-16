"""Synthetic sales batches, deterministic per seed. Rows are corrupted on
purpose; the manifest says how many should survive."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import random
from pathlib import Path

import pandas as pd
from faker import Faker

from mini_platform.config import load_env
from mini_platform.settings import PipelineConfig, get

# Each corruption must trip exactly one rule in transforms._reject_reason.
CORRUPTIONS = (
    ("customer_id", lambda rnd, cfg: ""),
    ("order_id", lambda rnd, cfg: ""),
    ("order_ts", lambda rnd, cfg: "not-a-date"),
    ("quantity", lambda rnd, cfg: str(cfg.rules.min_quantity - rnd.randint(1, 5))),
    ("unit_price", lambda rnd, cfg: "0"),
    ("currency", lambda rnd, cfg: "XXX"),
)


def _row(fake: Faker, rnd: random.Random, ts: dt.datetime, cfg: PipelineConfig) -> dict[str, str]:
    g = cfg.generator
    return {
        "order_id": fake.uuid4(),
        "order_ts": ts.isoformat(),
        "customer_id": f"C{rnd.randint(1000, 9999)}",
        "product_id": f"P{rnd.randint(100, 999)}",
        "product_category": rnd.choice(g.categories),
        "quantity": str(rnd.randint(cfg.rules.min_quantity, cfg.rules.min_quantity + 7)),
        "unit_price": f"{rnd.uniform(2.5, 450.0):.2f}",
        "currency": rnd.choice(cfg.rules.allowed_currencies),
        "country": rnd.choice(g.countries),
        "payment_method": rnd.choice(g.payment_methods),
    }


def generate(
    rows: int,
    seed: int,
    corrupt: int,
    duplicates: int,
    cfg: PipelineConfig | None = None,
) -> tuple[pd.DataFrame, dict]:
    cfg = cfg or get()
    if corrupt + duplicates > rows:
        raise ValueError("corrupt + duplicates must not exceed rows")

    fake = Faker()
    Faker.seed(seed)
    rnd = random.Random(seed)

    start = dt.datetime.fromisoformat(cfg.generator.start_date).replace(tzinfo=dt.UTC)
    step = dt.timedelta(minutes=cfg.generator.interval_minutes)
    records = [_row(fake, rnd, start + step * i, cfg) for i in range(rows)]

    # Corrupt the head, duplicate from the tail: no overlap, exact counts.
    breakdown: dict[str, int] = {}
    for i in range(corrupt):
        field, make = CORRUPTIONS[i % len(CORRUPTIONS)]
        records[i][field] = make(rnd, cfg)
        breakdown[field] = breakdown.get(field, 0) + 1

    copies = [dict(records[corrupt + i]) for i in range(duplicates)]
    all_records = records + copies
    rnd.shuffle(all_records)

    df = pd.DataFrame(all_records, columns=list(cfg.contract.raw_columns)).astype("string")
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
    load_env()
    cfg = get()
    g = cfg.generator

    p = argparse.ArgumentParser(description="Generate a synthetic sales batch")
    p.add_argument("--rows", type=int, default=g.rows)
    p.add_argument("--seed", type=int, default=g.seed)
    p.add_argument("--corrupt", type=int, default=g.corrupt)
    p.add_argument("--duplicates", type=int, default=g.duplicates)
    p.add_argument("--batch-id", default=None)
    p.add_argument("--outdir", default="out/batches")
    p.add_argument("--upload", action="store_true", help="upload to MinIO")
    args = p.parse_args()

    batch_id = args.batch_id or dt.datetime.now(dt.UTC).strftime("batch_%Y%m%dT%H%M%SZ")
    df, manifest = generate(args.rows, args.seed, args.corrupt, args.duplicates, cfg)
    manifest["batch_id"] = batch_id

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    csv_path = outdir / f"{batch_id}.csv"
    df.to_csv(csv_path, index=False)
    (outdir / f"{batch_id}.manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"wrote {csv_path} ({manifest['rows_written']} rows)")

    if args.upload:
        from mini_platform import storage

        uri = storage.upload(csv_path, f"{cfg.raw_prefix}{batch_id}.csv")
        print(f"uploaded {uri}")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
