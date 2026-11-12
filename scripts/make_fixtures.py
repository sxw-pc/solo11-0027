#!/usr/bin/env python3
"""Rebuild the deterministic offline fixture lake from scratch.

Usage:
    python scripts/make_fixtures.py
    python scripts/make_fixtures.py --out /path/to/empty-or-fixture-dir
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = REPO_ROOT / "lakewatch" / "fixtures" / "lake"
MANIFEST_FORMAT = "lakewatch.manifest/1"
SNAPSHOT_FORMAT = "lakewatch.snapshot/1"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _write_parquet(path: Path, rows: int, seed: int, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "id": pa.array(range(seed * 1000, seed * 1000 + rows), pa.int64()),
            "partition_label": [label] * rows,
            "quantity": [((seed * 17 + index * 13) % 97) + 1 for index in range(rows)],
            "amount": [round(((seed * 31 + index * 7) % 1000) / 10.0, 2)
                       for index in range(rows)],
            "note": [f"fixture-{seed:03d}-{index:03d}" for index in range(rows)],
        }
    )
    pq.write_table(table, path, compression="snappy", row_group_size=16)


def _descriptor(root: Path, table: str, rel: str, rows: int, sha: bool = True) -> dict:
    path = root / table / rel
    value = {"path": rel, "bytes": path.stat().st_size, "rows": rows}
    if sha and path.exists():
        value["sha256"] = _sha256(path)
    return value


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _write_manifest(table_dir: Path, table: str, generation: int,
                    updated_at: str, files: list[dict]) -> None:
    _write_json(
        table_dir / "_metadata" / "manifest.json",
        {
            "format": MANIFEST_FORMAT,
            "table": table,
            "generation": generation,
            "updated_at": updated_at,
            "files": sorted(files, key=lambda item: item["path"]),
        },
    )


def _write_snapshot(table_dir: Path, table: str, sequence: int,
                    created_at: str, files: list[dict]) -> None:
    snapshot_id = f"snap-{sequence:04d}"
    _write_json(
        table_dir / "_metadata" / "snapshots" / f"{snapshot_id}.json",
        {
            "format": SNAPSHOT_FORMAT,
            "table": table,
            "snapshot_id": snapshot_id,
            "sequence": sequence,
            "created_at": created_at,
            "files": sorted(files, key=lambda item: item["path"]),
        },
    )


def _events(root: Path) -> None:
    table_dir = root / "events"
    specs = [
        ("region=east/day=2025-01-01", 6),
        ("region=east/day=2025-01-02", 5),
        ("region=west/day=2025-01-03", 7),
    ]
    files = []
    seed = 10
    for partition, count in specs:
        for index in range(count):
            rel = f"{partition}/part-{index:04d}.parquet"
            _write_parquet(table_dir / rel, rows=5 + (index % 3), seed=seed,
                           label=partition)
            files.append(_descriptor(root, "events", rel, rows=5 + (index % 3)))
            seed += 1
    missing_rel = "region=west/day=2025-01-03/part-missing-000.parquet"
    files.append({"path": missing_rel, "bytes": 912, "rows": 4})

    orphan_rel = "region=east/day=2025-01-02/part-orphan-000.parquet"
    _write_parquet(table_dir / orphan_rel, rows=7, seed=90, label="orphan")
    legacy_rel = "region=north/day=2025-01-01/part-legacy-000.parquet"
    _write_parquet(table_dir / legacy_rel, rows=9, seed=91, label="legacy")
    legacy = _descriptor(root, "events", legacy_rel, rows=9)
    bad_rel = "raw_dump/part-bad-000.parquet"
    _write_parquet(table_dir / bad_rel, rows=3, seed=92, label="raw")

    current = files
    s1 = current + [legacy]
    s2 = current + [legacy]
    s3 = list(current)
    s4 = list(current)
    s5 = list(current)
    _write_snapshot(table_dir, "events", 1, "2024-10-01T00:00:00Z", s1)
    _write_snapshot(table_dir, "events", 2, "2024-12-01T00:00:00Z", s2)
    _write_snapshot(table_dir, "events", 3, "2025-01-01T00:00:00Z", s3)
    _write_snapshot(table_dir, "events", 4, "2025-02-01T00:00:00Z", s4)
    _write_snapshot(table_dir, "events", 5, "2025-03-01T00:00:00Z", s5)
    _write_manifest(table_dir, "events", 5, "2025-03-01T00:00:00Z", current)


def _orders(root: Path) -> None:
    table_dir = root / "orders"
    files = []
    seed = 40
    for partition, count in (
        ("region=us", 5), ("region=eu", 4), ("region=skew", 14)
    ):
        for index in range(count):
            rel = f"{partition}/part-{index:04d}.parquet"
            rows = 8 + (index % 4)
            _write_parquet(table_dir / rel, rows=rows, seed=seed, label=partition)
            files.append(_descriptor(root, "orders", rel, rows=rows))
            seed += 1
    for sequence, created_at in (
        (1, "2025-01-15T00:00:00Z"),
        (2, "2025-02-15T00:00:00Z"),
        (3, "2025-03-01T00:00:00Z"),
    ):
        _write_snapshot(table_dir, "orders", sequence, created_at, files)
    _write_manifest(table_dir, "orders", 3, "2025-03-01T00:00:00Z", files)


def _customers(root: Path) -> None:
    table_dir = root / "customers"
    files = []
    for index in range(2):
        rel = f"part-{index:04d}.parquet"
        _write_parquet(table_dir / rel, rows=120, seed=70 + index,
                       label="unpartitioned")
        files.append(_descriptor(root, "customers", rel, rows=120))
    for sequence, created_at in (
        (1, "2025-02-15T00:00:00Z"),
        (2, "2025-03-01T00:00:00Z"),
    ):
        _write_snapshot(table_dir, "customers", sequence, created_at, files)
    _write_manifest(table_dir, "customers", 2, "2025-03-01T00:00:00Z", files)


def _stray(root: Path) -> None:
    rel = "region=us/part-0000.parquet"
    _write_parquet(root / "stray_export" / rel, rows=4, seed=95, label="stray")


def build(out: Path) -> None:
    out = out.resolve()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    _events(out)
    _orders(out)
    _customers(out)
    _stray(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="fixture lake root")
    args = parser.parse_args()
    build(Path(args.out))
    total = sum(path.stat().st_size for path in Path(args.out).rglob("*")
                if path.is_file())
    print(f"built {args.out} ({total} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
