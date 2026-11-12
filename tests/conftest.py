from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from lakewatch.config import load_config
from lakewatch.planner.policy import Policy

REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_LAKE = REPO_ROOT / "lakewatch" / "fixtures" / "lake"
DEFAULT_CONFIG = REPO_ROOT / "configs" / "default.yaml"


@pytest.fixture(scope="session")
def default_config():
    return load_config(DEFAULT_CONFIG)


@pytest.fixture
def policy(default_config) -> Policy:
    return default_config.policy


@pytest.fixture
def lake(tmp_path, default_config) -> Path:
    target = tmp_path / "lake"
    shutil.copytree(default_config.lake_root, target)
    return target


@pytest.fixture
def app_config(tmp_path, lake, policy):
    return default_config_for(tmp_path, lake, policy)


def default_config_for(tmp_path: Path, lake: Path, policy: Policy):
    loaded = load_config(DEFAULT_CONFIG)
    return replace(
        loaded,
        source=tmp_path / "config.yaml",
        lake_root=lake,
        ledger_path=tmp_path / ".lakewatch" / "ledger.sqlite3",
        reports_dir=tmp_path / "reports",
        policy=policy,
    )


def policy_with(policy: Policy, **changes) -> Policy:
    return replace(policy, **changes)


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_parquet(path: Path, rows: int = 4, seed: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "id": list(range(seed * 100, seed * 100 + rows)),
            "label": [f"row-{seed}-{index}" for index in range(rows)],
            "amount": [float(index + seed) for index in range(rows)],
        }
    )
    pq.write_table(table, path, compression="snappy")


def write_manifest(table_dir: Path, files: list[dict] | None = None,
                   generation: int = 1, table: str | None = None) -> None:
    name = table or table_dir.name
    write_json(
        table_dir / "_metadata" / "manifest.json",
        {
            "format": "lakewatch.manifest/1",
            "table": name,
            "generation": generation,
            "updated_at": "2025-03-01T00:00:00Z",
            "files": files or [],
        },
    )


def write_snapshot(table_dir: Path, sequence: int, created_at: str,
                   files: list[dict] | None = None, table: str | None = None) -> None:
    name = table or table_dir.name
    write_json(
        table_dir / "_metadata" / "snapshots" / f"snap-{sequence:04d}.json",
        {
            "format": "lakewatch.snapshot/1",
            "table": name,
            "snapshot_id": f"snap-{sequence:04d}",
            "sequence": sequence,
            "created_at": created_at,
            "files": files or [],
        },
    )


def descriptor(table_dir: Path, rel: str, rows: int = 4) -> dict:
    path = table_dir / rel
    return {
        "path": rel,
        "bytes": path.stat().st_size,
        "rows": rows,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def tree_signature(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def write_cli_config(path: Path, lake: Path, policy: Policy, ledger: Path,
                     reports: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "lake_root": str(lake),
        "ledger_path": str(ledger),
        "reports_dir": str(reports),
        "host": "127.0.0.1",
        "port": 7311,
        "policy": policy.to_dict(),
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path
