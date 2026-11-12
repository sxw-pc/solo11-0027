"""Snapshot retention analysis with a union of count and age protection."""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path, PurePosixPath

from ..catalog.manifest import Manifest
from ..catalog.snapshot import Snapshot
from ..planner.policy import Policy
from ..utils import parse_utc


def _snapshot_files(snapshots: list[Snapshot]) -> dict[str, dict]:
    values: dict[str, dict] = {}
    for snap in snapshots:
        for item in snap.files:
            current = values.get(item.path)
            if current is None or item.bytes > current["bytes"]:
                values[item.path] = {
                    "path": item.path,
                    "bytes": item.bytes,
                    "rows": item.rows,
                }
    return values


def scan_snapshots(
    root: Path,
    tables: list[dict],
    snapshots: dict[str, list[Snapshot]],
    manifests: dict[str, Manifest | None],
    policy: Policy,
) -> dict:
    report_tables: dict[str, dict] = {}
    for table in tables:
        name = table["name"]
        snapshots_list = snapshots.get(name, [])
        settings = policy.settings(name)
        cutoff = policy.as_of - timedelta(days=settings.keep_days)
        protected_by_count = {
            snap.snapshot_id
            for snap in snapshots_list[-settings.keep_last_snapshots :]
        } if settings.keep_last_snapshots else set()
        protected = set()
        expired = []
        for snap in snapshots_list:
            recent = parse_utc(snap.created_at) >= cutoff
            if snap.snapshot_id in protected_by_count or recent:
                protected.add(snap.snapshot_id)
            else:
                expired.append(snap)
        protected_snaps = [snap for snap in snapshots_list
                           if snap.snapshot_id in protected]
        manifest = manifests.get(name)
        current_paths = manifest.paths if manifest else set()
        protected_paths = set(current_paths)
        for snap in protected_snaps:
            protected_paths.update(snap.paths)
        expired_paths = set()
        for snap in expired:
            expired_paths.update(snap.paths)
        unique_paths = sorted(expired_paths - protected_paths)
        known = _snapshot_files(expired)
        data_files = []
        table_root = root / name
        for rel in unique_paths:
            path = table_root / Path(rel)
            if path.is_file():
                data_files.append(
                    {
                        "path": rel,
                        "bytes": path.stat().st_size,
                        "rows": known.get(rel, {}).get("rows", 0),
                    }
                )
        snapshot_bytes = sum(snap.bytes for snap in expired)
        data_bytes = sum(item["bytes"] for item in data_files)
        affected = sorted(
            {
                parent if (parent := str(PurePosixPath(item["path"]).parent)) != "."
                else ""
                for item in data_files
            }
        )
        report_tables[name] = {
            "table": name,
            "total_snapshots": len(snapshots_list),
            "kept_snapshot_ids": [snap.snapshot_id for snap in protected_snaps],
            "expired_snapshot_ids": [snap.snapshot_id for snap in expired],
            "expired": [
                {
                    "snapshot_id": snap.snapshot_id,
                    "sequence": snap.sequence,
                    "created_at": snap.created_at,
                    "snapshot_bytes": snap.bytes,
                }
                for snap in expired
            ],
            "unique_data_files": data_files,
            "affected_partitions": affected,
            "snapshot_bytes": snapshot_bytes,
            "data_bytes": data_bytes,
            "released_bytes": snapshot_bytes + data_bytes,
            "expired_snapshot_count": len(expired),
        }
    expired_total = sum(item["expired_snapshot_count"] for item in report_tables.values())
    released = sum(item["released_bytes"] for item in report_tables.values())
    data_files = sum(len(item["unique_data_files"]) for item in report_tables.values())
    return {
        "as_of": policy.as_of_iso,
        "keep_last_snapshots": policy.keep_last_snapshots,
        "keep_days": policy.keep_days,
        "tables": report_tables,
        "expired_snapshot_count": expired_total,
        "unique_data_file_count": data_files,
        "released_bytes": released,
    }
