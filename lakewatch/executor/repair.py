"""Prune current-manifest entries that point at files no longer on disk."""
from __future__ import annotations

from pathlib import Path

from ..catalog.manifest import Manifest, load_manifest, write_manifest
from ..catalog.snapshot import load_snapshots, next_sequence, write_snapshot
from ..utils import LakewatchError, digest, resolve_under, utc_now_iso


class RepairError(LakewatchError):
    """Raised when a manifest cannot be repaired safely."""


def execute_repair(root: Path, action: dict) -> dict:
    table_name = action["table"]
    table_dir = (root / table_name).resolve()
    manifest, _ = load_manifest(table_dir)
    if manifest is None:
        raise RepairError(f"cannot repair {table_name}: manifest unavailable")
    requested = set(action["expected"].get("drop_paths", []))
    drop = []
    for item in manifest.files:
        if item.path not in requested:
            continue
        path = resolve_under(table_dir, item.path)
        # A file that reappeared after planning is not safe to drop.
        if not path.exists():
            drop.append(item)
    if not drop:
        return {
            "status": "completed",
            "applied": False,
            "dropped_entries": [],
            "manifest_generation": manifest.generation,
            "output_hash": digest(
                {"table": table_name, "generation": manifest.generation, "noop": True}
            ),
        }
    drop_paths = {item.path for item in drop}
    kept = [item for item in manifest.files if item.path not in drop_paths]
    updated_at = utc_now_iso()
    updated = Manifest(
        table=table_name,
        generation=manifest.generation + 1,
        updated_at=updated_at,
        files=tuple(kept),
        path=manifest.path,
    )
    snapshots, _ = load_snapshots(table_dir)
    snapshot = write_snapshot(
        table_dir, kept, updated_at, next_sequence(snapshots)
    )
    write_manifest(table_dir, updated)
    output_hash = digest(
        {
            "table": table_name,
            "generation": updated.generation,
            "dropped": sorted(drop_paths),
        }
    )
    return {
        "status": "completed",
        "applied": True,
        "dropped_entries": sorted(drop_paths),
        "manifest_generation": updated.generation,
        "snapshot_id": snapshot.snapshot_id,
        "output_hash": output_hash,
    }
