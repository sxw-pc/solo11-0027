"""Expired snapshot removal with a second safety check before data deletion."""
from __future__ import annotations

from pathlib import Path

from ..catalog.manifest import load_manifest
from ..catalog.snapshot import load_snapshots
from ..utils import LakewatchError, digest, resolve_under


class VacuumError(LakewatchError):
    """Raised when snapshot vacuuming would touch protected data."""


def execute_vacuum(root: Path, action: dict) -> dict:
    table_name = action["table"]
    table_dir = (root / table_name).resolve()
    manifest, _ = load_manifest(table_dir)
    snapshots, _ = load_snapshots(table_dir)
    by_id = {item.snapshot_id: item for item in snapshots}
    delete_ids = list(action["expected"].get("delete_snapshot_ids", []))
    delete_data = list(action["expected"].get("delete_data_files", []))
    current_paths = manifest.paths if manifest else set()

    removed_snapshots = []
    snapshot_bytes = 0
    for snapshot_id in delete_ids:
        snap = by_id.get(snapshot_id)
        if snap is None:
            continue
        snapshot_bytes += snap.bytes
        snap.path.unlink(missing_ok=True)
        removed_snapshots.append(snapshot_id)

    remaining, _ = load_snapshots(table_dir)
    remaining_paths: set[str] = set()
    for snap in remaining:
        remaining_paths.update(snap.paths)
    removed_data = []
    data_bytes = 0
    for rel in delete_data:
        if rel in current_paths or rel in remaining_paths:
            continue
        path = resolve_under(table_dir, rel)
        if path == table_dir or table_dir not in path.parents:
            raise VacuumError(f"refusing to delete outside table: {rel}")
        if path.is_file():
            data_bytes += path.stat().st_size
            path.unlink()
            removed_data.append(rel)

    output_hash = digest(
        {"snapshots": sorted(removed_snapshots), "data_files": sorted(removed_data)}
    )
    return {
        "status": "completed",
        "deleted_snapshots": sorted(removed_snapshots),
        "deleted_data_files": sorted(removed_data),
        "snapshot_bytes": snapshot_bytes,
        "data_bytes": data_bytes,
        "released_bytes": snapshot_bytes + data_bytes,
        "remaining_snapshots": len(remaining),
        "output_hash": output_hash,
    }
