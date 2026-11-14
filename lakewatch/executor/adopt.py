"""Adopt explicitly declared orphan files into the current manifest.

Adoption never moves, rewrites, or deletes a candidate: each file stays at its
path and the new manifest simply starts listing it.  Before touching any state
the executor re-verifies every candidate against the description frozen into
the plan, checks that the manifest generation has not moved, and confirms the
files are still unreferenced.  Any mismatch aborts the whole action with no
writes, so a partially applied adopt is impossible.
"""
from __future__ import annotations

from pathlib import Path

from ..catalog.manifest import (
    Manifest,
    ManifestFile,
    load_manifest,
    parquet_descriptor,
    write_manifest,
)
from ..catalog.snapshot import load_snapshots, next_sequence, write_snapshot
from ..utils import (
    LakewatchError,
    digest,
    resolve_under,
    safe_relpath,
    utc_now_iso,
)


class AdoptError(LakewatchError):
    """Raised when a declared orphan cannot be adopted safely."""


def _partition_of(rel: str) -> str:
    return rel.rsplit("/", 1)[0] if "/" in rel else ""


def _partition_keys(partition: str) -> tuple[str, ...]:
    if not partition:
        return ()
    return tuple(segment.split("=", 1)[0] for segment in partition.split("/"))


def _table_partition_keys(manifest: Manifest) -> tuple[str, ...]:
    """Derive the table's current partition keys from its manifest entries."""
    signatures: dict[tuple[str, ...], int] = {}
    for item in manifest.files:
        keys = _partition_keys(_partition_of(item.path))
        signatures[keys] = signatures.get(keys, 0) + 1
    if not signatures:
        return ()
    return sorted(signatures, key=lambda key: (-signatures[key], key))[0]


def _current_partitions(manifest: Manifest, table_keys: tuple[str, ...]) -> set[str]:
    return {
        _partition_of(item.path)
        for item in manifest.files
        if _partition_keys(_partition_of(item.path)) == table_keys
    }


def _verify_candidate(table_dir: Path, candidate: dict, manifest: Manifest,
                      snapshot_paths: set[str], table_keys: tuple[str, ...],
                      current_partitions: set[str]) -> ManifestFile:
    rel = safe_relpath(candidate["path"])
    if _partition_of(rel) != candidate["partition"]:
        raise AdoptError(f"candidate partition does not match the plan: {rel}")
    if rel in manifest.paths:
        raise AdoptError(f"candidate is already in the current manifest: {rel}")
    if _partition_keys(_partition_of(rel)) != table_keys:
        raise AdoptError(
            f"candidate partition layout does not match table keys: {rel}"
        )
    if _partition_of(rel) not in current_partitions:
        raise AdoptError(
            f"candidate partition is outside the current table layout: {rel}"
        )
    if rel in snapshot_paths:
        raise AdoptError(
            f"candidate is referenced by a historical snapshot; refusing to adopt {rel}"
        )
    path = resolve_under(table_dir, rel)
    if not path.is_file():
        raise AdoptError(f"candidate file is missing: {rel}")
    try:
        descriptor = parquet_descriptor(table_dir, rel)
    except LakewatchError as exc:
        raise AdoptError(str(exc)) from exc
    if descriptor.sha256 != candidate["sha256"]:
        raise AdoptError(f"candidate content changed since planning: {rel}")
    if descriptor.bytes != candidate["bytes"] or descriptor.rows != candidate["rows"]:
        raise AdoptError(f"candidate size/row count changed since planning: {rel}")
    if descriptor.schema != candidate["schema"]:
        raise AdoptError(f"candidate schema changed since planning: {rel}")
    return descriptor


def execute_adoption(root: Path, action: dict) -> dict:
    table_name = action["table"]
    table_dir = (root / table_name).resolve()
    manifest, _ = load_manifest(table_dir)
    if manifest is None:
        raise AdoptError(f"cannot adopt into {table_name}: manifest unavailable")

    expected = action["expected"]
    if manifest.generation != expected["manifest_generation"]:
        raise AdoptError(
            f"manifest generation moved from {expected['manifest_generation']} "
            f"to {manifest.generation}; refusing to adopt"
        )
    candidates = expected["candidates"]
    if not candidates:
        raise AdoptError("adopt action has no candidates")

    snapshots, _ = load_snapshots(table_dir)
    snapshot_paths: set[str] = set()
    for snap in snapshots:
        snapshot_paths.update(snap.paths)
    table_keys = tuple(expected.get("partition_keys") or ())
    if _table_partition_keys(manifest) != table_keys:
        raise AdoptError(
            "table partition key sequence changed since planning; refusing to adopt"
        )
    current_partitions = _current_partitions(manifest, table_keys)

    descriptors = [
        _verify_candidate(
            table_dir, candidate, manifest, snapshot_paths, table_keys,
            current_partitions,
        )
        for candidate in candidates
    ]
    # Check the batch itself for duplicates before writing anything.
    paths = [item.path for item in descriptors]
    if len(set(paths)) != len(paths):
        raise AdoptError("adopt action lists the same candidate more than once")

    new_files = list(manifest.files)
    new_files.extend(descriptors)
    new_files.sort(key=lambda item: item.path)
    updated_at = utc_now_iso()
    updated = Manifest(
        table=table_name,
        generation=manifest.generation + 1,
        updated_at=updated_at,
        files=tuple(new_files),
        path=manifest.path,
    )
    snapshot = None
    try:
        snapshot = write_snapshot(
            table_dir, new_files, updated_at, next_sequence(snapshots)
        )
        write_manifest(table_dir, updated)
    except Exception:
        if snapshot and snapshot.path.exists():
            snapshot.path.unlink(missing_ok=True)
        raise
    adopted = [item.as_dict() for item in sorted(descriptors, key=lambda x: x.path)]
    output_hash = digest(
        {
            "table": table_name,
            "generation": updated.generation,
            "adopted": adopted,
        }
    )
    return {
        "status": "completed",
        "applied": True,
        "adopted_files": adopted,
        "manifest_generation": updated.generation,
        "snapshot_id": snapshot.snapshot_id if snapshot else None,
        "output_hash": output_hash,
    }
