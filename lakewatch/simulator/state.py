"""Read-only, mutable-in-memory projection of a lake's current state.

The state mirrors exactly the three things the executors touch:

* data files on disk (path, byte size, lazily computed content SHA-256),
* the current manifest of each table (its ordered entries),
* the snapshot JSON files of each table.

Nothing here is ever written back.  A :class:`TableState` is cloned per action
replay so the engine can apply projected changes (compaction output, dropped
manifest entries, removed snapshots) while the original on-disk state stays
untouched.  Only files referenced by a plan are hashed, and only once.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..catalog.layout import discover_lake
from ..catalog.manifest import ManifestFile, load_manifest
from ..catalog.snapshot import Snapshot, load_snapshots
from ..utils import sha256_file


@dataclass
class FileState:
    """A data file visible on disk, or a file projected by a simulated action."""

    path: str
    bytes: int
    rows: int = 0
    sha256: str | None = None
    projected: bool = False

    def descriptor(self) -> ManifestFile:
        return ManifestFile(
            path=self.path,
            bytes=self.bytes,
            rows=self.rows,
            sha256=self.sha256,
        )


@dataclass
class SnapshotState:
    """A snapshot JSON file, or a snapshot projected by a simulated action."""

    snapshot_id: str
    sequence: int
    created_at: str
    files: tuple[ManifestFile, ...]
    bytes: int
    projected: bool = False

    @property
    def paths(self) -> set[str]:
        return {item.path for item in self.files}


@dataclass
class TableState:
    name: str
    table_dir: Path
    manifest_available: bool
    manifest_generation: int
    # Relative data-file path -> disk/projected file state.
    files: dict[str, FileState] = field(default_factory=dict)
    # Current manifest entries keyed by relative path.
    manifest_files: dict[str, ManifestFile] = field(default_factory=dict)
    # Snapshot id -> state, kept in on-disk sequence order via an index list.
    snapshots: dict[str, SnapshotState] = field(default_factory=dict)
    snapshot_order: list[str] = field(default_factory=list)
    # Relative data-file paths that an earlier simulated action has consumed.
    consumed: set[str] = field(default_factory=set)
    _sha_cache: dict[str, str] = field(default_factory=dict)

    def sha_of(self, rel_path: str) -> str | None:
        """Hash a referenced data file once; projected/missing files have none."""
        item = self.files.get(rel_path)
        if item is None or item.projected:
            return None
        if rel_path not in self._sha_cache:
            abs_path = self.table_dir / Path(rel_path)
            if abs_path.is_file():
                self._sha_cache[rel_path] = sha256_file(abs_path)
        return self._sha_cache.get(rel_path)

    @property
    def manifest_paths(self) -> set[str]:
        return set(self.manifest_files)

    @property
    def disk_paths(self) -> set[str]:
        return set(self.files)

    @property
    def total_data_bytes(self) -> int:
        return sum(item.bytes for item in self.files.values())

    @property
    def snapshot_bytes(self) -> int:
        # Projected snapshots do not exist on disk, so their JSON size is unknown
        # and must not contribute to a byte projection.
        return sum(
            snap.bytes for snap in self.snapshots.values() if not snap.projected
        )

    def ordered_snapshots(self) -> list[SnapshotState]:
        return [self.snapshots[sid] for sid in self.snapshot_order]

    def missing_paths(self) -> list[str]:
        # Mirrors check_consistency: tables without a manifest cannot have
        # manifest-vs-disk drift, so they contribute neither side of the diff.
        if not self.manifest_available:
            return []
        return sorted(set(self.manifest_files) - set(self.files))

    def orphan_paths(self) -> list[str]:
        if not self.manifest_available:
            return []
        return sorted(set(self.files) - set(self.manifest_files))

    def next_sequence(self) -> int:
        return max(
            (snap.sequence for snap in self.snapshots.values()), default=0
        ) + 1


def _snapshot_state(snapshot: Snapshot) -> SnapshotState:
    return SnapshotState(
        snapshot_id=snapshot.snapshot_id,
        sequence=snapshot.sequence,
        created_at=snapshot.created_at,
        files=snapshot.files,
        bytes=snapshot.path.stat().st_size if snapshot.path.exists() else 0,
        projected=False,
    )


@dataclass
class LakeState:
    root: Path
    tables: dict[str, TableState] = field(default_factory=dict)
    table_order: list[str] = field(default_factory=list)

    def table(self, name: str) -> TableState | None:
        return self.tables.get(name)


def load_lake_state(root: Path | str) -> LakeState:
    """Read the current lake layout, manifests and snapshots without writing.

    The data-file census comes from :func:`discover_lake`, the same read-only
    layout pass the scanner uses, so byte/file totals agree with a real scan.
    Parquet content is never opened: hashing is lazy and limited to the files a
    plan actually references.
    """
    root_path = Path(root).resolve()
    layouts = discover_lake(root_path)
    state = LakeState(root=root_path)
    for layout in layouts:
        table_dir = layout.path
        manifest, _ = load_manifest(table_dir)
        snapshots, _ = load_snapshots(table_dir)
        disk_files = {
            item.rel_path: FileState(
                path=item.rel_path,
                bytes=item.size,
                rows=0,
                sha256=None,
                projected=False,
            )
            for item in layout.files
        }
        if manifest is not None:
            for entry in manifest.files:
                if entry.path in disk_files:
                    # The manifest is the authoritative row/sha source at scan time.
                    disk_files[entry.path].rows = entry.rows
                    disk_files[entry.path].sha256 = entry.sha256
        table = TableState(
            name=layout.name,
            table_dir=table_dir,
            manifest_available=manifest is not None,
            manifest_generation=manifest.generation if manifest else 0,
            files=disk_files,
            manifest_files={entry.path: entry for entry in manifest.files}
            if manifest is not None else {},
            snapshots={snap.snapshot_id: _snapshot_state(snap) for snap in snapshots},
            snapshot_order=[snap.snapshot_id for snap in snapshots],
        )
        state.tables[layout.name] = table
        state.table_order.append(layout.name)
    return state


__all__ = [
    "FileState",
    "LakeState",
    "SnapshotState",
    "TableState",
    "load_lake_state",
]
