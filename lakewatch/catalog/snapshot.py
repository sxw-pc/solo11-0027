"""Lightweight snapshot manifests and their sequence helpers."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ..utils import LakewatchError, atomic_write_text, json_text, make_issue, parse_utc
from .manifest import ManifestFile, parse_files

SNAPSHOT_FORMAT = "lakewatch.snapshot/1"


@dataclass
class Snapshot:
    table: str
    snapshot_id: str
    sequence: int
    created_at: str
    files: tuple[ManifestFile, ...]
    path: Path

    @property
    def paths(self) -> set[str]:
        return {item.path for item in self.files}

    @property
    def bytes(self) -> int:
        return self.path.stat().st_size if self.path.exists() else 0


def load_snapshots(table_dir: Path) -> tuple[list[Snapshot], list[dict]]:
    table = table_dir.name
    snap_dir = table_dir / "_metadata" / "snapshots"
    if not snap_dir.is_dir():
        return [], [make_issue(
            "snapshot_missing_dir", "warning", table, "",
            "_metadata/snapshots directory is missing")]
    snapshots: list[Snapshot] = []
    issues: list[dict] = []
    seen_ids: set[str] = set()
    seen_sequences: set[int] = set()
    for path in sorted(snap_dir.glob("*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            issues.append(make_issue(
                "snapshot_invalid", "error", table, "",
                f"{path.name} is not valid JSON: {exc}", files=[path.name]))
            continue
        if not isinstance(raw, dict) or raw.get("format") != SNAPSHOT_FORMAT:
            issues.append(make_issue(
                "snapshot_invalid", "error", table, "",
                f"{path.name} has an unsupported format", files=[path.name]))
            continue
        sid, seq = raw.get("snapshot_id"), raw.get("sequence")
        if (not isinstance(sid, str) or not sid or not isinstance(seq, int)
                or isinstance(seq, bool) or seq < 1):
            issues.append(make_issue(
                "snapshot_invalid", "error", table, "",
                f"{path.name} has invalid id or sequence", files=[path.name]))
            continue
        try:
            parse_utc(raw.get("created_at", ""))
        except LakewatchError as exc:
            issues.append(make_issue(
                "snapshot_invalid", "error", table, "",
                f"{path.name}: {exc}", files=[path.name]))
            continue
        if sid in seen_ids or seq in seen_sequences:
            issues.append(make_issue(
                "snapshot_duplicate", "error", table, "",
                f"duplicate snapshot id/sequence at {path.name}", files=[path.name]))
            continue
        files, file_issues = parse_files(table, raw.get("files"), "snapshot")
        issues.extend(file_issues)
        seen_ids.add(sid)
        seen_sequences.add(seq)
        snapshots.append(
            Snapshot(table, sid, seq, raw["created_at"], files, path)
        )
    snapshots.sort(key=lambda item: (item.sequence, item.snapshot_id))
    return snapshots, issues


def next_sequence(snapshots: Iterable[Snapshot]) -> int:
    return max((item.sequence for item in snapshots), default=0) + 1


def write_snapshot(
    table_dir: Path, files: Iterable[ManifestFile], created_at: str, sequence: int
) -> Snapshot:
    ordered = tuple(sorted(files, key=lambda item: item.path))
    snapshot_id = f"snap-{sequence:04d}"
    path = table_dir / "_metadata" / "snapshots" / f"{snapshot_id}.json"
    doc = {
        "format": SNAPSHOT_FORMAT,
        "table": table_dir.name,
        "snapshot_id": snapshot_id,
        "sequence": sequence,
        "created_at": created_at,
        "files": [item.as_dict() for item in ordered],
    }
    atomic_write_text(path, json_text(doc))
    return Snapshot(table_dir.name, snapshot_id, sequence, created_at, ordered, path)
