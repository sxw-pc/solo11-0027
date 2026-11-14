"""Read and write the lightweight current-table manifest."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from ..utils import (
    LakewatchError,
    atomic_write_text,
    json_text,
    make_issue,
    parse_utc,
    safe_relpath,
    sha256_file,
)

MANIFEST_FORMAT = "lakewatch.manifest/1"
SHA_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class ManifestFile:
    path: str
    bytes: int
    rows: int
    sha256: str | None = None
    # Only populated for files adopted from an orphan; regular entries keep the
    # existing on-disk manifest shape byte-for-byte.
    schema: str | None = None

    def as_dict(self) -> dict:
        value = {"path": self.path, "bytes": self.bytes, "rows": self.rows}
        if self.sha256:
            value["sha256"] = self.sha256
        if self.schema:
            value["schema"] = self.schema
        return value


@dataclass
class Manifest:
    table: str
    generation: int
    updated_at: str
    files: tuple[ManifestFile, ...]
    path: Path

    @property
    def paths(self) -> set[str]:
        return {item.path for item in self.files}

    @property
    def total_bytes(self) -> int:
        return sum(item.bytes for item in self.files)

    def as_dict(self) -> dict:
        return {
            "format": MANIFEST_FORMAT,
            "table": self.table,
            "generation": self.generation,
            "updated_at": self.updated_at,
            "files": [item.as_dict() for item in self.files],
        }


def parse_files(table: str, values: Any, source: str) -> tuple[tuple[ManifestFile, ...], list[dict]]:
    """Validate a manifest/snapshot file list into descriptors plus issues."""
    issues: list[dict] = []
    if not isinstance(values, list):
        return (), [
            make_issue("manifest_invalid", "error", table, "", f"{source} files must be a list")
        ]
    files: list[ManifestFile] = []
    seen: set[str] = set()
    for index, raw in enumerate(values):
        if not isinstance(raw, dict):
            issues.append(make_issue(
                "manifest_invalid", "error", table, "",
                f"{source} file entry {index} is not an object"))
            continue
        try:
            rel = safe_relpath(raw.get("path"))
        except LakewatchError as exc:
            issues.append(make_issue(
                "manifest_invalid", "error", table, "", f"{source} entry {index}: {exc}",
                files=[str(raw.get("path", ""))]))
            continue
        size, rows = raw.get("bytes"), raw.get("rows")
        if (not isinstance(size, int) or isinstance(size, bool) or size < 0
                or not isinstance(rows, int) or isinstance(rows, bool) or rows < 0):
            issues.append(make_issue(
                "manifest_invalid", "error", table, "",
                f"{source} entry {rel} has invalid bytes/rows", files=[rel]))
            continue
        sha = raw.get("sha256")
        if sha is not None and (not isinstance(sha, str) or not SHA_RE.match(sha)):
            issues.append(make_issue(
                "manifest_invalid", "error", table, "",
                f"{source} entry {rel} has invalid sha256", files=[rel]))
            sha = None
        schema = raw.get("schema")
        if schema is not None and (not isinstance(schema, str) or not schema.strip()):
            issues.append(make_issue(
                "manifest_invalid", "error", table, "",
                f"{source} entry {rel} has invalid schema", files=[rel]))
            schema = None
        if rel in seen:
            issues.append(make_issue(
                "manifest_duplicate_file", "error", table, "",
                f"{source} lists {rel} more than once", files=[rel]))
            continue
        seen.add(rel)
        files.append(ManifestFile(path=rel, bytes=size, rows=rows, sha256=sha,
                                  schema=schema))
    files.sort(key=lambda item: item.path)
    return tuple(files), issues


def load_manifest(table_dir: Path) -> tuple[Manifest | None, list[dict]]:
    table = table_dir.name
    path = table_dir / "_metadata" / "manifest.json"
    if not path.exists():
        return None, [make_issue(
            "manifest_missing", "error", table, "", "_metadata/manifest.json is missing")]
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return None, [make_issue(
            "manifest_invalid", "error", table, "", f"manifest is not valid JSON: {exc}")]
    if not isinstance(raw, dict) or raw.get("format") != MANIFEST_FORMAT:
        return None, [make_issue(
            "manifest_invalid", "error", table, "",
            f"manifest format must be {MANIFEST_FORMAT}")]
    generation = raw.get("generation")
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 0:
        return None, [make_issue(
            "manifest_invalid", "error", table, "", "generation must be >= 0")]
    try:
        parse_utc(raw.get("updated_at", ""))
    except LakewatchError as exc:
        return None, [make_issue("manifest_invalid", "error", table, "", str(exc))]
    files, issues = parse_files(table, raw.get("files"), "manifest")
    return Manifest(table, generation, raw["updated_at"], files, path), issues


def write_manifest(table_dir: Path, manifest: Manifest) -> None:
    atomic_write_text(manifest.path, json_text(manifest.as_dict()))


def file_descriptor(
    table_dir: Path, rel_path: str, rows: int, schema: str | None = None
) -> ManifestFile:
    rel = safe_relpath(rel_path)
    path = table_dir / Path(rel)
    return ManifestFile(
        path=rel, bytes=path.stat().st_size, rows=rows, sha256=sha256_file(path),
        schema=schema,
    )


def parquet_descriptor(table_dir: Path, rel_path: str) -> ManifestFile:
    """Describe a candidate data file, refusing anything pyarrow cannot open.

    Adoption uses this so the recorded rows/schema/sha are derived from the
    file itself rather than from the planner's cached scan result.
    """
    rel = safe_relpath(rel_path)
    path = table_dir / Path(rel)
    if not path.is_file():
        raise LakewatchError(f"candidate file is missing: {rel}")
    try:
        parquet = pq.ParquetFile(path)
        rows = int(parquet.metadata.num_rows)
        schema = parquet.schema_arrow.to_string()
    except Exception as exc:  # pyarrow raises several backend-specific errors
        raise LakewatchError(f"candidate is not valid parquet: {rel}: {exc}") from exc
    return ManifestFile(
        path=rel, bytes=path.stat().st_size, rows=rows,
        sha256=sha256_file(path), schema=schema,
    )


def __getattr__(name: str):
    """Compatibility exports for the snapshot helpers (avoids a circular import)."""
    if name in {"Snapshot", "load_snapshots", "next_sequence", "write_snapshot"}:
        from . import snapshot

        return getattr(snapshot, name)
    raise AttributeError(name)
