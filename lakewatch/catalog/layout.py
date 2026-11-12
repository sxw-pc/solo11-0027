"""Parse the on-disk lake layout without trusting it.

The layout is intentionally simple: one directory per table, optional Hive-like
``key=value`` partition directories, and a ``_metadata`` directory owned by
Lakewatch.  Parsing must never hide malformed paths; they become diagnostics.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

from ..utils import LakewatchError, make_issue

TABLE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")
PART_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PART_FORBIDDEN_RE = re.compile(r'[\x00-\x1f\\/:*?"<>|]')


@dataclass(frozen=True)
class LakeFile:
    table: str
    rel_path: str
    partition: str
    abs_path: Path
    size: int
    valid_partition: bool


@dataclass
class TableLayout:
    name: str
    path: Path
    has_metadata: bool
    files: list[LakeFile] = field(default_factory=list)
    issues: list[dict] = field(default_factory=list)
    partition_keys: tuple[str, ...] = ()

    @property
    def disk_paths(self) -> set[str]:
        return {item.rel_path for item in self.files}

    def partitions(self) -> dict[str, list[LakeFile]]:
        result: dict[str, list[LakeFile]] = {}
        for item in self.files:
            result.setdefault(item.partition, []).append(item)
        for files in result.values():
            files.sort(key=lambda item: item.rel_path)
        return dict(sorted(result.items()))


def _validate_partition(
    table: str, rel_path: str, partition: str, issues: list[dict]
) -> tuple[bool, tuple[str, ...]]:
    if partition == "":
        return True, ()
    keys: list[str] = []
    for segment in partition.split("/"):
        if "=" not in segment:
            issues.append(
                make_issue(
                    "layout_bad_partition",
                    "error",
                    table,
                    partition,
                    f"partition segment {segment!r} is not key=value",
                    files=[rel_path],
                    segment=segment,
                )
            )
            return False, ()
        key, value = segment.split("=", 1)
        if not PART_KEY_RE.match(key) or not value or PART_FORBIDDEN_RE.search(value):
            issues.append(
                make_issue(
                    "layout_bad_partition",
                    "error",
                    table,
                    partition,
                    f"invalid partition segment {segment!r}",
                    files=[rel_path],
                    segment=segment,
                )
            )
            return False, ()
        keys.append(key)
    return True, tuple(keys)


def _parse_table(path: Path) -> TableLayout:
    name = path.name
    issues: list[dict] = []
    metadata = path / "_metadata"
    has_metadata = metadata.is_dir()
    if not TABLE_NAME_RE.match(name):
        issues.append(
            make_issue(
                "layout_bad_table_name",
                "warning",
                name,
                "",
                "table directory name contains unsupported characters",
                files=[name],
            )
        )
    if not has_metadata:
        issues.append(
            make_issue(
                "layout_missing_metadata",
                "error",
                name,
                "",
                "table has no _metadata directory",
            )
        )

    table = TableLayout(name=name, path=path, has_metadata=has_metadata)
    signatures: dict[tuple[str, ...], list[str]] = {}
    for dirpath, dirnames, filenames in os.walk(path):
        current = Path(dirpath)
        rel_dir = current.relative_to(path)
        # Never descend into metadata, hidden, or temporary directories.
        dirnames[:] = sorted(
            item
            for item in dirnames
            if item != "_metadata" and not item.startswith(".")
        )
        if rel_dir.parts and rel_dir.parts[0] == "_metadata":
            continue
        for filename in sorted(filenames):
            if filename.startswith(".") or filename.endswith(".tmp"):
                continue
            abs_path = current / filename
            rel_path = abs_path.relative_to(path).as_posix()
            partition = "" if rel_dir == Path(".") else rel_dir.as_posix()
            if not filename.endswith(".parquet"):
                issues.append(
                    make_issue(
                        "layout_non_parquet_file",
                        "warning",
                        name,
                        partition,
                        "non-parquet file found in table data area",
                        files=[rel_path],
                    )
                )
                continue
            valid, keys = _validate_partition(name, rel_path, partition, issues)
            if valid:
                signatures.setdefault(keys, []).append(partition)
            try:
                size = abs_path.stat().st_size
            except OSError:
                size = 0
                issues.append(
                    make_issue(
                        "layout_unreadable_file",
                        "error",
                        name,
                        partition,
                        "file could not be stat'ed",
                        files=[rel_path],
                    )
                )
            table.files.append(
                LakeFile(
                    table=name,
                    rel_path=rel_path,
                    partition=partition,
                    abs_path=abs_path,
                    size=size,
                    valid_partition=valid,
                )
            )

    if len(signatures) > 1:
        rendered = sorted("/".join(keys) if keys else "<root>" for keys in signatures)
        issues.append(
            make_issue(
                "layout_key_mismatch",
                "error",
                name,
                "",
                "partitions use inconsistent key sequences",
                files=[rendered[0], rendered[1]],
                key_sequences=rendered,
            )
        )
    if signatures:
        # Pick the most common signature; ties resolve lexicographically.
        counts = {keys: len(paths) for keys, paths in signatures.items()}
        winner = sorted(counts, key=lambda key: (-counts[key], key))[0]
        table.partition_keys = winner
    if not table.files:
        issues.append(
            make_issue("layout_empty_table", "warning", name, "", "table has no data files")
        )
    table.files.sort(key=lambda item: item.rel_path)
    table.issues = sorted(
        issues, key=lambda item: (item["category"], item["partition"], item["reason"])
    )
    return table


def discover_lake(root: Path | str) -> list[TableLayout]:
    """Discover tables in deterministic order. Missing roots raise a clear error."""
    root_path = Path(root)
    if not root_path.exists():
        raise LakewatchError(f"lake root does not exist: {root_path}")
    if not root_path.is_dir():
        raise LakewatchError(f"lake root is not a directory: {root_path}")
    tables = []
    for child in sorted(root_path.iterdir(), key=lambda item: item.name):
        if not child.is_dir() or child.name.startswith((".", "_")):
            continue
        tables.append(_parse_table(child))
    return tables


def build_partitions(layout: TableLayout) -> list[dict]:
    """Return partition summaries including the (possibly invalid) raw name."""
    summaries = []
    for name, files in layout.partitions().items():
        total = sum(item.size for item in files)
        summaries.append(
            {
                "name": name,
                "valid": all(item.valid_partition for item in files),
                "file_count": len(files),
                "bytes": total,
                "avg_file_bytes": total // len(files) if files else 0,
                "files": [item.rel_path for item in files],
            }
        )
    return summaries
