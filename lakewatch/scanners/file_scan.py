"""The shared scan result and the file-layout diagnostic pass."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from ..catalog.layout import TableLayout, build_partitions
from ..catalog.manifest import Manifest
from ..catalog.snapshot import Snapshot
from ..planner.policy import Policy
from ..utils import digest, make_issue, sha256_file


@dataclass
class ScanResult:
    root: Path
    as_of: str
    policy: Policy
    tables: list[dict]
    diagnostics: list[dict]
    layouts: dict[str, TableLayout]
    manifests: dict[str, Manifest | None]
    snapshots: dict[str, list[Snapshot]]
    consistency: dict[str, dict] = field(default_factory=dict)
    snapshot_report: dict = field(default_factory=dict)

    def summary(self) -> dict:
        orphan_count = sum(
            item.get("orphan_count", 0) for item in self.consistency.values()
        )
        missing_count = sum(
            item.get("missing_count", 0) for item in self.consistency.values()
        )
        severities = {"info": 0, "warning": 0, "error": 0}
        for item in self.diagnostics:
            severities[item["severity"]] = severities.get(item["severity"], 0) + 1
        snap = self.snapshot_report or {}
        return {
            "table_count": len(self.tables),
            "partition_count": sum(item["partition_count"] for item in self.tables),
            "file_count": sum(item["file_count"] for item in self.tables),
            "total_bytes": sum(item["bytes"] for item in self.tables),
            "small_file_count": sum(item["small_file_count"] for item in self.tables),
            "orphan_count": orphan_count,
            "missing_count": missing_count,
            "expired_snapshot_count": snap.get("expired_snapshot_count", 0),
            "releasable_bytes": snap.get("released_bytes", 0),
            "diagnostic_count": len(self.diagnostics),
            "diagnostics_by_severity": severities,
        }

    def input_digest(self) -> str:
        material = {
            "tables": [
                {
                    "name": table["name"],
                    "generation": table.get("manifest_generation"),
                    "files": [
                        {key: item[key] for key in ("path", "bytes", "rows", "sha256")}
                        for item in table["files"]
                    ],
                }
                for table in self.tables
            ],
            "snapshots": {
                name: [
                    {
                        "id": snap.snapshot_id,
                        "sequence": snap.sequence,
                        "created_at": snap.created_at,
                        "files": [
                            {"path": item.path, "bytes": item.bytes, "rows": item.rows}
                            for item in snap.files
                        ],
                    }
                    for snap in values
                ]
                for name, values in sorted(self.snapshots.items())
            },
            "diagnostics": [item["id"] for item in self.diagnostics],
        }
        return digest(material)

    def public(self) -> dict:
        return {
            "as_of": self.as_of,
            "policy_hash": self.policy.policy_hash,
            "summary": self.summary(),
            "tables": self.tables,
            "diagnostics": self.diagnostics,
            "snapshots": self.snapshot_report,
            "consistency": self.consistency,
        }


def _rows_and_sha(path: Path) -> tuple[int, str, str | None]:
    try:
        rows = pq.ParquetFile(path).metadata.num_rows
        return int(rows), sha256_file(path), None
    except Exception as exc:  # pyarrow raises several backend-specific exceptions
        return 0, sha256_file(path), str(exc)


def _small_count(files: list[dict], threshold: int) -> int:
    return sum(1 for item in files if item["bytes"] < threshold)


def build_tables(
    root: Path, layouts: list[TableLayout], manifests: dict[str, Manifest | None],
    snapshots: dict[str, list[Snapshot]], policy: Policy
) -> tuple[list[dict], list[dict]]:
    """Build table/partition summaries and threshold diagnostics."""
    tables: list[dict] = []
    diagnostics: list[dict] = []
    for layout in layouts:
        settings = policy.settings(layout.name)
        diagnostic_sources = list(layout.issues)
        files: list[dict] = []
        for item in layout.files:
            rows, sha, error = _rows_and_sha(item.abs_path)
            if error:
                diagnostic_sources.append(
                    make_issue(
                        "unreadable_parquet", "error", layout.name, item.partition,
                        f"parquet metadata could not be read: {error}", files=[item.rel_path]
                    )
                )
            files.append(
                {
                    "path": item.rel_path,
                    "partition": item.partition,
                    "bytes": item.size,
                    "rows": rows,
                    "sha256": sha,
                    "valid_partition": item.valid_partition,
                }
            )
        manifest = manifests.get(layout.name)
        partition_names = layout.partitions()
        partitions = build_partitions(layout)
        for part in partitions:
            part_files = [item for item in files if item["partition"] == part["name"]]
            part["small_file_count"] = _small_count(part_files, settings.small_file_bytes)
            small = [item["path"] for item in part_files
                     if item["bytes"] < settings.small_file_bytes]
            if small:
                listed = small[: settings.max_files_listed]
                diagnostic_sources.append(
                    make_issue(
                        "small_file", "warning", layout.name, part["name"],
                        f"{len(small)} file(s) smaller than {settings.small_file_bytes} bytes",
                        files=listed, count=len(small),
                        hidden_files=max(0, len(small) - len(listed)),
                    )
                )
            if part["file_count"] > settings.max_files_per_partition:
                listed = part["files"][: settings.max_files_listed]
                diagnostic_sources.append(
                    make_issue(
                        "partition_skew", "warning", layout.name, part["name"],
                        f"{part['file_count']} files exceed limit "
                        f"{settings.max_files_per_partition}",
                        files=listed, count=part["file_count"],
                        hidden_files=max(0, part["file_count"] - len(listed)),
                    )
                )
        total_bytes = sum(item.size for item in layout.files)
        small_count = _small_count(files, settings.small_file_bytes)
        tables.append(
            {
                "name": layout.name,
                "has_metadata": layout.has_metadata,
                "partition_keys": list(layout.partition_keys),
                "manifest_generation": manifest.generation if manifest else None,
                "snapshot_count": len(snapshots.get(layout.name, [])),
                "partition_count": len(partition_names),
                "file_count": len(files),
                "bytes": total_bytes,
                "avg_file_bytes": total_bytes // len(files) if files else 0,
                "small_file_count": small_count,
                "files": files,
                "partitions": partitions,
            }
        )
        diagnostics.extend(diagnostic_sources)
    diagnostics.sort(
        key=lambda item: (
            item["category"], item["table"], item["partition"], item["reason"]
        )
    )
    return tables, diagnostics
