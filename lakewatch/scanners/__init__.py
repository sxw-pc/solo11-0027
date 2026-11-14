"""Read-only lake scanning pipeline used by CLI, planner, and web."""
from __future__ import annotations

from pathlib import Path

from ..catalog.layout import discover_lake
from ..catalog.manifest import load_manifest
from ..catalog.snapshot import load_snapshots
from ..planner.policy import Policy
from .consistency import check_consistency
from .file_scan import ScanResult, build_tables
from .snapshot_scan import scan_snapshots

__all__ = ["ScanResult", "scan_lake"]


def scan_lake(root: Path | str, policy: Policy) -> ScanResult:
    """Scan without modifying the lake except for file reads."""
    root_path = Path(root).resolve()
    layouts = discover_lake(root_path)
    manifests = {}
    snapshots = {}
    load_diagnostics = []
    for layout in layouts:
        manifest, manifest_issues = load_manifest(layout.path)
        manifest_snaps, snapshot_issues = load_snapshots(layout.path)
        manifests[layout.name] = manifest
        snapshots[layout.name] = manifest_snaps
        load_diagnostics.extend(manifest_issues)
        load_diagnostics.extend(snapshot_issues)
    tables, diagnostics = build_tables(
        root_path, layouts, manifests, snapshots, policy
    )
    consistency, consistency_diagnostics = check_consistency(
        root_path, tables, manifests, snapshots, policy
    )
    snapshot_report = scan_snapshots(
        root_path, tables, snapshots, manifests, policy
    )
    diagnostics = diagnostics + load_diagnostics + consistency_diagnostics
    diagnostics.sort(
        key=lambda item: (
            item["category"], item["table"], item["partition"], item["reason"]
        )
    )
    return ScanResult(
        root=root_path,
        as_of=policy.as_of_iso,
        policy=policy,
        tables=tables,
        diagnostics=diagnostics,
        layouts={layout.name: layout for layout in layouts},
        manifests=manifests,
        snapshots=snapshots,
        consistency=consistency,
        snapshot_report=snapshot_report,
    )
