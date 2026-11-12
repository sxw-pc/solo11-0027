"""Bidirectional manifest/disk comparison with orphan provenance."""
from __future__ import annotations

from ..catalog.manifest import Manifest
from ..catalog.snapshot import Snapshot
from ..planner.policy import Policy
from ..utils import make_issue


def check_consistency(
    tables: list[dict],
    manifests: dict[str, Manifest | None],
    snapshots: dict[str, list[Snapshot]],
    policy: Policy,
) -> tuple[dict[str, dict], list[dict]]:
    result: dict[str, dict] = {}
    diagnostics: list[dict] = []
    for table in tables:
        name = table["name"]
        manifest = manifests.get(name)
        if manifest is None:
            result[name] = {
                "manifest_available": False,
                "missing": [],
                "orphans": [],
                "missing_count": 0,
                "orphan_count": 0,
                "orphan_referenced_count": 0,
                "orphan_unreferenced_count": 0,
            }
            continue
        settings = policy.settings(name)
        disk = {item["path"]: item for item in table["files"]}
        manifest_paths = manifest.paths
        missing = [
            {
                "path": item.path,
                "bytes": item.bytes,
                "rows": item.rows,
                "sha256": item.sha256,
            }
            for item in manifest.files
            if item.path not in disk
        ]
        all_snapshot_paths: set[str] = set()
        for snap in snapshots.get(name, []):
            all_snapshot_paths.update(snap.paths)
        orphans = []
        for path in sorted(set(disk) - manifest_paths):
            scope = "snapshot_referenced" if path in all_snapshot_paths else "unreferenced"
            orphans.append(
                {
                    "path": path,
                    "bytes": disk[path]["bytes"],
                    "rows": disk[path]["rows"],
                    "scope": scope,
                }
            )
        referenced = [item["path"] for item in orphans
                      if item["scope"] == "snapshot_referenced"]
        unreferenced = [item["path"] for item in orphans
                        if item["scope"] == "unreferenced"]
        all_missing = [item["path"] for item in missing]
        if missing:
            listed = all_missing[: settings.max_files_listed]
            diagnostics.append(
                make_issue(
                    "missing_file", "error", name, "",
                    f"{len(missing)} manifest file(s) are absent from disk",
                    files=listed, count=len(missing),
                    hidden_files=max(0, len(missing) - len(listed)),
                )
            )
        if unreferenced:
            listed = unreferenced[: settings.max_files_listed]
            diagnostics.append(
                make_issue(
                    "orphan_unreferenced", "warning", name, "",
                    f"{len(unreferenced)} disk file(s) are referenced by no manifest",
                    files=listed, count=len(unreferenced),
                    hidden_files=max(0, len(unreferenced) - len(listed)),
                    scope="unreferenced",
                )
            )
        if referenced:
            listed = referenced[: settings.max_files_listed]
            diagnostics.append(
                make_issue(
                    "orphan_snapshot_referenced", "warning", name, "",
                    f"{len(referenced)} disk file(s) only appear in historical snapshots",
                    files=listed, count=len(referenced),
                    hidden_files=max(0, len(referenced) - len(listed)),
                    scope="snapshot_referenced",
                )
            )
        result[name] = {
            "manifest_available": True,
            "missing": missing,
            "orphans": orphans,
            "missing_count": len(missing),
            "orphan_count": len(orphans),
            "orphan_referenced_count": len(referenced),
            "orphan_unreferenced_count": len(unreferenced),
        }
    diagnostics.sort(key=lambda item: (item["category"], item["table"], item["reason"]))
    return result, diagnostics
