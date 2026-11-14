"""Bidirectional manifest/disk comparison with orphan provenance.

Orphans are reported in two scopes: files that no manifest lists but a
historical snapshot still references, and files referenced by nothing at all.
Only the latter are eligible for *adoption*, and only when the operator names
the exact path in the policy.  Nothing is ever adopted implicitly.
"""
from __future__ import annotations

from pathlib import Path

from ..catalog.manifest import Manifest, parquet_descriptor
from ..catalog.snapshot import Snapshot
from ..planner.policy import Policy
from ..utils import LakewatchError, make_issue


def _partition_keys(partition: str) -> tuple[str, ...]:
    if not partition:
        return ()
    return tuple(segment.split("=", 1)[0] for segment in partition.split("/"))


def _manifest_partitions(manifest: Manifest, table_keys: tuple[str, ...]) -> set[str]:
    """Partition paths the table currently declares, restricted to its key shape."""
    result: set[str] = set()
    for item in manifest.files:
        partition = item.path.rsplit("/", 1)[0] if "/" in item.path else ""
        if _partition_keys(partition) == table_keys:
            result.add(partition)
    return result


def _adoption(
    root: Path,
    table: dict,
    manifest: Manifest,
    snapshots: list[Snapshot],
    policy: Policy,
    disk: dict[str, dict],
) -> dict:
    """Classify the policy's explicitly named candidates for one table.

    A candidate is eligible only if it is a real, readable parquet file that is
    absent from the current manifest and unreferenced by every historical
    snapshot, and whose partition lies inside the table's current partition
    layout.  Each rejection keeps a stable machine-readable code so the plan and
    report can explain exactly why nothing was generated.
    """
    name = table["name"]
    settings = policy.settings(name)
    declared = policy.candidates_for(name)
    if not settings.adopt_enabled:
        return {
            "enabled": False,
            "declared": list(declared),
            "eligible": [],
            "rejected": [],
        }
    table_keys = tuple(table.get("partition_keys") or ())
    current_partitions = _manifest_partitions(manifest, table_keys)
    snapshot_paths: set[str] = set()
    for snap in snapshots:
        snapshot_paths.update(snap.paths)

    eligible: list[dict] = []
    rejected: list[dict] = []
    for rel in declared:
        reason = None
        code = None
        disk_item = disk.get(rel)
        if manifest is not None and rel in manifest.paths:
            code, reason = "already_in_manifest", "file is already listed in the current manifest"
        elif disk_item is None:
            code, reason = "missing", "file does not exist in the table directory"
        elif not disk_item["valid_partition"]:
            code, reason = "bad_partition", "partition path is not well-formed key=value"
        elif _partition_keys(disk_item["partition"]) != table_keys:
            code = "partition_mismatch"
            reason = (
                "partition key sequence "
                f"{list(_partition_keys(disk_item['partition']))} does not match "
                f"table keys {list(table_keys)}"
            )
        elif disk_item["partition"] not in current_partitions:
            code = "partition_mismatch"
            reason = (
                f"partition {disk_item['partition']!r} is not part of the table's "
                "current partition layout"
            )
        elif rel in snapshot_paths:
            code = "snapshot_referenced"
            reason = "file is referenced by a historical snapshot; refusing to adopt"
        descriptor = None
        if code is None:
            try:
                descriptor = parquet_descriptor(root / name, rel)
            except LakewatchError as exc:
                code, reason = "invalid_parquet", str(exc)
        if code is not None:
            rejected.append({"path": rel, "code": code, "reason": reason})
            continue
        eligible.append(
            {
                "path": descriptor.path,
                "partition": disk_item["partition"],
                "bytes": descriptor.bytes,
                "rows": descriptor.rows,
                "sha256": descriptor.sha256,
                "schema": descriptor.schema,
                "manifest_generation": manifest.generation,
            }
        )
    return {
        "enabled": bool(settings.adopt_enabled),
        "declared": list(declared),
        "eligible": sorted(eligible, key=lambda item: item["path"]),
        "rejected": sorted(rejected, key=lambda item: item["path"]),
    }


def check_consistency(
    root: Path,
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
                "adoption": {
                    "enabled": bool(policy.settings(name).adopt_enabled),
                    "declared": list(policy.candidates_for(name)),
                    "eligible": [],
                    "rejected": [],
                },
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
        adoption = _adoption(
            root, table, manifest, snapshots.get(name, []), policy, disk
        )
        for item in adoption["rejected"]:
            diagnostics.append(
                make_issue(
                    "adopt_candidate_rejected", "warning", name,
                    disk[item["path"]]["partition"] if item["path"] in disk else "",
                    f"adopt candidate {item['path']} rejected ({item['code']}): "
                    f"{item['reason']}",
                    files=[item["path"]], code=item["code"],
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
            "adoption": adoption,
        }
    diagnostics.sort(key=lambda item: (item["category"], item["table"], item["reason"]))
    return result, diagnostics
