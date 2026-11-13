"""Deterministic, in-memory replay of a saved maintenance plan.

The engine walks the plan in its stored, deterministic order and mirrors each
executor's decision rules against a mutable projection of the lake:

* compaction reproduces the input verification, byte-parity output budget,
  manifest rewrite and archived snapshot of ``executor.compact``;
* repair reproduces the "drop only entries whose file is still absent" rule of
  ``executor.repair``;
* vacuum reproduces the expired-snapshot removal and the second reference
  check before data deletion of ``executor.vacuum``.

Every mismatch between the plan and the current directory becomes a structured
risk instead of an exception, so a single stale input never aborts the whole
replay.  A failed action changes no projected state; later actions still run,
which is what makes ordering and cross-action dependencies visible.
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

from ..catalog.manifest import ManifestFile
from ..planner.plan import validate_plan
from ..utils import LakewatchError, digest, human_bytes, parse_utc
from .state import FileState, LakeState, SnapshotState, TableState, load_lake_state

SIMULATION_FORMAT = "lakewatch.simulation/1"
SCHEMA_VERSION = 1

# Stable counter vocabulary.  Every before/after block, per action or
# aggregate, uses exactly these fields in this order.
COUNTER_FIELDS = (
    "data_file_count",
    "data_bytes",
    "manifest_entry_count",
    "manifest_bytes",
    "snapshot_count",
    "snapshot_bytes",
    "total_bytes",
    "missing_count",
    "orphan_count",
)

# Scan-summary fields the projection can derive exactly, under the same names
# ``ScanResult.summary`` uses, so acceptance can diff against ledger reports.
SCAN_COMPARISON_FIELDS = (
    "table_count",
    "partition_count",
    "file_count",
    "total_bytes",
    "small_file_count",
    "orphan_count",
    "missing_count",
    "expired_snapshot_count",
    "releasable_bytes",
)

# Risk codes are part of the stable JSON contract; reuse them verbatim in
# downstream tooling rather than matching on human-readable messages.
SEVERITY_ORDER = {"error": 0, "warning": 1, "info": 2}


class SimulationError(LakewatchError):
    """Raised when a simulation cannot be started from the given inputs."""


def _risk(
    code: str,
    severity: str,
    action: dict | None,
    message: str,
    *,
    path: str | None = None,
    expected: Any = None,
    actual: Any = None,
) -> dict:
    return {
        "code": code,
        "severity": severity,
        "action_id": action["action_id"] if action else None,
        "kind": action["kind"] if action else None,
        "table": action["table"] if action else None,
        "path": path,
        "message": message,
        "expected": expected,
        "actual": actual,
    }


def table_counters(table: TableState) -> dict[str, int]:
    """Project the nine stable counters for one table from virtual state."""
    data_bytes = table.total_data_bytes
    snapshot_bytes = table.snapshot_bytes
    return {
        "data_file_count": len(table.files),
        "data_bytes": data_bytes,
        "manifest_entry_count": len(table.manifest_files),
        "manifest_bytes": sum(item.bytes for item in table.manifest_files.values()),
        "snapshot_count": len(table.snapshots),
        "snapshot_bytes": snapshot_bytes,
        "total_bytes": data_bytes + snapshot_bytes,
        "missing_count": len(table.missing_paths()),
        "orphan_count": len(table.orphan_paths()),
    }


def aggregate_counters(state: LakeState) -> dict[str, int]:
    totals = {field: 0 for field in COUNTER_FIELDS}
    for name in state.table_order:
        counters = table_counters(state.tables[name])
        for field in COUNTER_FIELDS:
            totals[field] += counters[field]
    return totals


def _delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {field: after[field] - before[field] for field in COUNTER_FIELDS}


def _project_snapshot(table: TableState, created_at: str) -> SnapshotState:
    """Mirror write_snapshot: next sequence, files ordered by path, unknown size."""
    sequence = table.next_sequence()
    snapshot_id = f"snap-{sequence:04d}"
    entries = tuple(
        sorted(table.manifest_files.values(), key=lambda item: item.path)
    )
    return SnapshotState(
        snapshot_id=snapshot_id,
        sequence=sequence,
        created_at=created_at,
        files=entries,
        bytes=0,
        projected=True,
    )


def _has_error(action_risks: list[dict]) -> bool:
    return any(item["severity"] == "error" for item in action_risks)


def _simulate_compact(
    table: TableState, action: dict, created_at: str
) -> tuple[str, list[dict], dict]:
    risks: list[dict] = []
    if not table.manifest_available:
        risks.append(_risk(
            "manifest_unavailable", "error", action,
            "table has no current manifest; the executor would refuse to compact",
        ))
        return "would_fail", risks, {"applied": False}

    verified: list[tuple[dict, FileState]] = []
    for item in action["inputs"]:
        rel = item["path"]
        disk = table.files.get(rel)
        if disk is None:
            if rel in table.consumed:
                risks.append(_risk(
                    "input_already_consumed", "error", action,
                    "input was already removed by an earlier action",
                    path=rel, expected=item["bytes"], actual=None,
                ))
            else:
                risks.append(_risk(
                    "input_missing", "error", action,
                    "planned input file is absent from the current directory",
                    path=rel, expected=item["bytes"], actual=None,
                ))
            continue
        if disk.projected:
            risks.append(_risk(
                "input_is_projected", "error", action,
                "input only exists as the output of an earlier simulated action",
                path=rel,
            ))
        if disk.bytes != item["bytes"]:
            risks.append(_risk(
                "input_bytes_mismatch", "error", action,
                "input byte size no longer matches the plan",
                path=rel, expected=item["bytes"], actual=disk.bytes,
            ))
        actual_sha = table.sha_of(rel)
        if item.get("sha256") and actual_sha and item["sha256"] != actual_sha:
            risks.append(_risk(
                "input_hash_mismatch", "error", action,
                "input content SHA-256 no longer matches the plan",
                path=rel, expected=item["sha256"], actual=actual_sha,
            ))
        if rel not in table.manifest_files:
            risks.append(_risk(
                "input_not_in_manifest", "error", action,
                "input is not listed by the current manifest",
                path=rel,
            ))
        verified.append((item, disk))

    output_rel = action["expected"]["output_path"]
    if output_rel in table.files:
        risks.append(_risk(
            "output_already_exists", "error", action,
            "compaction output path already exists; the executor would refuse",
            path=output_rel,
        ))

    if _has_error(risks):
        return "would_fail", risks, {"applied": False}

    # Byte parity: the executor pads the rewrite to the sum of the verified
    # input sizes, so the projection budgets exactly the current on-disk bytes.
    total_bytes = sum(disk.bytes for _, disk in verified)
    total_rows = sum(item.get("rows", 0) for item, _ in verified)
    input_paths = [item["path"] for item, _ in verified]

    for rel in input_paths:
        table.files.pop(rel, None)
        table.manifest_files.pop(rel, None)
        table.consumed.add(rel)
    output = FileState(
        path=output_rel, bytes=total_bytes, rows=total_rows,
        sha256=None, projected=True,
    )
    table.files[output_rel] = output
    table.manifest_files[output_rel] = ManifestFile(
        path=output_rel, bytes=total_bytes, rows=total_rows, sha256=None
    )
    table.manifest_generation += 1
    snapshot = _project_snapshot(table, created_at)
    table.snapshots[snapshot.snapshot_id] = snapshot
    table.snapshot_order.append(snapshot.snapshot_id)

    projection = {
        "applied": True,
        "output_path": output_rel,
        "output_bytes": total_bytes,
        "output_rows": total_rows,
        "deleted_inputs": input_paths,
        "bytes_in": total_bytes,
        "bytes_out": total_bytes,
        "manifest_generation": table.manifest_generation,
        "snapshot_id": snapshot.snapshot_id,
        "released_bytes": 0,
    }
    return "would_apply", risks, projection


def _simulate_repair(
    table: TableState, action: dict, created_at: str
) -> tuple[str, list[dict], dict]:
    risks: list[dict] = []
    if not table.manifest_available:
        risks.append(_risk(
            "manifest_unavailable", "error", action,
            "table has no current manifest; the executor would refuse to repair",
        ))
        return "would_fail", risks, {"applied": False}

    requested = list(action["expected"].get("drop_paths", []))
    drop: list[str] = []
    for rel in requested:
        if rel not in table.manifest_files:
            # Already pruned (e.g. half-applied plan) or never listed.
            risks.append(_risk(
                "repair_path_not_in_manifest", "info", action,
                "planned manifest entry is no longer present; nothing to prune",
                path=rel,
            ))
            continue
        if rel in table.files:
            # execute_repair explicitly keeps entries whose file reappeared.
            risks.append(_risk(
                "repair_path_reappeared", "info", action,
                "file exists again; the executor would keep this manifest entry",
                path=rel,
            ))
            continue
        drop.append(rel)

    if not drop:
        return "would_noop", risks, {
            "applied": False,
            "dropped_entries": [],
            "manifest_generation": table.manifest_generation,
            "released_bytes": 0,
        }

    for rel in drop:
        table.manifest_files.pop(rel, None)
    table.manifest_generation += 1
    snapshot = _project_snapshot(table, created_at)
    table.snapshots[snapshot.snapshot_id] = snapshot
    table.snapshot_order.append(snapshot.snapshot_id)
    return "would_apply", risks, {
        "applied": True,
        "dropped_entries": sorted(drop),
        "manifest_generation": table.manifest_generation,
        "snapshot_id": snapshot.snapshot_id,
        "released_bytes": 0,
    }


def _simulate_vacuum(
    table: TableState, action: dict
) -> tuple[str, list[dict], dict]:
    risks: list[dict] = []
    if not table.manifest_available:
        risks.append(_risk(
            "manifest_unavailable", "warning", action,
            "table has no current manifest; reference protection falls back to "
            "remaining snapshots only",
        ))

    # Re-verify the snapshot JSON inputs recorded on the plan.
    for item in action["inputs"]:
        snapshot_id = item.get("snapshot_id")
        snap = table.snapshots.get(snapshot_id) if snapshot_id else None
        if snap is None:
            risks.append(_risk(
                "snapshot_missing", "info", action,
                "planned expired snapshot is already gone",
                path=item.get("path"), expected=snapshot_id, actual=None,
            ))
        elif snap.bytes != item.get("bytes"):
            risks.append(_risk(
                "input_bytes_mismatch", "warning", action,
                "snapshot JSON byte size no longer matches the plan",
                path=item.get("path"),
                expected=item.get("bytes"), actual=snap.bytes,
            ))

    removed_snapshots: list[str] = []
    snapshot_bytes = 0
    for snapshot_id in action["expected"].get("delete_snapshot_ids", []):
        snap = table.snapshots.get(snapshot_id)
        if snap is None:
            risks.append(_risk(
                "snapshot_missing", "info", action,
                "snapshot targeted for expiry is already gone",
                path=f"_metadata/snapshots/{snapshot_id}.json",
                expected=snapshot_id, actual=None,
            ))
            continue
        if not snap.projected:
            snapshot_bytes += snap.bytes
        table.snapshots.pop(snapshot_id, None)
        table.snapshot_order.remove(snapshot_id)
        removed_snapshots.append(snapshot_id)

    # Mirror execute_vacuum's second safety check on the remaining state.
    remaining_paths: set[str] = set(table.manifest_paths)
    for snap in table.ordered_snapshots():
        remaining_paths.update(snap.paths)

    removed_data: list[str] = []
    data_bytes = 0
    for rel in action["expected"].get("delete_data_files", []):
        if rel in remaining_paths:
            risks.append(_risk(
                "vacuum_data_file_protected", "info", action,
                "file is still referenced by the manifest or a retained "
                "snapshot; the executor would skip it",
                path=rel,
            ))
            continue
        disk = table.files.get(rel)
        if disk is None:
            if rel in table.consumed:
                risks.append(_risk(
                    "input_already_consumed", "info", action,
                    "data file was already removed by an earlier action",
                    path=rel,
                ))
            else:
                risks.append(_risk(
                    "input_missing", "info", action,
                    "data file targeted by vacuum is absent from disk",
                    path=rel,
                ))
            continue
        if not disk.projected:
            data_bytes += disk.bytes
        table.files.pop(rel, None)
        table.consumed.add(rel)
        removed_data.append(rel)

    status = "would_apply" if removed_snapshots or removed_data else "would_noop"
    return status, risks, {
        "applied": status == "would_apply",
        "deleted_snapshots": sorted(removed_snapshots),
        "deleted_data_files": sorted(removed_data),
        "snapshot_bytes": snapshot_bytes,
        "data_bytes": data_bytes,
        "released_bytes": snapshot_bytes + data_bytes,
        "remaining_snapshots": len(table.snapshots),
    }


HANDLERS = {
    "compact": _simulate_compact,
    "repair": _simulate_repair,
    "vacuum": _simulate_vacuum,
}


def _partition_count(table: TableState) -> int:
    partitions = {
        str(Path(rel).parent) if str(Path(rel).parent) != "." else ""
        for rel in table.files
    }
    return len(partitions)


def _retention_projection(state: LakeState, policy: Any) -> dict[str, int]:
    """Recompute the scanner's retention counters on the projected state."""
    expired_total = 0
    releasable = 0
    for name in state.table_order:
        table = state.tables[name]
        settings = policy.settings(name)
        cutoff = policy.as_of - timedelta(days=settings.keep_days)
        ordered = table.ordered_snapshots()
        protected_by_count = {
            snap.snapshot_id
            for snap in ordered[-settings.keep_last_snapshots:]
        } if settings.keep_last_snapshots else set()
        expired = [
            snap for snap in ordered
            if snap.snapshot_id not in protected_by_count
            and parse_utc(snap.created_at) < cutoff
        ]
        expired_total += len(expired)
        protected_paths = set(table.manifest_paths)
        for snap in ordered:
            if snap not in expired:
                protected_paths.update(snap.paths)
        expired_paths: set[str] = set()
        for snap in expired:
            expired_paths.update(snap.paths)
        unique = sorted(expired_paths - protected_paths)
        releasable += sum(
            snap.bytes for snap in expired if not snap.projected
        )
        releasable += sum(
            table.files[rel].bytes
            for rel in unique
            if rel in table.files and not table.files[rel].projected
        )
    return {
        "expired_snapshot_count": expired_total,
        "releasable_bytes": releasable,
    }


def _scan_comparison(
    state: LakeState, policy: Any | None
) -> dict[str, int] | None:
    if policy is None:
        return None
    retention = _retention_projection(state, policy)
    block = {
        "table_count": len(state.table_order),
        "partition_count": sum(
            _partition_count(state.tables[name]) for name in state.table_order
        ),
        "file_count": sum(
            len(state.tables[name].files) for name in state.table_order
        ),
        "total_bytes": sum(
            state.tables[name].total_data_bytes for name in state.table_order
        ),
        "small_file_count": 0,
        "orphan_count": 0,
        "missing_count": 0,
        "expired_snapshot_count": retention["expired_snapshot_count"],
        "releasable_bytes": retention["releasable_bytes"],
    }
    for name in state.table_order:
        table = state.tables[name]
        threshold = policy.settings(name).small_file_bytes
        block["small_file_count"] += sum(
            1 for item in table.files.values() if item.bytes < threshold
        )
        block["orphan_count"] += len(table.orphan_paths())
        block["missing_count"] += len(table.missing_paths())
    return {field: block[field] for field in SCAN_COMPARISON_FIELDS}


def _fingerprint(state: LakeState, plan: dict) -> str:
    material = {
        "plan_hash": plan["plan_hash"],
        "lake_root": str(state.root),
        "tables": [
            {
                "name": name,
                "generation": state.tables[name].manifest_generation,
                "files": [
                    [rel, state.tables[name].files[rel].bytes]
                    for rel in sorted(state.tables[name].files)
                ],
                "manifest": [
                    [rel, entry.bytes]
                    for rel, entry in sorted(
                        state.tables[name].manifest_files.items()
                    )
                ],
                "snapshots": [
                    [snap.snapshot_id, snap.sequence, snap.bytes]
                    for snap in state.tables[name].ordered_snapshots()
                ],
            }
            for name in state.table_order
        ],
    }
    return digest(material)


def simulate_plan(
    root: Path | str,
    plan: dict,
    *,
    policy: Any | None = None,
    plan_file: Path | str | None = None,
) -> dict:
    """Replay ``plan`` against the lake at ``root`` without touching disk.

    ``policy`` is optional; when supplied it enables the scan-summary-shaped
    comparison block and a plan/policy hash drift check.
    """
    validate_plan(plan)
    state = load_lake_state(root)
    all_risks: list[dict] = []

    if policy is not None and policy.policy_hash != plan.get("policy_hash"):
        all_risks.append(_risk(
            "plan_policy_mismatch", "warning", None,
            "plan was built under a different policy than the configured one",
            expected=plan.get("policy_hash"),
            actual=policy.policy_hash,
        ))

    before_counters = aggregate_counters(state)
    before_scan = _scan_comparison(state, policy)
    action_records: list[dict] = []
    released_bytes = 0
    projected_snapshots = 0
    status_counts = {"would_apply": 0, "would_noop": 0, "would_fail": 0}

    for order_index, action in enumerate(plan["actions"]):
        table = state.tables.get(action["table"])
        action_risks: list[dict] = []
        if table is None:
            action_risks.append(_risk(
                "table_missing", "error", action,
                "plan references a table that does not exist in the lake",
            ))
            status, projection = "would_fail", {"applied": False}
        else:
            before = table_counters(table)
            handler = HANDLERS.get(action["kind"])
            if handler is None:
                action_risks.append(_risk(
                    "unknown_action", "error", action,
                    "unsupported action kind",
                ))
                status, projection = "would_fail", {"applied": False}
            elif action["kind"] == "vacuum":
                status, handler_risks, projection = handler(table, action)
                action_risks.extend(handler_risks)
            else:
                status, handler_risks, projection = handler(
                    table, action, plan["as_of"]
                )
                action_risks.extend(handler_risks)
            after = table_counters(table) if table is not None else before

        if table is not None:
            per_action_before = before
            per_action_after = after
        else:
            empty = {field: 0 for field in COUNTER_FIELDS}
            per_action_before = per_action_after = empty

        released_bytes += int(projection.get("released_bytes", 0))
        if action["kind"] in ("compact", "repair") and projection.get("snapshot_id"):
            projected_snapshots += 1
        status_counts[status] += 1
        all_risks.extend(action_risks)
        action_records.append({
            "order_index": order_index,
            "action_id": action["action_id"],
            "kind": action["kind"],
            "table": action["table"],
            "partition": action.get("partition", ""),
            "status": status,
            "risks": action_risks,
            "before": per_action_before,
            "after": per_action_after,
            "delta": _delta(per_action_before, per_action_after),
            "projection": projection,
        })

    after_counters = aggregate_counters(state)
    after_scan = _scan_comparison(state, policy)
    all_risks.sort(
        key=lambda item: (
            SEVERITY_ORDER.get(item["severity"], 9),
            item["action_id"] or "",
            item["code"],
            item["path"] or "",
        )
    )
    risks_by_severity = {"error": 0, "warning": 0, "info": 0}
    for item in all_risks:
        risks_by_severity[item["severity"]] = (
            risks_by_severity.get(item["severity"], 0) + 1
        )

    compact_records = [
        item for item in action_records if item["kind"] == "compact"
    ]
    repair_records = [
        item for item in action_records if item["kind"] == "repair"
    ]
    vacuum_records = [
        item for item in action_records if item["kind"] == "vacuum"
    ]
    projected_outcomes = {
        "compactions": sum(
            1 for item in compact_records if item["status"] == "would_apply"
        ),
        "compaction_input_files_removed": sum(
            len(item["projection"].get("deleted_inputs", []))
            for item in compact_records
        ),
        "compaction_output_files": sum(
            1 for item in compact_records
            if item["projection"].get("output_path")
        ),
        "manifest_entries_dropped": sum(
            len(item["projection"].get("dropped_entries", []))
            for item in repair_records
        ),
        "snapshots_removed": sum(
            len(item["projection"].get("deleted_snapshots", []))
            for item in vacuum_records
        ),
        "vacuumed_data_files": sum(
            len(item["projection"].get("deleted_data_files", []))
            for item in vacuum_records
        ),
        "snapshots_projected": projected_snapshots,
        "released_bytes": released_bytes,
        "released_human": human_bytes(released_bytes),
    }

    simulation_id = "sim-" + _fingerprint(state, plan)[:16]
    return {
        "format": SIMULATION_FORMAT,
        "schema_version": SCHEMA_VERSION,
        "simulation_id": simulation_id,
        "plan_hash": plan["plan_hash"],
        "plan_file": str(plan_file) if plan_file else None,
        "lake_root": str(state.root),
        "as_of": plan["as_of"],
        "policy_hash": plan.get("policy_hash"),
        "summary": {
            "action_count": len(plan["actions"]),
            "status_counts": status_counts,
            "would_apply": status_counts["would_apply"],
            "would_noop": status_counts["would_noop"],
            "would_fail": status_counts["would_fail"],
            "risk_count": len(all_risks),
            "risks_by_severity": risks_by_severity,
            "state_drift": risks_by_severity["error"] > 0
            or risks_by_severity["warning"] > 0,
            "before": before_counters,
            "after": after_counters,
            "delta": _delta(before_counters, after_counters),
            "projected": projected_outcomes,
        },
        "scan_comparison": {
            "before": before_scan,
            "after": after_scan,
        },
        "actions": action_records,
        "risks": all_risks,
    }


__all__ = [
    "COUNTER_FIELDS",
    "SCAN_COMPARISON_FIELDS",
    "SIMULATION_FORMAT",
    "SCHEMA_VERSION",
    "SimulationError",
    "simulate_plan",
    "table_counters",
]
