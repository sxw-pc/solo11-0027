"""Turn a read-only scan into a deterministic, auditable maintenance plan."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..scanners import ScanResult
from ..utils import LakewatchError, digest, json_text
from .policy import Policy

PLAN_FORMAT = "lakewatch.plan/1"
KIND_ORDER = {"repair": 0, "compact": 1, "vacuum": 2}


class PlanError(LakewatchError):
    """Raised when a plan file is malformed or its hash does not verify."""


def _input_hash(inputs: list[dict]) -> str:
    return digest(
        [
            {
                "path": item["path"],
                "bytes": item["bytes"],
                "rows": item.get("rows", 0),
                "sha256": item.get("sha256"),
            }
            for item in inputs
        ]
    )


def _finish(kind: str, table: str, partition: str, reason: str,
            inputs: list[dict], expected: dict) -> dict:
    core = {
        "kind": kind,
        "table": table,
        "partition": partition,
        "reason": reason,
        "inputs": inputs,
        "expected": expected,
        "input_hash": _input_hash(inputs),
    }
    core["action_id"] = f"{kind}-{digest(core)[:16]}"
    return core


def _groups(files: list[dict], target_bytes: int, max_inputs: int) -> list[list[dict]]:
    groups: list[list[dict]] = []
    current: list[dict] = []
    total = 0
    for item in sorted(files, key=lambda value: value["path"]):
        if current and (
            len(current) >= max_inputs
            or (total + item["bytes"] > target_bytes and len(current) >= 2)
        ):
            groups.append(current)
            current, total = [], 0
        current.append(item)
        total += item["bytes"]
    if current:
        groups.append(current)
    return groups


def _compaction_actions(table: dict, policy: Policy, manifest_paths: set[str]) -> list[dict]:
    settings = policy.settings(table["name"])
    if not settings.enabled or not settings.compact_small_files:
        return []
    actions = []
    files_by_path = {item["path"]: item for item in table["files"]}
    for partition in table["partitions"]:
        if not partition["valid"]:
            continue
        small = [
            files_by_path[path]
            for path in partition["files"]
            if path in manifest_paths
            and files_by_path[path]["bytes"] < settings.small_file_bytes
        ]
        for group in _groups(
            small, settings.compact_target_bytes, settings.compact_max_inputs
        ):
            if len(group) < 2:
                continue
            inputs = [
                {
                    "path": item["path"],
                    "bytes": item["bytes"],
                    "rows": item["rows"],
                    "sha256": item["sha256"],
                }
                for item in group
            ]
            fingerprint = digest(
                {"table": table["name"], "partition": partition["name"],
                 "inputs": [(item["path"], item["sha256"]) for item in inputs]}
            )[:12]
            output_name = f"part-compact-{fingerprint}.parquet"
            output_path = (
                f"{partition['name']}/{output_name}"
                if partition["name"] else output_name
            )
            total_bytes = sum(item["bytes"] for item in inputs)
            expected = {
                "output_path": output_path,
                "bytes": total_bytes,
                "rows": sum(item["rows"] for item in inputs),
                "file_count": 1,
            }
            reason = (
                f"merge {len(inputs)} small files ({total_bytes} bytes) below "
                f"{settings.small_file_bytes}-byte threshold"
            )
            actions.append(
                _finish(
                    "compact", table["name"], partition["name"], reason,
                    inputs, expected
                )
            )
    return actions


def _repair_actions(scan: ScanResult, policy: Policy) -> list[dict]:
    actions = []
    for table in scan.tables:
        name = table["name"]
        settings = policy.settings(name)
        status = scan.consistency.get(name, {})
        missing = status.get("missing", [])
        if not missing or not settings.enabled or not settings.repair_missing:
            continue
        inputs = [
            {
                "path": item["path"],
                "bytes": item["bytes"],
                "rows": item["rows"],
                "sha256": item.get("sha256"),
            }
            for item in missing
        ]
        expected = {
            "generation": (table.get("manifest_generation") or 0) + 1,
            "drop_paths": [item["path"] for item in inputs],
            "entry_count_after": max(0, len(scan.manifests[name].files) - len(inputs)),
        }
        reason = f"prune {len(inputs)} manifest entries whose files are missing"
        actions.append(_finish("repair", name, "", reason, inputs, expected))
    return actions


def _vacuum_actions(scan: ScanResult, policy: Policy) -> list[dict]:
    actions = []
    for table in scan.tables:
        name = table["name"]
        settings = policy.settings(name)
        report = scan.snapshot_report.get("tables", {}).get(name, {})
        expired = report.get("expired", [])
        if not expired or not settings.enabled or not settings.vacuum_expired:
            continue
        inputs = [
            {
                "path": f"_metadata/snapshots/{item['snapshot_id']}.json",
                "bytes": item["snapshot_bytes"],
                "rows": 0,
                "sha256": None,
                "snapshot_id": item["snapshot_id"],
                "sequence": item["sequence"],
                "created_at": item["created_at"],
            }
            for item in expired
        ]
        expected = {
            "delete_snapshot_ids": [item["snapshot_id"] for item in expired],
            "delete_data_files": [
                item["path"] for item in report.get("unique_data_files", [])
            ],
            "affected_partitions": report.get("affected_partitions", []),
            "released_bytes": report.get("released_bytes", 0),
        }
        reason = (
            f"expire {len(expired)} snapshot(s) outside the retention window; "
            f"free {report.get('released_bytes', 0)} bytes"
        )
        actions.append(_finish("vacuum", name, "", reason, inputs, expected))
    return actions


def hash_payload(plan: dict) -> dict:
    return {
        "format": plan["format"],
        "policy_hash": plan["policy_hash"],
        "as_of": plan["as_of"],
        "input_digest": plan["input_digest"],
        "actions": plan["actions"],
    }


def compute_plan_hash(plan: dict) -> str:
    return digest(hash_payload(plan))


def build_plan(scan: ScanResult, policy: Policy, max_actions: int | None = None) -> dict:
    candidates = []
    for table in scan.tables:
        manifest = scan.manifests.get(table["name"])
        if manifest is not None:
            candidates.extend(
                _compaction_actions(table, policy, manifest.paths)
            )
    candidates.extend(_repair_actions(scan, policy))
    candidates.extend(_vacuum_actions(scan, policy))
    candidates.sort(
        key=lambda item: (
            KIND_ORDER[item["kind"]], item["table"], item["partition"],
            item["action_id"],
        )
    )
    limit = policy.max_actions if max_actions is None else max(0, max_actions)
    actions = candidates[:limit]
    plan = {
        "format": PLAN_FORMAT,
        "policy_hash": policy.policy_hash,
        "as_of": policy.as_of_iso,
        "input_digest": scan.input_digest(),
        "actions": actions,
        "summary": _summary(actions, candidates, limit),
    }
    plan["plan_hash"] = compute_plan_hash(plan)
    return plan


def _summary(actions: list[dict], candidates: list[dict], limit: int) -> dict:
    by_kind = {kind: 0 for kind in KIND_ORDER}
    for item in actions:
        by_kind[item["kind"]] += 1
    return {
        "action_count": len(actions),
        "total_candidate_actions": len(candidates),
        "deferred_action_count": max(0, len(candidates) - limit),
        "by_kind": by_kind,
        "compaction_input_files": sum(
            len(item["inputs"]) for item in actions if item["kind"] == "compact"
        ),
        "expected_released_bytes": sum(
            item["expected"].get("released_bytes", 0)
            for item in actions
            if item["kind"] == "vacuum"
        ),
    }


def save_plan(plan: dict, path: Path | str) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json_text(plan), encoding="utf-8", newline="\n")
    return target


def validate_plan(plan: Any) -> dict:
    if not isinstance(plan, dict) or plan.get("format") != PLAN_FORMAT:
        raise PlanError(f"plan format must be {PLAN_FORMAT}")
    required = {"policy_hash", "as_of", "input_digest", "actions", "plan_hash"}
    missing = required - set(plan)
    if missing:
        raise PlanError(f"plan missing field(s): {', '.join(sorted(missing))}")
    if not isinstance(plan["actions"], list):
        raise PlanError("plan actions must be a list")
    for item in plan["actions"]:
        if not isinstance(item, dict) or item.get("kind") not in KIND_ORDER:
            raise PlanError("plan contains an invalid action")
        if "action_id" not in item or "inputs" not in item or "expected" not in item:
            raise PlanError("plan action is missing action_id/inputs/expected")
    actual = compute_plan_hash(plan)
    if actual != plan["plan_hash"]:
        raise PlanError("plan hash does not match plan contents")
    return plan


def load_plan(path: Path | str) -> dict:
    target = Path(path)
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PlanError(f"cannot read plan: {exc}") from exc
    return validate_plan(raw)


__all__ = [
    "PLAN_FORMAT", "PlanError", "build_plan", "compute_plan_hash", "load_plan",
    "save_plan", "validate_plan",
]
