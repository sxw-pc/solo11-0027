"""Execute a maintenance plan once, recording every action in SQLite."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from ..ledger.db import Ledger
from ..planner.plan import validate_plan
from ..utils import LakewatchError, digest
from .adopt import execute_adoption
from .compact import execute_compaction
from .repair import execute_repair
from .vacuum import execute_vacuum

EXECUTORS = {
    "adopt": execute_adoption,
    "compact": execute_compaction,
    "repair": execute_repair,
    "vacuum": execute_vacuum,
}


class ApplyError(LakewatchError):
    """Raised when execution fails after the plan was accepted."""


def _json(value: Any) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def _summary(value: Any) -> dict | None:
    if value is None:
        return None
    if hasattr(value, "summary"):
        return value.summary()
    return value


def apply_plan(
    root: Path | str,
    plan: dict,
    ledger: Ledger,
    *,
    before: Any = None,
    policy: Any = None,
    reports_dir: Path | str | None = None,
) -> dict:
    """Apply the plan.  The second call with the same plan is a pure no-op."""
    validate_plan(plan)
    root_path = Path(root).resolve()
    ledger.migrate()
    row = ledger.ensure_plan(plan)
    if row["status"] == "completed":
        tasks = {item["action_id"]: item for item in ledger.list_tasks(plan["plan_hash"])}
        prior_before = _json(row.get("before_json"))
        prior_after = _json(row.get("after_json"))
        prior_report = _json(row.get("report_json"))
        actions = [
            {
                "action_id": action["action_id"],
                "kind": action["kind"],
                "table": action["table"],
                "partition": action.get("partition", ""),
                "status": "skipped",
                "output_hash": tasks.get(action["action_id"], {}).get("output_hash"),
                "result": _json(tasks.get(action["action_id"], {}).get("result_json")),
            }
            for action in plan["actions"]
        ]
        return {
            "status": "already_completed",
            "plan_hash": plan["plan_hash"],
            "completed_actions": 0,
            "skipped_actions": len(plan["actions"]),
            "failed_actions": 0,
            "actions": actions,
            "before": prior_before if prior_before is not None else _summary(before),
            "after": prior_after,
            "report": prior_report,
            "report_files": None,
            "ledger_path": str(ledger.path),
        }

    ledger.set_plan_status(plan["plan_hash"], "running")
    before_summary = _summary(before)
    outcomes = []
    completed = 0
    skipped = 0
    try:
        for action in plan["actions"]:
            existing = ledger.get_task(plan["plan_hash"], action["action_id"])
            if existing and existing["status"] == "completed":
                skipped += 1
                outcomes.append(
                    {
                        "action_id": action["action_id"],
                        "kind": action["kind"],
                        "table": action["table"],
                        "partition": action.get("partition", ""),
                        "status": "skipped",
                        "output_hash": existing.get("output_hash"),
                        "result": _json(existing.get("result_json")),
                    }
                )
                continue
            task_id = ledger.start_task(plan["plan_hash"], action)
            started = time.perf_counter()
            try:
                result = EXECUTORS[action["kind"]](root_path, action)
            except Exception as exc:
                elapsed = (time.perf_counter() - started) * 1000
                ledger.fail_task(task_id, str(exc), elapsed)
                ledger.finish_plan(
                    plan["plan_hash"], "failed", completed, skipped,
                    before_summary, None, None,
                )
                raise ApplyError(
                    f"action {action['action_id']} failed: {exc}"
                ) from exc
            elapsed = (time.perf_counter() - started) * 1000
            output_hash = result.get("output_hash") or digest(result)
            ledger.complete_task(task_id, output_hash, elapsed, result)
            completed += 1
            outcomes.append(
                {
                    "action_id": action["action_id"],
                    "kind": action["kind"],
                    "table": action["table"],
                    "partition": action.get("partition", ""),
                    "status": "completed",
                    "output_hash": output_hash,
                    "duration_ms": round(elapsed, 3),
                    "result": result,
                }
            )
    except ApplyError:
        raise
    except Exception as exc:
        ledger.finish_plan(
            plan["plan_hash"], "failed", completed, skipped,
            before_summary, None, None,
        )
        raise ApplyError(str(exc)) from exc

    result = {
        "status": "completed",
        "plan_hash": plan["plan_hash"],
        "completed_actions": completed,
        "skipped_actions": skipped,
        "failed_actions": 0,
        "actions": outcomes,
        "before": before_summary,
        "after": None,
        "report": None,
        "report_files": None,
        "ledger_path": str(ledger.path),
    }
    after_scan = None
    if policy is not None:
        from ..scanners import scan_lake

        after_scan = scan_lake(root_path, policy)
        result["after"] = after_scan.summary()
    if reports_dir is not None and after_scan is not None:
        from ..reports.render import build_report, write_report

        report = build_report(before, after_scan, result)
        files = write_report(report, Path(reports_dir))
        report_id = files["report_id"]
        result["report"] = report
        result["report_files"] = files
        ledger.record_report(
            report_id, plan["plan_hash"], files["json_path"],
            files["markdown_path"], report,
        )
    ledger.finish_plan(
        plan["plan_hash"], "completed", completed, skipped,
        result.get("before"), result.get("after"), result.get("report"),
    )
    return result


__all__ = ["ApplyError", "apply_plan"]
