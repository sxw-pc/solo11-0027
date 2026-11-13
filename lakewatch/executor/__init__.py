"""Execute a maintenance plan once, recording every action in SQLite.

Concurrency is handled by per-action claims in the ledger (see
:mod:`lakewatch.ledger.claims`).  Each action is atomically claimed before
its executor runs, so two overlapping ``apply`` processes can never execute
the same action twice, and a crashed holder's lease is taken over once it
expires.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from ..ledger import (
    Ledger,
    acquire_claim,
    lease_settings,
    new_owner_id,
)
from ..planner.plan import validate_plan
from ..utils import LakewatchError, digest
from .compact import execute_compaction
from .repair import execute_repair
from .vacuum import execute_vacuum

EXECUTORS = {
    "compact": execute_compaction,
    "repair": execute_repair,
    "vacuum": execute_vacuum,
}

# Outcome statuses.
COMPLETED = "completed"
SKIPPED = "skipped"

# Why an action was skipped.  ``already_done`` is the historical idempotent
# resume path; the claim-related reasons make concurrent execution visible.
REASON_ALREADY_DONE = "already_completed"
REASON_CLAIM_HELD = "claim_held_by_other"
REASON_COMPLETED_WHILE_WAITING = "completed_by_other_while_waiting"


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


def _skipped_outcome(action: dict, reason: str, *, output_hash=None,
                     result=None, holder: str | None = None) -> dict:
    item = {
        "action_id": action["action_id"],
        "kind": action["kind"],
        "table": action["table"],
        "partition": action.get("partition", ""),
        "status": SKIPPED,
        "skip_reason": reason,
        "output_hash": output_hash,
        "result": result,
    }
    if holder is not None:
        item["claimed_by"] = holder
    return item


def apply_plan(
    root: Path | str,
    plan: dict,
    ledger: Ledger,
    *,
    before: Any = None,
    policy: Any = None,
    reports_dir: Path | str | None = None,
    lease_ttl_seconds: float | None = None,
    wait_timeout_seconds: float | None = None,
    poll_interval_seconds: float | None = None,
    owner_id: str | None = None,
) -> dict:
    """Apply the plan.  The second call with the same plan is a pure no-op."""
    validate_plan(plan)
    root_path = Path(root).resolve()
    ttl, wait_timeout, poll_interval = lease_settings(
        lease_ttl_seconds, wait_timeout_seconds, poll_interval_seconds
    )
    holder_id = owner_id or new_owner_id()
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
                "status": SKIPPED,
                "skip_reason": REASON_ALREADY_DONE,
                "output_hash": tasks.get(action["action_id"], {}).get("output_hash"),
                "result": _json(tasks.get(action["action_id"], {}).get("result_json")),
            }
            for action in plan["actions"]
        ]
        return {
            "status": "already_completed",
            "plan_hash": plan["plan_hash"],
            "owner_id": holder_id,
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
    concurrent_skips = 0
    try:
        for action in plan["actions"]:
            existing = ledger.get_task(plan["plan_hash"], action["action_id"])
            if existing and existing["status"] == "completed":
                skipped += 1
                outcomes.append(
                    _skipped_outcome(
                        action, REASON_ALREADY_DONE,
                        output_hash=existing.get("output_hash"),
                        result=_json(existing.get("result_json")),
                    )
                )
                continue

            lease, holder_row = acquire_claim(
                ledger, plan["plan_hash"], action, holder_id,
                ttl_seconds=ttl, wait_timeout_seconds=wait_timeout,
                poll_interval_seconds=poll_interval,
            )
            if lease is None:
                # A live holder still owns the action after the wait budget.
                skipped += 1
                concurrent_skips += 1
                holder = holder_row.get("owner_id") if holder_row else None
                outcomes.append(
                    _skipped_outcome(
                        action, REASON_CLAIM_HELD, holder=holder
                    )
                )
                continue

            # Another process may have finished the action while we were
            # waiting for its lease; re-check before touching anything.
            current = ledger.get_task(plan["plan_hash"], action["action_id"])
            if current and current["status"] == "completed":
                skipped += 1
                outcomes.append(
                    _skipped_outcome(
                        action, REASON_COMPLETED_WHILE_WAITING,
                        output_hash=current.get("output_hash"),
                        result=_json(current.get("result_json")),
                    )
                )
                lease.close()
                continue

            task_id = ledger.start_task(plan["plan_hash"], action)
            started = time.perf_counter()
            try:
                with lease:
                    result = EXECUTORS[action["kind"]](root_path, action)
                    if lease.lost:
                        # Our lease was stolen after stalled heartbeats; never
                        # overwrite the new owner's task record or side effects.
                        raise ApplyError(
                            f"action {action['action_id']} lost its claim lease "
                            "to another process; refusing to record its result"
                        )
            except ApplyError:
                elapsed = (time.perf_counter() - started) * 1000
                if lease.lost:
                    # Another owner now holds the claim; do not touch its
                    # task record.  The action may need a later re-run, which
                    # the non-completed task status already makes possible.
                    ledger.finish_plan(
                        plan["plan_hash"], "failed", completed, skipped,
                        before_summary, None, None,
                    )
                else:
                    ledger.fail_task(task_id, "claim lease lost", elapsed)
                    ledger.finish_plan(
                        plan["plan_hash"], "failed", completed, skipped,
                        before_summary, None, None,
                    )
                raise
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
                    "status": COMPLETED,
                    "owner_id": holder_id,
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

    # If a concurrent process kept some actions, do not pretend the plan is
    # finished: a later re-run must keep executing rather than hit the
    # already_completed short-circuit.
    plan_status = "skipped_concurrent" if concurrent_skips else "completed"
    result = {
        "status": plan_status,
        "plan_hash": plan["plan_hash"],
        "owner_id": holder_id,
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
    if plan_status != "completed":
        ledger.finish_plan(
            plan["plan_hash"], plan_status, completed, skipped,
            before_summary, None, None,
        )
        return result

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
