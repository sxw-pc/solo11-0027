"""Worker process used by the multi-process claim tests.

It is a plain module (not a ``test_*`` file so pytest never collects it) and
is deliberately self-contained: spawned via multiprocessing with all inputs
passed as arguments, writing a JSON result to an output file.

Two environment knobs make races deterministic:

* ``LAKEWATCH_BLOCK_ACTION`` -- an action_id; the worker stalls inside that
  action on a per-worker file flag, long enough for the second worker to
  reach the same action and prove it waits rather than executes.
* ``LAKEWATCH_HOLD_FILE`` / ``LAKEWATCH_HOLD_MODE`` -- when set, the worker
  either freezes (SIGSTOP-style test kills the process) inside the action.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from lakewatch.executor import apply_plan
from lakewatch.executor.compact import execute_compaction
from lakewatch.executor.repair import execute_repair
from lakewatch.executor.vacuum import execute_vacuum
from lakewatch.ledger.db import Ledger
from lakewatch.planner.plan import load_plan

_ORIGINAL = {
    "compact": execute_compaction,
    "repair": execute_repair,
    "vacuum": execute_vacuum,
}


def _instrument() -> None:
    block_action = os.environ.get("LAKEWATCH_BLOCK_ACTION")
    hold_file = os.environ.get("LAKEWATCH_HOLD_FILE")
    hold_mode = os.environ.get("LAKEWATCH_HOLD_MODE", "wait")
    ready_dir = os.environ.get("LAKEWATCH_READY_DIR")

    import lakewatch.executor as executor_pkg

    def make_wrapper(kind):
        original = _ORIGINAL[kind]

        def wrapper(root, action):
            action_id = action["action_id"]
            if ready_dir:
                ready = Path(ready_dir)
                ready.mkdir(parents=True, exist_ok=True)
                (ready / f"{os.getpid()}.enter").write_text(action_id)
            if hold_file and action_id == block_action:
                if hold_mode == "wait":
                    deadline = time.time() + 120
                    while time.time() < deadline:
                        if not Path(hold_file).exists():
                            break
                        time.sleep(0.05)
                else:  # "freeze": block until killed
                    time.sleep(300)
            elif block_action and action_id == block_action:
                # Deterministic overlap: linger so the sibling reaches the
                # claim boundary while this holder is still inside the action.
                time.sleep(2.0)
            return original(root, action)

        return wrapper

    executor_pkg.EXECUTORS = {
        kind: make_wrapper(kind) for kind in _ORIGINAL
    }


def main() -> int:
    _instrument()
    lake = Path(sys.argv[1])
    plan_path = Path(sys.argv[2])
    ledger_path = Path(sys.argv[3])
    out_path = Path(sys.argv[4])
    ttl = float(os.environ.get("LAKEWATCH_TEST_TTL", "5"))
    wait = float(os.environ.get("LAKEWATCH_TEST_WAIT", "8"))
    poll = float(os.environ.get("LAKEWATCH_TEST_POLL", "0.1"))
    plan = load_plan(plan_path)
    ledger = Ledger(ledger_path)
    try:
        result = apply_plan(
            lake, plan, ledger,
            lease_ttl_seconds=ttl,
            wait_timeout_seconds=wait,
            poll_interval_seconds=poll,
        )
    except Exception as exc:  # noqa: BLE001 - surfaced via JSON to the test
        payload = {"error": f"{type(exc).__name__}: {exc}", "pid": os.getpid()}
    else:
        payload = {"result": _slim(result), "pid": os.getpid()}
    out_path.write_text(json.dumps(payload), encoding="utf-8")
    return 0


def _slim(result: dict) -> dict:
    return {
        "status": result["status"],
        "plan_hash": result["plan_hash"],
        "owner_id": result.get("owner_id"),
        "completed_actions": result["completed_actions"],
        "skipped_actions": result["skipped_actions"],
        "actions": [
            {
                "action_id": item["action_id"],
                "kind": item["kind"],
                "status": item["status"],
                "skip_reason": item.get("skip_reason"),
                "claimed_by": item.get("claimed_by"),
                "owner_id": item.get("owner_id"),
            }
            for item in result["actions"]
        ],
    }


if __name__ == "__main__":
    raise SystemExit(main())
