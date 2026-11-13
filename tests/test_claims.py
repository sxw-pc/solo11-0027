"""Tests for execution-time per-action claims and concurrent apply safety."""
from __future__ import annotations

import json
import multiprocessing as mp
import os
import sqlite3
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import pytest

from conftest import tree_signature, write_cli_config

from lakewatch.cli import EXIT_BUSY, main
from lakewatch.executor import apply_plan
from lakewatch.executor import (
    REASON_ALREADY_DONE,
    REASON_CLAIM_HELD,
    REASON_COMPLETED_WHILE_WAITING,
)
from lakewatch.ledger.claims import acquire_claim, new_owner_id
from lakewatch.ledger.db import Ledger, LedgerBusyError, SCHEMA_VERSION
from lakewatch.planner.plan import build_plan, compute_plan_hash, save_plan
from lakewatch.scanners import scan_lake
from lakewatch.utils import iso_z, utc_now

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKER = REPO_ROOT / "tests" / "_claim_worker.py"


# --------------------------------------------------------------------------
# Schema migration
# --------------------------------------------------------------------------
def test_migrate_v1_database_adds_claims_smoothly(tmp_path):
    db_path = tmp_path / "old" / "ledger.sqlite3"
    db_path.parent.mkdir(parents=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE plans (
          plan_hash TEXT PRIMARY KEY, format TEXT NOT NULL, status TEXT NOT NULL,
          action_count INTEGER NOT NULL DEFAULT 0,
          completed_actions INTEGER NOT NULL DEFAULT 0,
          skipped_actions INTEGER NOT NULL DEFAULT 0,
          before_json TEXT, after_json TEXT, report_json TEXT,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE tasks (
          task_id TEXT PRIMARY KEY,
          plan_hash TEXT NOT NULL REFERENCES plans(plan_hash) ON DELETE CASCADE,
          action_id TEXT NOT NULL, kind TEXT NOT NULL, table_name TEXT NOT NULL,
          partition_name TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL, input_hash TEXT, output_hash TEXT,
          duration_ms REAL NOT NULL DEFAULT 0, result_json TEXT, error TEXT,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          UNIQUE(plan_hash, action_id));
        CREATE TABLE reports (
          report_id TEXT PRIMARY KEY, plan_hash TEXT NOT NULL,
          json_path TEXT NOT NULL, markdown_path TEXT NOT NULL,
          payload_json TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE INDEX idx_tasks_plan ON tasks(plan_hash);
        CREATE INDEX idx_reports_plan ON reports(plan_hash);
        """
    )
    conn.execute("PRAGMA user_version = 1")
    conn.execute(
        "INSERT INTO plans (plan_hash, format, status, created_at, updated_at) "
        "VALUES ('ph1', 'lakewatch.plan/1', 'completed', 't', 't')"
    )
    conn.commit()
    conn.close()

    ledger = Ledger(db_path)
    ledger.migrate()
    with sqlite3.connect(db_path) as check:
        assert check.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        tables = {
            row[0]
            for row in check.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "action_claims" in tables
        # Old data survived untouched.
        row = check.execute(
            "SELECT status FROM plans WHERE plan_hash='ph1'"
        ).fetchone()
        assert row == ("completed",)

    # Migrating again is a no-op.
    ledger.migrate()


# --------------------------------------------------------------------------
# Busy/timeout is a retryable error, not a business failure
# --------------------------------------------------------------------------
def test_sqlite_lock_is_classified_as_retryable_busy(tmp_path):
    ledger = Ledger(tmp_path / "busy.sqlite3")
    ledger.migrate()
    blocker = sqlite3.connect(ledger.path, timeout=0)
    blocker.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.raises(LedgerBusyError):
            ledger.set_plan_status("anything", "running")
    finally:
        blocker.rollback()
        blocker.close()


def test_cli_apply_under_lock_exits_with_retryable_code(tmp_path, lake, policy):
    ledger_path = tmp_path / "ledger.sqlite3"
    config = write_cli_config(
        tmp_path / "config.yaml", lake, policy,
        ledger_path, tmp_path / "reports",
    )
    plan_path = tmp_path / "plan.json"
    assert main(["plan", "--config", str(config), "--out", str(plan_path)]) == 0
    # Create the ledger first, then hold an exclusive lock across the apply.
    Ledger(ledger_path).migrate()
    blocker = sqlite3.connect(ledger_path, timeout=0)
    blocker.execute("BEGIN EXCLUSIVE")
    try:
        code = main(["apply", "--config", str(config), "--plan", str(plan_path)])
    finally:
        blocker.rollback()
        blocker.close()
    assert code == EXIT_BUSY
    assert EXIT_BUSY != 1  # distinct from the generic failure exit code


# --------------------------------------------------------------------------
# Claim mechanics at the ledger boundary
# --------------------------------------------------------------------------
def _plan_and_action(lake, policy):
    plan = build_plan(scan_lake(lake, policy), policy)
    action = plan["actions"][0]
    return plan, action


def test_second_owner_is_blocked_by_live_lease_and_skips_with_reason(
    tmp_path, lake, policy
):
    plan, action = _plan_and_action(lake, policy)
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    ledger.migrate()
    ledger.ensure_plan(plan)
    owner_a = new_owner_id()
    lease, holder = acquire_claim(
        ledger, plan["plan_hash"], action, owner_a,
        ttl_seconds=30, wait_timeout_seconds=0.2, poll_interval_seconds=0.05,
    )
    assert lease is not None and holder is None

    owner_b = new_owner_id()
    lease_b, holder_b = acquire_claim(
        ledger, plan["plan_hash"], action, owner_b,
        ttl_seconds=30, wait_timeout_seconds=0.2, poll_interval_seconds=0.05,
    )
    assert lease_b is None
    assert holder_b is not None and holder_b["owner_id"] == owner_a

    claim = ledger.get_claim(plan["plan_hash"], action["action_id"])
    assert claim["status"] == "held"
    assert claim["attempts"] == 1  # blocked, never taken over
    lease.close()


def test_expired_lease_is_taken_over_and_reaped(tmp_path, lake, policy):
    plan, action = _plan_and_action(lake, policy)
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    ledger.migrate()
    ledger.ensure_plan(plan)
    ph, aid = plan["plan_hash"], action["action_id"]

    first, _ = acquire_claim(
        ledger, ph, action, new_owner_id(),
        ttl_seconds=30, wait_timeout_seconds=0.2, poll_interval_seconds=0.05,
    )
    assert first is not None
    # Simulate a crashed holder: lease in the past, nobody heartbeating.
    with ledger._connect() as conn:
        conn.execute(
            "UPDATE action_claims SET lease_expires_at = ?",
            (iso_z(utc_now() - timedelta(seconds=5)),),
        )

    reaped = ledger.reap_stale_claims()
    assert len(reaped) == 1 and reaped[0]["action_id"] == aid
    assert ledger.get_claim(ph, aid)["status"] == "released"

    # Re-claim after release works and counts another attempt.
    second, holder = acquire_claim(
        ledger, ph, action, new_owner_id(),
        ttl_seconds=30, wait_timeout_seconds=0.2, poll_interval_seconds=0.05,
    )
    assert second is not None and holder is None
    claim = ledger.get_claim(ph, aid)
    assert claim["attempts"] == 2
    second.close()


def test_heartbeat_renews_lease_and_detects_loss(tmp_path, lake, policy):
    plan, action = _plan_and_action(lake, policy)
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    ledger.migrate()
    ledger.ensure_plan(plan)
    ph, aid = plan["plan_hash"], action["action_id"]
    owner = new_owner_id()
    lease, _ = acquire_claim(
        ledger, ph, action, owner,
        ttl_seconds=30, wait_timeout_seconds=0.2, poll_interval_seconds=0.05,
    )
    later = iso_z(utc_now() + timedelta(seconds=30))
    assert ledger.heartbeat_claim(ph, aid, owner, later, later)
    # A different owner cannot renew a lease it does not hold.
    assert not ledger.heartbeat_claim(ph, aid, "intruder", later, later)
    lease.close()
    assert ledger.get_claim(ph, aid)["status"] == "released"


def test_released_claim_is_reclaimable_after_failed_action(tmp_path, lake, policy):
    """A failed action's released claim must not wedge future retries."""
    plan, action = _plan_and_action(lake, policy)
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    ledger.migrate()
    ledger.ensure_plan(plan)
    first, _ = acquire_claim(
        ledger, plan["plan_hash"], action, new_owner_id(),
        ttl_seconds=30, wait_timeout_seconds=0.2, poll_interval_seconds=0.05,
    )
    first.close()  # action failed before completion; claim released
    second, holder = acquire_claim(
        ledger, plan["plan_hash"], action, new_owner_id(),
        ttl_seconds=30, wait_timeout_seconds=0.2, poll_interval_seconds=0.05,
    )
    assert second is not None and holder is None
    assert ledger.get_claim(
        plan["plan_hash"], action["action_id"]
    )["attempts"] == 2
    second.close()


# --------------------------------------------------------------------------
# dry-run never touches the ledger
# --------------------------------------------------------------------------
def test_dry_run_writes_nothing_and_creates_no_ledger(
    tmp_path, lake, policy
):
    from conftest import write_cli_config

    ledger_path = tmp_path / "ledger.sqlite3"
    config = write_cli_config(
        tmp_path / "config.yaml", lake, policy,
        ledger_path, tmp_path / "reports",
    )
    plan_path = tmp_path / "plan.json"
    assert main(["plan", "--config", str(config), "--out", str(plan_path)]) == 0
    before = set(tmp_path.rglob("*"))
    assert main([
        "apply", "--config", str(config),
        "--plan", str(plan_path), "--dry-run",
    ]) == 0
    after = set(tmp_path.rglob("*"))
    assert not ledger_path.exists()
    assert before == after  # no ledger, no plans dir, nothing else written


# --------------------------------------------------------------------------
# plan_hash is independent of claim state
# --------------------------------------------------------------------------
def test_action_finished_elsewhile_is_skipped_not_reexecuted(
    tmp_path, lake, policy, monkeypatch
):
    """If the task is completed between claim acquisition and execution, the
    process must skip it instead of running the executor a second time."""
    before = scan_lake(lake, policy)
    plan = build_plan(before, policy)
    target = plan["actions"][0]
    ledger = Ledger(tmp_path / "ledger.sqlite3")

    # The post-claim re-check calls get_task a second time.  The first call
    # (pre-claim idempotence check) finds no row; the second finds the row
    # created by claim_task -- pretend a sibling completed it in between.
    real_get = ledger.get_task
    flipped = {"done": False}

    def sibling_finished(plan_hash, action_id):
        row = real_get(plan_hash, action_id)
        # Inject the sibling's completion exactly once, on the post-claim
        # re-check of the target (the first call where its row exists).
        if (
            action_id == target["action_id"]
            and row is not None
            and not flipped["done"]
        ):
            flipped["done"] = True
            return {**row, "status": "completed", "output_hash": "sibling-hash",
                    "result_json": json.dumps({"output_hash": "sibling-hash"})}
        return row

    executed = []

    def spy_executor(root, action):
        executed.append(action["action_id"])
        return {"status": "completed", "output_hash": "spy-hash"}

    monkeypatch.setattr(ledger, "get_task", sibling_finished)
    import lakewatch.executor as executor_pkg
    monkeypatch.setitem(executor_pkg.EXECUTORS, target["kind"], spy_executor)

    result = apply_plan(
        lake, plan, ledger, before=before,
        lease_ttl_seconds=30, wait_timeout_seconds=0.5,
        poll_interval_seconds=0.05,
    )
    target_outcome = next(
        a for a in result["actions"] if a["action_id"] == target["action_id"]
    )
    assert target_outcome["status"] == "skipped"
    assert target_outcome["skip_reason"] == REASON_COMPLETED_WHILE_WAITING
    assert target_outcome["output_hash"] == "sibling-hash"
    # The spied executor ran only for other actions, never the target.
    assert target["action_id"] not in executed
    monkeypatch.undo()


def test_plan_hash_unchanged_by_claims_and_rerun_is_already_completed(
    tmp_path, lake, policy
):
    before = scan_lake(lake, policy)
    plan = build_plan(before, policy)
    original_hash = plan["plan_hash"]
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    result = apply_plan(lake, plan, ledger, before=before, policy=policy)
    assert result["status"] == "completed"
    # Claims live only in the ledger; the plan file and hash are untouched.
    assert compute_plan_hash(plan) == original_hash

    repeat = apply_plan(lake, plan, ledger, before=before, policy=policy)
    assert repeat["status"] == "already_completed"
    assert repeat["skipped_actions"] == len(plan["actions"])
    assert all(
        item["skip_reason"] == REASON_ALREADY_DONE for item in repeat["actions"]
    )
    # Exactly one task row and one claim row per action -- no duplicates.
    tasks = ledger.list_tasks(original_hash)
    claims = ledger.list_claims(original_hash)
    assert len(tasks) == len(plan["actions"])
    assert len(claims) == len(plan["actions"])
    assert len({row["task_id"] for row in tasks}) == len(tasks)


# --------------------------------------------------------------------------
# Multi-process acceptance tests
# --------------------------------------------------------------------------
def _spawn_worker(lake, plan_path, ledger_path, out_path, env_extra=None):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT)
    if env_extra:
        env.update(env_extra)
    return subprocess.Popen(
        [sys.executable, str(WORKER),
         str(lake), str(plan_path), str(ledger_path), str(out_path)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _read_result(out_path: Path, timeout: float = 90.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if out_path.exists():
            try:
                return json.loads(out_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pass
        time.sleep(0.1)
    raise AssertionError(f"worker result never appeared at {out_path}")


def test_two_processes_apply_same_plan_without_double_execution(
    tmp_path, lake, policy
):
    before = scan_lake(lake, policy)
    plan = build_plan(before, policy)
    plan_path = save_plan(plan, tmp_path / "plan.json")
    ledger_path = tmp_path / "ledger.sqlite3"
    signature_before = tree_signature(lake)

    target = plan["actions"][0]["action_id"]
    env = {
        "LAKEWATCH_BLOCK_ACTION": target,
        "LAKEWATCH_TEST_TTL": "30",
        "LAKEWATCH_TEST_WAIT": "10",
        "LAKEWATCH_TEST_POLL": "0.1",
    }
    out_a = tmp_path / "a.json"
    out_b = tmp_path / "b.json"
    proc_a = _spawn_worker(lake, plan_path, ledger_path, out_a, env)
    # Give A a head start so it holds the first claim before B starts.
    time.sleep(0.7)
    proc_b = _spawn_worker(lake, plan_path, ledger_path, out_b, env)

    rc_a = proc_a.wait(timeout=120)
    rc_b = proc_b.wait(timeout=120)
    assert rc_a == 0, proc_a.stderr.read().decode()
    assert rc_b == 0, proc_b.stderr.read().decode()

    payload_a = _read_result(out_a)
    payload_b = _read_result(out_b)
    assert "error" not in payload_a, payload_a
    assert "error" not in payload_b, payload_b
    ra, rb = payload_a["result"], payload_b["result"]

    # The plan is finished exactly once; the other run observes completion.
    statuses = {ra["status"], rb["status"]}
    assert "completed" in statuses
    assert statuses <= {"completed", "already_completed", "skipped_concurrent"}

    # Every action was executed by exactly one owner end to end.
    executed = [
        item for item in ra["actions"] + rb["actions"]
        if item["status"] == "completed"
    ]
    executed_ids = [item["action_id"] for item in executed]
    assert sorted(executed_ids) == sorted(a["action_id"] for a in plan["actions"])
    assert len(executed_ids) == len(set(executed_ids))

    ledger = Ledger(ledger_path)
    tasks = ledger.list_tasks(plan["plan_hash"])
    assert len(tasks) == len(plan["actions"])
    assert {t["status"] for t in tasks} == {"completed"}
    # No duplicate ledger records and no dangling held claims at the end.
    claims = ledger.list_claims(plan["plan_hash"])
    assert len(claims) == len(plan["actions"])
    assert {c["status"] for c in claims} == {"released"}

    # Physical lake was changed once, consistently.
    assert signature_before != tree_signature(lake)
    after = scan_lake(lake, policy)
    assert after.summary()["missing_count"] == 0
    # Each compacted output exists exactly once on disk.
    for item in plan["actions"]:
        if item["kind"] == "compact":
            out_rel = item["expected"]["output_path"]
            assert (lake / item["table"] / out_rel).is_file()
    assert ledger.get_plan(plan["plan_hash"])["status"] == "completed"


def test_killed_holder_is_taken_over_without_permanent_block(
    tmp_path, lake, policy
):
    plan = build_plan(scan_lake(lake, policy), policy)
    plan_path = save_plan(plan, tmp_path / "plan.json")
    ledger_path = tmp_path / "ledger.sqlite3"
    target = plan["actions"][0]["action_id"]

    ctx = mp.get_context("spawn")
    out_holder = tmp_path / "holder.json"
    env = {
        "LAKEWATCH_BLOCK_ACTION": target,
        "LAKEWATCH_HOLD_FILE": str(tmp_path / "hold"),
        "LAKEWATCH_HOLD_MODE": "freeze",
        "LAKEWATCH_TEST_TTL": "1",
        "LAKEWATCH_TEST_WAIT": "20",
        "LAKEWATCH_TEST_POLL": "0.05",
    }
    holder = ctx.Process(
        target=_mp_worker,
        args=(lake, plan_path, ledger_path, out_holder, env),
    )
    holder.start()

    # Wait until the holder is parked inside the target action.
    deadline = time.time() + 30
    while time.time() < deadline:
        ledger = Ledger(ledger_path)
        if ledger.path.exists():
            claims = ledger.list_claims(plan["plan_hash"])
            if any(c["action_id"] == target and c["status"] == "held"
                   for c in claims):
                break
        time.sleep(0.1)
    else:
        holder.terminate()
        raise AssertionError("holder never claimed the target action")

    claim_before = ledger.get_claim(plan["plan_hash"], target)
    assert claim_before["status"] == "held"

    # Hard kill: no heartbeat release, zombie lease left behind.
    holder.kill()
    holder.join(timeout=10)
    assert not holder.is_alive()

    # A second process takes over once the 1s lease expires and finishes.
    out_taker = tmp_path / "taker.json"
    taker_env = {
        "LAKEWATCH_TEST_TTL": "5",
        "LAKEWATCH_TEST_WAIT": "20",
        "LAKEWATCH_TEST_POLL": "0.05",
    }
    start = time.time()
    taker = subprocess.run(
        [sys.executable, str(WORKER),
         str(lake), str(plan_path), ledger_path, out_taker],
        env={**dict(os.environ), "PYTHONPATH": str(REPO_ROOT), **taker_env},
        capture_output=True, text=True, timeout=120,
    )
    elapsed = time.time() - start
    assert taker.returncode == 0, taker.stderr
    payload = _read_result(out_taker)
    assert "error" not in payload, payload
    assert payload["result"]["status"] == "completed"

    # Takeover happened in bounded time (lease TTL, not a permanent hang).
    assert elapsed < 30

    ledger = Ledger(ledger_path)
    target_claim = ledger.get_claim(plan["plan_hash"], target)
    assert target_claim["attempts"] >= 2  # holder's claim + takeover
    tasks = ledger.list_tasks(plan["plan_hash"])
    assert {t["status"] for t in tasks} == {"completed"}
    assert {c["status"] for c in ledger.list_claims(plan["plan_hash"])} == {
        "released"
    }
    assert scan_lake(lake, policy).summary()["missing_count"] == 0


def test_live_holder_produces_skipped_concurrent_without_duplicate_work(
    tmp_path, lake, policy
):
    plan = build_plan(scan_lake(lake, policy), policy)
    plan_path = save_plan(plan, tmp_path / "plan.json")
    ledger_path = tmp_path / "ledger.sqlite3"
    target = plan["actions"][0]["action_id"]
    hold_file = tmp_path / "hold"
    hold_file.write_text("on")

    env = {
        "LAKEWATCH_BLOCK_ACTION": target,
        "LAKEWATCH_HOLD_FILE": str(hold_file),
        "LAKEWATCH_HOLD_MODE": "wait",
        "LAKEWATCH_TEST_TTL": "30",
        "LAKEWATCH_TEST_WAIT": "1",
        "LAKEWATCH_TEST_POLL": "0.1",
    }
    out_holder = tmp_path / "holder.json"
    holder_p = _spawn_worker(lake, plan_path, ledger_path, out_holder, env)

    deadline = time.time() + 30
    ledger = Ledger(ledger_path)
    while time.time() < deadline:
        if ledger.path.exists() and ledger.list_claims(plan["plan_hash"]):
            if any(c["action_id"] == target for c in
                   ledger.list_claims(plan["plan_hash"])):
                break
        time.sleep(0.1)

    out_rush = tmp_path / "rush.json"
    rush = subprocess.run(
        [sys.executable, str(WORKER),
         str(lake), plan_path, ledger_path, out_rush],
        env={
            **dict(os.environ), "PYTHONPATH": str(REPO_ROOT),
            "LAKEWATCH_TEST_TTL": "30",
            "LAKEWATCH_TEST_WAIT": "1",
            "LAKEWATCH_TEST_POLL": "0.1",
        },
        capture_output=True, text=True, timeout=60,
    )
    assert rush.returncode == 0, rush.stderr
    rushed = _read_result(out_rush)["result"]
    assert rushed["status"] == "skipped_concurrent"
    target_outcomes = [
        a for a in rushed["actions"] if a["action_id"] == target
    ]
    assert len(target_outcomes) == 1
    assert target_outcomes[0]["status"] == "skipped"
    assert target_outcomes[0]["skip_reason"] == REASON_CLAIM_HELD
    assert target_outcomes[0]["claimed_by"]
    # The rush took every *unheld* action and only yielded the parked one.
    rushed_executed = [
        a["action_id"] for a in rushed["actions"] if a["status"] == "completed"
    ]
    assert target not in rushed_executed
    assert len(rushed_executed) == len(plan["actions"]) - 1

    # Release the holder; it finishes its parked action then idempotently
    # skips the work the rush already completed.
    hold_file.unlink()
    assert holder_p.wait(timeout=120) == 0
    holder_payload = _read_result(out_holder)
    assert "error" not in holder_payload, holder_payload
    holder_result = holder_payload["result"]
    holder_executed = [
        a["action_id"]
        for a in holder_result["actions"] if a["status"] == "completed"
    ]
    assert target in holder_executed
    # The two processes' executed sets partition the plan, never overlap.
    assert not set(holder_executed) & set(rushed_executed)
    assert sorted(holder_executed + rushed_executed) == sorted(
        a["action_id"] for a in plan["actions"]
    )

    ledger = Ledger(ledger_path)
    tasks = ledger.list_tasks(plan["plan_hash"])
    assert len(tasks) == len(plan["actions"])
    assert {t["status"] for t in tasks} == {"completed"}
    # A subsequent re-run is the standard already_completed no-op.
    rerun = apply_plan(lake, plan, Ledger(ledger_path))
    assert rerun["status"] == "already_completed"


def _mp_worker(lake, plan_path, ledger_path, out_path, env_extra):
    """Entry point for a spawned multiprocessing.Process worker."""
    os.environ.update(env_extra)
    sys.path.insert(0, str(REPO_ROOT))
    import runpy

    sys.argv = [
        str(WORKER), str(lake), str(plan_path),
        str(ledger_path), str(out_path),
    ]
    runpy.run_path(str(WORKER), run_name="__main__")
