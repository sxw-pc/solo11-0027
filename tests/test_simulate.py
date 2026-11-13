"""Tests for the read-only plan simulator.

The acceptance logic mirrors how an operator reviews a plan before applying:
the projected summary must match the real ledger report on a pristine lake and
on a half-applied lake, while every stale or missing input must surface as a
structured risk instead of aborting the run.
"""
from __future__ import annotations

import dataclasses
import json
import socket
from pathlib import Path

import pytest

from lakewatch.cli import main
from lakewatch.executor import apply_plan
from lakewatch.executor.compact import execute_compaction
from lakewatch.executor.repair import execute_repair
from lakewatch.executor.vacuum import execute_vacuum
from lakewatch.ledger.db import Ledger
from lakewatch.planner.plan import build_plan, load_plan, save_plan
from lakewatch.scanners import scan_lake
from lakewatch.simulator import (
    SIMULATION_FORMAT,
    render_simulation_text,
    simulate_plan,
)
from lakewatch.simulator.engine import COUNTER_FIELDS, SCAN_COMPARISON_FIELDS

from conftest import tree_signature

EXECUTORS = {
    "compact": execute_compaction,
    "repair": execute_repair,
    "vacuum": execute_vacuum,
}


def _simulate(lake: Path, policy, plan: dict, **kwargs):
    return simulate_plan(lake, plan, policy=policy, **kwargs)


def test_simulation_on_pristine_lake_matches_real_apply(lake, policy, tmp_path):
    before = scan_lake(lake, policy)
    plan = build_plan(before, policy)
    simulation = _simulate(lake, policy, plan)

    assert simulation["format"] == SIMULATION_FORMAT
    assert simulation["schema_version"] == 1
    assert simulation["plan_hash"] == plan["plan_hash"]
    assert simulation["summary"]["would_fail"] == 0
    assert simulation["summary"]["risk_count"] == 0
    assert simulation["summary"]["state_drift"] is False
    assert set(simulation["summary"]["before"]) == set(COUNTER_FIELDS)
    for field in SCAN_COMPARISON_FIELDS:
        assert simulation["scan_comparison"]["before"][field] == before.summary()[field]

    ledger = Ledger(tmp_path / "ledger.sqlite3")
    result = apply_plan(
        lake, plan, ledger, before=before, policy=policy,
        reports_dir=tmp_path / "reports",
    )
    report = result["report"]

    # The scan-shaped projection must agree field-for-field with the ledger
    # for the counters the simulator projects; diagnostic counters are out of
    # scope for a what-if replay.
    for field in SCAN_COMPARISON_FIELDS:
        assert simulation["scan_comparison"]["before"][field] == report["before"][field]
        assert simulation["scan_comparison"]["after"][field] == report["after"][field]

    projected = simulation["summary"]["projected"]
    outcomes = report["outcomes"]
    assert projected["compactions"] == outcomes["compactions"]
    assert (
        projected["compaction_input_files_removed"]
        == outcomes["compaction_input_files_removed"]
    )
    assert projected["compaction_output_files"] == outcomes["compaction_output_files"]
    assert (
        projected["manifest_entries_dropped"]
        == outcomes["manifest_entries_dropped"]
    )
    assert projected["snapshots_removed"] == outcomes["snapshots_removed"]
    assert projected["vacuumed_data_files"] == outcomes["vacuumed_data_files"]
    assert projected["released_bytes"] == outcomes["bytes_released"]

    # Compaction conserves data bytes; only vacuum frees physical bytes.
    delta = simulation["summary"]["delta"]
    assert delta["data_bytes"] < 0
    assert delta["data_file_count"] == -(
        projected["compaction_input_files_removed"]
        - projected["compaction_output_files"]
        + projected["vacuumed_data_files"]
    )


def test_simulation_replays_half_applied_plan_without_double_counting(
    lake, policy, tmp_path
):
    before = scan_lake(lake, policy)
    plan = build_plan(before, policy)
    half = len(plan["actions"]) // 2
    for action in plan["actions"][:half]:
        EXECUTORS[action["kind"]](lake, action)

    half_scan = scan_lake(lake, policy)
    simulation = _simulate(lake, policy, plan)

    # The already-executed prefix must be detected, not counted again.
    half_summary = half_scan.summary()
    for field in SCAN_COMPARISON_FIELDS:
        assert simulation["scan_comparison"]["before"][field] == half_summary[field]
    statuses = [item["status"] for item in simulation["actions"]]
    assert "would_fail" in statuses or "would_noop" in statuses
    assert statuses[half:] == ["would_apply"] * (len(plan["actions"]) - half)
    assert all(
        item["status"] != "would_apply" for item in simulation["actions"][:half]
    )

    # Finish the plan for real; the simulator's final projection must match.
    for action in plan["actions"][half:]:
        EXECUTORS[action["kind"]](lake, action)
    final_scan = scan_lake(lake, policy)
    for field in SCAN_COMPARISON_FIELDS:
        assert (
            simulation["scan_comparison"]["after"][field]
            == final_scan.summary()[field]
        )

    # The aggregate delta of the remaining actions plus the half-state equals
    # the real end state, proving no file's bytes were counted twice.
    projected_after = simulation["summary"]["after"]
    real_totals = final_scan.summary()
    assert projected_after["data_file_count"] == real_totals["file_count"]
    assert projected_after["data_bytes"] == real_totals["total_bytes"]


def test_manually_deleted_input_is_a_risk_not_a_crash(lake, policy):
    plan = build_plan(scan_lake(lake, policy), policy)
    compact = next(item for item in plan["actions"] if item["kind"] == "compact")
    victim = compact["inputs"][0]["path"]
    target = lake / compact["table"] / victim
    target.unlink()

    simulation = _simulate(lake, policy, plan)

    assert simulation["summary"]["would_fail"] >= 1
    risks = [
        item for item in simulation["risks"]
        if item["code"] == "input_missing" and item["path"] == victim
    ]
    assert len(risks) == 1
    assert risks[0]["severity"] == "error"
    assert set(risks[0]) == {
        "code", "severity", "action_id", "kind", "table", "path",
        "message", "expected", "actual",
    }
    failed = next(
        item for item in simulation["actions"]
        if item["action_id"] == compact["action_id"]
    )
    assert failed["status"] == "would_fail"
    # A failed action projects no change and the other actions still replay.
    assert failed["delta"] == {field: 0 for field in COUNTER_FIELDS}
    assert simulation["summary"]["would_apply"] == len(plan["actions"]) - 1


def test_byte_and_hash_drift_are_reported_separately(lake, policy):
    plan = build_plan(scan_lake(lake, policy), policy)
    compact = next(item for item in plan["actions"] if item["kind"] == "compact")
    victim_rel = compact["inputs"][0]["path"]
    target = lake / compact["table"] / victim_rel
    original = target.read_bytes()
    target.write_bytes(original + b"\x00" * 64)

    simulation = _simulate(lake, policy, plan)
    codes = {item["code"] for item in simulation["risks"] if item["path"] == victim_rel}
    assert {"input_bytes_mismatch", "input_hash_mismatch"} <= codes
    target_action = next(
        item for item in simulation["actions"]
        if item["action_id"] == compact["action_id"]
    )
    assert target_action["status"] == "would_fail"


def test_repair_reappeared_file_is_kept_and_missing_entry_is_pruned(
    tmp_path, lake, policy
):
    plan = build_plan(scan_lake(lake, policy), policy)
    repair = next(item for item in plan["actions"] if item["kind"] == "repair")
    dropped = repair["expected"]["drop_paths"][0]

    # Recreate the "missing" file: the executor would keep the manifest entry.
    restored = lake / repair["table"] / dropped
    restored.parent.mkdir(parents=True, exist_ok=True)
    restored.write_bytes(b"reappeared")

    simulation = _simulate(lake, policy, plan)
    action = next(
        item for item in simulation["actions"]
        if item["action_id"] == repair["action_id"]
    )
    assert action["status"] == "would_noop"
    assert any(
        item["code"] == "repair_path_reappeared" and item["path"] == dropped
        for item in action["risks"]
    )
    assert action["projection"]["dropped_entries"] == []


def test_simulation_is_strictly_read_only(lake, policy, tmp_path):
    plan = build_plan(scan_lake(lake, policy), policy)
    plan_path = save_plan(plan, tmp_path / "plan.json")
    before = tree_signature(lake)
    plan_before = plan_path.read_bytes()

    simulate_plan(lake, load_plan(plan_path), policy=policy, plan_file=plan_path)

    assert tree_signature(lake) == before
    assert plan_path.read_bytes() == plan_before
    # No ledger or report artifacts may appear next to the lake.
    assert not (tmp_path / "ledger.sqlite3").exists()
    assert not (lake.parent / "reports").exists()


def test_simulation_makes_no_network_calls(lake, policy, monkeypatch):
    plan = build_plan(scan_lake(lake, policy), policy)

    def forbid_dns(host, *args, **kwargs):
        raise AssertionError(f"simulator performed a DNS lookup: {host}")

    def forbid_connect(sock, address):
        raise AssertionError(f"simulator opened a socket: {address}")

    monkeypatch.setattr(socket, "getaddrinfo", forbid_dns)
    monkeypatch.setattr(socket.socket, "connect", forbid_connect)
    simulation = simulate_plan(lake, plan, policy=policy)
    assert simulation["summary"]["risk_count"] == 0


def test_simulator_module_cannot_reach_executor_or_ledger():
    import lakewatch.simulator.engine as engine
    import lakewatch.simulator.state as state

    source = Path(engine.__file__).read_text(encoding="utf-8")
    source += Path(state.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "from ..executor",
        "import executor",
        "from ..ledger",
        "import ledger",
        "import pyarrow",
        "write_text",
        "write_bytes",
        "unlink",
        "os.replace",
    ):
        assert forbidden not in source


def test_simulation_json_contract_is_stable(lake, policy):
    plan = build_plan(scan_lake(lake, policy), policy)
    first = _simulate(lake, policy, plan)
    second = _simulate(lake, policy, plan)
    # simulation_id is a content fingerprint: identical inputs, identical id.
    assert first["simulation_id"] == second["simulation_id"]
    top_level = {
        "format", "schema_version", "simulation_id", "plan_hash", "plan_file",
        "lake_root", "as_of", "policy_hash", "summary", "scan_comparison",
        "actions", "risks",
    }
    assert set(first) == top_level
    action_keys = {
        "order_index", "action_id", "kind", "table", "partition", "status",
        "risks", "before", "after", "delta", "projection",
    }
    assert set(first["actions"][0]) == action_keys
    # Pristine lake has no risks; the risk field contract is covered by the
    # drift tests where structured risks are asserted individually.
    assert set(first["risks"]) == set()
    # Order indices follow the deterministic plan order.
    assert [item["order_index"] for item in first["actions"]] == list(
        range(len(plan["actions"]))
    )
    # JSON serialization must accept the whole document (stable, no exotic types).
    json.loads(json.dumps(first, sort_keys=True))


def test_rendered_text_summary_contains_key_sections(lake, policy):
    plan = build_plan(scan_lake(lake, policy), policy)
    text = render_simulation_text(_simulate(lake, policy, plan))
    for heading in (
        "Action verdict",
        "Projected lake totals",
        "Projected outcomes",
        "Actions in execution order",
        "Risks",
        "Scan-summary comparison",
    ):
        assert heading in text


def test_cli_simulate_json_and_strict_exit_codes(
    tmp_path, lake, policy, capsys
):
    from conftest import write_cli_config

    config = write_cli_config(
        tmp_path / "configs" / "default.yaml", lake, policy,
        tmp_path / "ledger.sqlite3", tmp_path / "reports",
    )
    plan_path = tmp_path / "plan.json"
    assert main(["plan", "--config", str(config), "--out", str(plan_path)]) == 0
    capsys.readouterr()

    out_path = tmp_path / "sim.json"
    assert main([
        "simulate", "--config", str(config), "--plan", str(plan_path),
        "--json", "--out", str(out_path),
    ]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["format"] == SIMULATION_FORMAT
    saved = json.loads(out_path.read_text(encoding="utf-8"))
    assert saved["simulation_id"] == payload["simulation_id"]

    # Pristine lake: strict stays green.
    assert main([
        "simulate", "--config", str(config), "--plan", str(plan_path),
        "--strict",
    ]) == 0
    capsys.readouterr()

    # Delete an input: strict must flag drift with exit 1 but still emit JSON.
    plan = load_plan(plan_path)
    compact = next(item for item in plan["actions"] if item["kind"] == "compact")
    (lake / compact["table"] / compact["inputs"][0]["path"]).unlink()
    assert main([
        "simulate", "--config", str(config), "--plan", str(plan_path),
        "--json", "--strict",
    ]) == 1
    drifted = json.loads(capsys.readouterr().out)
    assert drifted["summary"]["would_fail"] >= 1
    assert any(item["code"] == "input_missing" for item in drifted["risks"])


def test_simulation_without_policy_omits_scan_comparison_values(lake, policy):
    plan = build_plan(scan_lake(lake, policy), policy)
    simulation = simulate_plan(lake, plan)
    assert simulation["scan_comparison"] == {"before": None, "after": None}
    # Core counters are always present regardless of policy.
    assert set(simulation["summary"]["before"]) == set(COUNTER_FIELDS)


def test_policy_drift_is_flagged_as_warning(lake, policy):
    plan = build_plan(scan_lake(lake, policy), policy)
    drifted_policy = dataclasses.replace(policy, small_file_bytes=8192)
    simulation = _simulate(lake, drifted_policy, plan)
    assert any(
        item["code"] == "plan_policy_mismatch" and item["severity"] == "warning"
        for item in simulation["risks"]
    )


def test_missing_table_action_is_risk_not_exception(tmp_path, policy):
    # A plan referencing a non-existent table cannot be rebuilt from the lake,
    # so construct a minimal valid plan document and re-hash it.
    from lakewatch.planner.plan import compute_plan_hash

    plan = {
        "format": "lakewatch.plan/1",
        "policy_hash": policy.policy_hash,
        "as_of": policy.as_of_iso,
        "input_digest": "0" * 64,
        "actions": [
            {
                "kind": "vacuum",
                "table": "ghost_table",
                "partition": "",
                "reason": "synthetic",
                "inputs": [],
                "expected": {
                    "delete_snapshot_ids": [],
                    "delete_data_files": [],
                    "affected_partitions": [],
                    "released_bytes": 0,
                },
                "input_hash": "0" * 64,
                "action_id": "vacuum-0000000000000000",
            }
        ],
        "summary": {
            "action_count": 1,
            "total_candidate_actions": 1,
            "deferred_action_count": 0,
            "by_kind": {"repair": 0, "compact": 0, "vacuum": 1},
            "compaction_input_files": 0,
            "expected_released_bytes": 0,
        },
    }
    plan["plan_hash"] = compute_plan_hash(plan)
    root = tmp_path / "empty-lake"
    root.mkdir()
    simulation = simulate_plan(root, plan, policy=policy)
    assert simulation["actions"][0]["status"] == "would_fail"
    assert simulation["risks"][0]["code"] == "table_missing"


@pytest.mark.parametrize("half_point", [1, 3, 7])
def test_overlapping_compaction_inputs_never_double_count(
    lake, policy, half_point
):
    plan = build_plan(scan_lake(lake, policy), policy)
    for action in plan["actions"][:half_point]:
        if action["kind"] == "compact":
            EXECUTORS[action["kind"]](lake, action)

    simulation = _simulate(lake, policy, plan)
    # Sum every per-action byte delta; the engine removes consumed inputs from
    # virtual state, so a re-referenced input contributes zero a second time.
    total_data_delta = sum(
        item["delta"]["data_bytes"] for item in simulation["actions"]
    )
    overall = simulation["summary"]["delta"]["data_bytes"]
    assert total_data_delta == overall
