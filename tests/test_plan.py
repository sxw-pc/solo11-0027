import pytest

from lakewatch.planner.plan import (
    PlanError,
    build_plan,
    load_plan,
    save_plan,
    validate_plan,
)
from lakewatch.scanners import scan_lake

from conftest import policy_with, tree_signature


def test_plan_hash_is_stable_and_actions_are_described(lake, policy):
    first = build_plan(scan_lake(lake, policy), policy)
    second = build_plan(scan_lake(lake, policy), policy)
    assert first["plan_hash"] == second["plan_hash"]
    assert first["summary"]["action_count"] == 10
    for action in first["actions"]:
        assert action["reason"]
        assert action["inputs"]
        assert action["expected"]
        assert action["input_hash"]
    compact = next(item for item in first["actions"] if item["kind"] == "compact")
    assert compact["expected"]["bytes"] == sum(item["bytes"] for item in compact["inputs"])
    assert compact["expected"]["output_path"].endswith(".parquet")
    validate_plan(first)


def test_max_actions_truncation_is_stable(lake, policy):
    capped = policy_with(policy, max_actions=3)
    first = build_plan(scan_lake(lake, capped), capped)
    second = build_plan(scan_lake(lake, capped), capped)
    assert len(first["actions"]) == 3
    assert first["summary"]["total_candidate_actions"] == 10
    assert first["summary"]["deferred_action_count"] == 7
    assert first["plan_hash"] == second["plan_hash"]
    assert [item["action_id"] for item in first["actions"]] == [
        item["action_id"] for item in second["actions"]
    ]


def test_plan_is_read_only(lake, policy):
    scan = scan_lake(lake, policy)
    before = tree_signature(lake)
    build_plan(scan, policy)
    assert tree_signature(lake) == before


def test_saved_plan_round_trip_and_tamper_detection(tmp_path, lake, policy):
    plan = build_plan(scan_lake(lake, policy), policy)
    target = save_plan(plan, tmp_path / "plans" / "plan.json")
    loaded = load_plan(target)
    assert loaded["plan_hash"] == plan["plan_hash"]
    loaded["actions"][0]["reason"] = "tampered"
    with pytest.raises(PlanError, match="hash"):
        validate_plan(loaded)
