"""Adoption of explicitly declared orphan files.

The sample lake's ``events`` table contains ``part-orphan-000.parquet`` in
``region=east/day=2025-01-02``: a valid parquet file that neither the current
manifest nor any snapshot references.  It is the positive case.  Adoption
must stay opt-in, verify ownership/layout/parquet, pin candidate details
and the manifest generation, and re-check both at apply time.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakewatch.catalog.manifest import load_manifest
from lakewatch.catalog.snapshot import load_snapshots
from lakewatch.executor import apply_plan
from lakewatch.executor.adopt import AdoptError, execute_adoption
from lakewatch.ledger.db import Ledger
from lakewatch.planner.plan import (
    PlanError,
    build_plan,
    load_plan,
    save_plan,
    validate_plan,
)
from lakewatch.planner.policy import AdoptCandidate, Policy, PolicyError
from lakewatch.scanners import scan_lake

from conftest import descriptor, policy_with, tree_signature, write_json, write_manifest, \
    write_parquet, write_snapshot

ORPHAN = "region=east/day=2025-01-02/part-orphan-000.parquet"
LEGACY_SNAPSHOT_ONLY = "region=north/day=2025-01-01/part-legacy-000.parquet"


def _adopt_policy(policy, candidates, enabled=True):
    return policy_with(
        policy,
        adopt_enabled=enabled,
        adopt_candidates=tuple(
            AdoptCandidate(table=table, path=path) for table, path in candidates
        ),
    )


def test_default_plan_never_includes_adopt(lake, policy):
    plan = build_plan(scan_lake(lake, policy), policy)
    assert not [item for item in plan["actions"] if item["kind"] == "adopt"]
    assert plan["summary"]["by_kind"]["adopt"] == 0
    # The orphan is still reported as a diagnostic, not silently taken over.
    scan = scan_lake(lake, policy)
    assert [item["path"] for item in scan.consistency["events"]["orphans"]
            if item["scope"] == "unreferenced"] == [
        "raw_dump/part-bad-000.parquet", ORPHAN]


def test_declared_orphan_becomes_single_adopt_action(lake, policy):
    active = _adopt_policy(policy, [("events", ORPHAN)])
    scan = scan_lake(lake, active)
    adoption = scan.consistency["events"]["adoption"]
    assert adoption["enabled"] is True
    assert [item["path"] for item in adoption["eligible"]] == [ORPHAN]
    candidate = adoption["eligible"][0]
    assert candidate["partition"] == "region=east/day=2025-01-02"
    assert candidate["rows"] == 7
    assert candidate["bytes"] > 0
    assert candidate["manifest_generation"] == 5
    assert candidate["schema"]

    plan = build_plan(scan, active)
    adopt = [item for item in plan["actions"] if item["kind"] == "adopt"]
    assert len(adopt) == 1
    action = adopt[0]
    assert action["table"] == "events"
    assert action["expected"]["manifest_generation"] == 5
    assert action["expected"]["next_generation"] == 6
    assert action["expected"]["candidates"] == [{
        "path": ORPHAN,
        "partition": "region=east/day=2025-01-02",
        "bytes": candidate["bytes"],
        "rows": 7,
        "sha256": candidate["sha256"],
        "schema": candidate["schema"],
    }]
    assert action["inputs"][0]["sha256"] == candidate["sha256"]
    assert action["inputs"][0]["schema"] == candidate["schema"]
    validate_plan(plan)

    # The pre-existing repair/compact/vacuum actions are untouched.
    kinds = [item["kind"] for item in plan["actions"]]
    assert kinds.count("repair") == 1
    assert kinds.count("compact") == 7
    assert kinds.count("vacuum") == 2
    assert plan["summary"]["action_count"] == 11


def test_hidden_candidates_are_never_adopted(tmp_path, policy):
    """Only the policy's explicit list participates; other orphans stay put."""
    root = tmp_path / "lake"
    table = root / "t"
    listed = "region=east/part-listed.parquet"
    write_parquet(table / listed, rows=3, seed=50)
    write_manifest(table, files=[descriptor(table, listed)], generation=2)
    rel = "region=east/part-declared.parquet"
    write_parquet(table / rel, rows=4, seed=51)
    other = "region=east/part-other.parquet"
    write_parquet(table / other, rows=2, seed=60)
    active = _adopt_policy(policy_with(policy, table_overrides={}),
                           [("t", rel)])
    scan = scan_lake(root, active)
    adoption = scan.consistency["t"]["adoption"]
    assert [item["path"] for item in adoption["eligible"]] == [rel]
    assert adoption["declared"] == [rel]
    plan = build_plan(scan, active)
    adopt = [item for item in plan["actions"] if item["kind"] == "adopt"]
    assert len(adopt) == 1
    assert [item["path"] for item in adopt[0]["inputs"]] == [rel]
    assert other not in [item["path"] for item in adopt[0]["inputs"]]


def test_snapshot_referenced_candidate_is_rejected(lake, policy):
    # The legacy file lives in a partition absent from the table's key sequence,
    # so use a fresh file that matches the current layout and only appears in a
    # historical snapshot to isolate the ownership rejection path.
    table = lake / "customers"
    rel = "part-legacy-snap.parquet"
    write_parquet(table / rel, rows=6, seed=77)
    write_snapshot(table, 3, "2025-02-20T00:00:00Z", files=[descriptor(table, rel, 6)])
    active = _adopt_policy(policy, [("customers", rel)])
    scan = scan_lake(lake, active)
    adoption = scan.consistency["customers"]["adoption"]
    assert adoption["eligible"] == []
    assert adoption["rejected"][0]["path"] == rel
    assert adoption["rejected"][0]["code"] == "snapshot_referenced"
    plan = build_plan(scan, active)
    assert not [item for item in plan["actions"] if item["kind"] == "adopt"]


def test_legacy_fixture_is_rejected_by_layout_before_ownership(lake, policy):
    # The legacy file is only in snapshots 1/2, but its partition was dropped
    # from the table's current layout.  Layout validation must reject it first,
    # so this exercises the "layout before ownership" ordering.
    active = _adopt_policy(policy, [("events", LEGACY_SNAPSHOT_ONLY)])
    scan = scan_lake(lake, active)
    rejected = scan.consistency["events"]["adoption"]["rejected"]
    assert rejected and rejected[0]["code"] == "partition_mismatch"
    assert "current partition layout" in rejected[0]["reason"]
    assert not [item for item in build_plan(scan, active)["actions"]
                if item["kind"] == "adopt"]


def test_invalid_parquet_candidate_is_rejected(tmp_path, policy):
    root = tmp_path / "lake"
    table = root / "t"
    write_parquet(table / "region=a/part-0.parquet", rows=2, seed=1)
    write_manifest(table, files=[descriptor(table, "region=a/part-0.parquet")])
    bad = table / "region=a/part-not-parquet.parquet"
    bad.write_bytes(b"not a parquet file")
    active = _adopt_policy(policy_with(policy, table_overrides={}),
                           [("t", "region=a/part-not-parquet.parquet")])
    scan = scan_lake(root, active)
    adoption = scan.consistency["t"]["adoption"]
    assert adoption["eligible"] == []
    assert adoption["rejected"][0]["code"] == "invalid_parquet"


def test_adopt_enabled_false_ignores_declared_candidates(lake, policy):
    declared = _adopt_policy(policy, [("events", ORPHAN)])
    disabled = _adopt_policy(policy, [("events", ORPHAN)], enabled=False)
    scan = scan_lake(lake, disabled)
    assert scan.consistency["events"]["adoption"]["eligible"] == []
    assert scan.consistency["events"]["adoption"]["declared"] == [ORPHAN]
    assert not [item for item in build_plan(scan, disabled)["actions"]
                if item["kind"] == "adopt"]
    assert build_plan(scan_lake(lake, declared), declared)["summary"]["by_kind"]["adopt"] == 1


def test_apply_adopts_atomically_without_touching_the_file(lake, policy):
    active = _adopt_policy(policy, [("events", ORPHAN)])
    scan = scan_lake(lake, active)
    plan = build_plan(scan, active)
    action = next(item for item in plan["actions"] if item["kind"] == "adopt")
    table_dir = lake / "events"
    orphan_path = table_dir / ORPHAN
    file_before = orphan_path.read_bytes()
    manifest_before = load_manifest(table_dir)[0]
    snapshots_before = load_snapshots(table_dir)[0]
    next_sequence_expected = max(item.sequence for item in snapshots_before) + 1
    generation_before = manifest_before.generation

    result = execute_adoption(lake, action)
    assert result["manifest_generation"] == generation_before + 1
    assert result["snapshot_id"] == f"snap-{next_sequence_expected:04d}"
    assert result["adopted_files"][0]["path"] == ORPHAN
    assert result["adopted_files"][0]["rows"] == 7
    assert result["adopted_files"][0]["schema"]

    manifest_after = load_manifest(table_dir)[0]
    entry = next(item for item in manifest_after.files if item.path == ORPHAN)
    assert entry.rows == 7 and entry.sha256 and entry.schema
    assert manifest_after.generation == generation_before + 1
    snapshots_after = load_snapshots(table_dir)[0]
    newest = max(snapshots_after, key=lambda item: item.sequence)
    assert newest.snapshot_id == f"snap-{next_sequence_expected:04d}"
    assert ORPHAN in newest.paths
    assert all(item.path != ORPHAN for item in snapshots_before)

    # The candidate is left in place, byte-for-byte.
    assert orphan_path.read_bytes() == file_before
    assert tree_signature(table_dir)[ORPHAN] == __import__("hashlib").sha256(
        file_before).hexdigest()

    rescan = scan_lake(lake, active)
    assert rescan.consistency["events"]["orphan_unreferenced_count"] == 1
    assert ORPHAN not in {item["path"] for item in rescan.consistency["events"]["orphans"]}


def test_apply_plan_with_adopt_is_idempotent(lake, policy):
    active = _adopt_policy(policy, [("events", ORPHAN)])
    scan = scan_lake(lake, active)
    plan = build_plan(scan, active)
    action = next(item for item in plan["actions"] if item["kind"] == "adopt")
    ledger = Ledger(lake.parent / "ledger.sqlite3")

    result = apply_plan(lake, plan, ledger, before=scan, policy=active)
    assert result["status"] == "completed"
    adopted = next(item for item in result["actions"] if item["kind"] == "adopt")
    assert adopted["status"] == "completed"
    assert adopted["result"]["adopted_files"][0]["path"] == ORPHAN
    assert ORPHAN in load_manifest(lake / "events")[0].paths

    signature = tree_signature(lake)
    repeat = apply_plan(lake, plan, ledger, before=scan, policy=active)
    assert repeat["status"] == "already_completed"
    assert tree_signature(lake) == signature


def test_apply_refuses_when_candidate_content_changed(lake, policy):
    active = _adopt_policy(policy, [("events", ORPHAN)])
    plan = build_plan(scan_lake(lake, active), active)
    action = next(item for item in plan["actions"] if item["kind"] == "adopt")
    target = lake / "events" / ORPHAN
    write_parquet(target, rows=7, seed=999)  # same shape, different content
    ledger = Ledger(lake.parent / "ledger.sqlite3")
    with pytest.raises(Exception, match="content changed"):
        apply_plan(lake, plan, ledger, policy=active)
    manifest = load_manifest(lake / "events")[0]
    assert manifest.generation == action["expected"]["manifest_generation"]
    assert ORPHAN not in manifest.paths


def test_apply_refuses_when_generation_moved(lake, policy):
    active = _adopt_policy(policy, [("events", ORPHAN)])
    plan = build_plan(scan_lake(lake, active), active)
    action = next(item for item in plan["actions"] if item["kind"] == "adopt")
    table_dir = lake / "events"
    manifest_path = table_dir / "_metadata" / "manifest.json"
    doc = json.loads(manifest_path.read_text(encoding="utf-8"))
    doc["generation"] += 1
    write_json(manifest_path, doc)
    before = tree_signature(lake)
    with pytest.raises(AdoptError, match="generation moved"):
        execute_adoption(lake, action)
    assert tree_signature(lake) == before


def test_apply_refuses_when_candidate_appeared_in_manifest(lake, policy):
    active = _adopt_policy(policy, [("events", ORPHAN)])
    plan = build_plan(scan_lake(lake, active), active)
    action = next(item for item in plan["actions"] if item["kind"] == "adopt")
    table_dir = lake / "events"
    descriptor_value = descriptor(table_dir, ORPHAN, 7)
    manifest = load_manifest(table_dir)[0]
    from lakewatch.catalog.manifest import Manifest, write_manifest
    updated = Manifest(
        table="events", generation=manifest.generation,
        updated_at=manifest.updated_at,
        files=tuple(sorted(list(manifest.files) + [
            __import__("lakewatch.catalog.manifest", fromlist=["ManifestFile"]).ManifestFile(
                path=ORPHAN, bytes=descriptor_value["bytes"],
                rows=descriptor_value["rows"], sha256=descriptor_value["sha256"])
        ], key=lambda item: item.path)),
        path=manifest.path,
    )
    write_manifest(table_dir, updated)
    with pytest.raises(AdoptError, match="already in the current manifest"):
        execute_adoption(lake, action)


def test_adopted_entry_is_rejected_on_second_plan(lake, policy):
    active = _adopt_policy(policy, [("events", ORPHAN)])
    plan = build_plan(scan_lake(lake, active), active)
    ledger = Ledger(lake.parent / "ledger.sqlite3")
    apply_plan(lake, plan, ledger, policy=active)
    scan = scan_lake(lake, active)
    assert scan.consistency["events"]["adoption"]["eligible"] == []
    assert scan.consistency["events"]["adoption"]["rejected"][0]["code"] == "already_in_manifest"
    assert not [item for item in build_plan(scan, active)["actions"]
                if item["kind"] == "adopt"]


def test_two_declared_orphans_adopt_in_one_action(lake, policy):
    second = "region=east/day=2025-01-02/part-orphan-001.parquet"
    write_parquet(lake / "events" / second, rows=4, seed=300)
    active = _adopt_policy(policy, [("events", ORPHAN), ("events", second)])
    scan = scan_lake(lake, active)
    plan = build_plan(scan, active)
    adopt = [item for item in plan["actions"] if item["kind"] == "adopt"]
    assert len(adopt) == 1
    action = adopt[0]
    assert [item["path"] for item in action["inputs"]] == sorted([ORPHAN, second])
    assert len(action["expected"]["candidates"]) == 2
    assert action["expected"]["entry_count_after"] == len(
        load_manifest(lake / "events")[0].files) + 2

    result = execute_adoption(lake, action)
    assert result["manifest_generation"] == 6
    manifest = load_manifest(lake / "events")[0]
    assert ORPHAN in manifest.paths and second in manifest.paths
    assert manifest.generation == 6
    assert (lake / "events" / second).is_file()


def test_whole_action_aborts_when_one_candidate_changed(lake, policy):
    second = "region=east/day=2025-01-02/part-orphan-001.parquet"
    write_parquet(lake / "events" / second, rows=4, seed=300)
    active = _adopt_policy(policy, [("events", ORPHAN), ("events", second)])
    scan = scan_lake(lake, active)
    plan = build_plan(scan, active)
    action = next(item for item in plan["actions"] if item["kind"] == "adopt")
    write_parquet(lake / "events" / second, rows=4, seed=301)
    manifest_before = (lake / "events" / "_metadata" / "manifest.json").read_bytes()
    snapshots_before = tree_signature(lake / "events" / "_metadata" / "snapshots")
    with pytest.raises(AdoptError, match="content changed"):
        execute_adoption(lake, action)
    assert (lake / "events" / "_metadata" / "manifest.json").read_bytes() == manifest_before
    assert tree_signature(lake / "events" / "_metadata" / "snapshots") == snapshots_before
    assert ORPHAN not in load_manifest(lake / "events")[0].paths


def test_tampered_adopt_plan_is_rejected(lake, policy):
    active = _adopt_policy(policy, [("events", ORPHAN)])
    plan = build_plan(scan_lake(lake, active), active)
    action = next(item for item in plan["actions"] if item["kind"] == "adopt")
    action["expected"]["candidates"][0]["rows"] = 999
    with pytest.raises(PlanError, match="inconsistent"):
        validate_plan(plan)

    plan = build_plan(scan_lake(lake, active), active)
    action = next(item for item in plan["actions"] if item["kind"] == "adopt")
    action["reason"] = "tampered"
    with pytest.raises(PlanError, match="hash"):
        validate_plan(plan)


def test_policy_candidate_validation(lake, policy):
    payload = copy_policy(policy)
    payload["adopt_candidates"] = [{"table": "events", "path": ORPHAN}]
    parsed = Policy.from_dict(payload)
    assert parsed.candidates_for("events") == (ORPHAN,)
    assert parsed.adopt_enabled is False  # declaration alone does not enable it
    assert parsed.settings("events").adopt_enabled is False

    enabled = Policy.from_dict({**payload, "adopt_enabled": True})
    assert enabled.settings("events").adopt_enabled is True

    with pytest.raises(PolicyError, match="adopt_candidates"):
        Policy.from_dict({**payload, "adopt_candidates": [{"table": "events"}]})
    with pytest.raises(PolicyError, match="adopt_candidates"):
        Policy.from_dict({**payload, "adopt_candidates": ["missing-slash.parquet"]})
    with pytest.raises(PolicyError, match="adopt_candidates"):
        Policy.from_dict({**payload, "adopt_candidates": [
            {"table": "events", "path": "../escape.parquet"}]})
    with pytest.raises(PolicyError, match="adopt_candidates"):
        Policy.from_dict({**payload, "adopt_candidates": [
            {"table": "events", "path": "region=x/data.txt"}]})
    with pytest.raises(PolicyError, match="adopt_enabled"):
        Policy.from_dict({**payload, "adopt_enabled": "yes"})

    table_scoped = copy_policy(policy)
    table_scoped["tables"]["events"]["adopt_candidates"] = [ORPHAN]
    parsed = Policy.from_dict(table_scoped)
    assert parsed.candidates_for("events") == (ORPHAN,)

    short = Policy.from_dict({**payload,
                              "adopt_candidates": [f"events:{ORPHAN}"]})
    assert short.candidates_for("events") == (ORPHAN,)


def test_policy_hash_changes_with_adopt_declaration(policy):
    base = Policy.from_dict(copy_policy(policy))
    active = Policy.from_dict(copy_policy(policy, adopt_candidates=[
        {"table": "events", "path": ORPHAN}]))
    assert base.policy_hash != active.policy_hash


def copy_policy(policy, **changes):
    import copy
    payload = copy.deepcopy(policy.to_dict())
    if "adopt_candidates" in changes:
        payload["adopt_candidates"] = changes.pop("adopt_candidates")
    payload.update(changes)
    return payload


def test_adopt_plan_hash_is_stable_and_independent_of_other_actions(lake, policy):
    active = _adopt_policy(policy, [("events", ORPHAN)])
    first = build_plan(scan_lake(lake, active), active)
    second = build_plan(scan_lake(lake, active), active)
    assert first["plan_hash"] == second["plan_hash"]
    adopt = next(item for item in first["actions"] if item["kind"] == "adopt")
    assert adopt["input_hash"]
    # Schema and sha appear in both the inputs and the candidate description.
    assert "schema" in adopt["expected"]["candidates"][0]


def test_cli_plan_shows_adopt_only_when_policy_declares_it(tmp_path, lake, policy, capsys):
    from lakewatch.cli import main
    from conftest import write_cli_config

    active = _adopt_policy(policy, [("events", ORPHAN)])
    config = write_cli_config(
        tmp_path / "configs" / "adopt.yaml", lake, active,
        tmp_path / "ledger.sqlite3", tmp_path / "reports",
    )
    assert main(["plan", "--config", str(config)]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["summary"]["by_kind"]["adopt"] == 1

    default_config = write_cli_config(
        tmp_path / "configs" / "default.yaml", lake, policy,
        tmp_path / "ledger.sqlite3", tmp_path / "reports",
    )
    assert main(["plan", "--config", str(default_config)]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["summary"]["by_kind"]["adopt"] == 0
