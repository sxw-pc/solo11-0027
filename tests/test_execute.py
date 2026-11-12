import pyarrow.parquet as pq

from lakewatch.catalog.manifest import load_manifest
from lakewatch.catalog.snapshot import load_snapshots
from lakewatch.executor import apply_plan
from lakewatch.executor.compact import execute_compaction
from lakewatch.ledger.db import Ledger
from lakewatch.planner.plan import build_plan
from lakewatch.scanners import scan_lake

from conftest import tree_signature


def test_compaction_reduces_files_and_conserves_bytes(lake, policy):
    before = scan_lake(lake, policy)
    plan = build_plan(before, policy)
    action = next(
        item for item in plan["actions"]
        if item["kind"] == "compact" and item["table"] == "orders"
        and item["partition"] == "region=eu"
    )
    table_dir = lake / "orders"
    partition_dir = table_dir / "region=eu"
    input_bytes = sum(
        (table_dir / item["path"]).stat().st_size for item in action["inputs"]
    )
    before_manifest = load_manifest(table_dir)[0]
    before_snapshots = len(load_snapshots(table_dir)[0])
    before_count = len(list(partition_dir.glob("*.parquet")))

    result = execute_compaction(lake, action)

    output = table_dir / result["outputs"][0]["path"]
    assert output.stat().st_size == input_bytes == result["bytes_out"]
    assert result["bytes_in"] == result["bytes_out"]
    assert len(list(partition_dir.glob("*.parquet"))) == 1 < before_count
    assert all(not (table_dir / item["path"]).exists() for item in action["inputs"])
    assert pq.read_table(output).num_rows == action["expected"]["rows"]
    after_manifest = load_manifest(table_dir)[0]
    assert after_manifest.total_bytes == before_manifest.total_bytes
    assert after_manifest.generation == before_manifest.generation + 1
    assert len(load_snapshots(table_dir)[0]) == before_snapshots + 1


def test_full_apply_vacuum_repair_and_idempotence(lake, policy):
    before = scan_lake(lake, policy)
    plan = build_plan(before, policy)
    ledger = Ledger(lake.parent / "ledger.sqlite3")
    result = apply_plan(lake, plan, ledger, before=before, policy=policy)
    assert result["status"] == "completed"
    assert result["completed_actions"] == 10
    assert result["skipped_actions"] == 0

    vacuum_actions = [item for item in plan["actions"] if item["kind"] == "vacuum"]
    assert vacuum_actions
    for action in vacuum_actions:
        for snapshot_id in action["expected"]["delete_snapshot_ids"]:
            path = lake / action["table"] / "_metadata" / "snapshots" / f"{snapshot_id}.json"
            assert not path.exists()

    after = scan_lake(lake, policy)
    assert after.summary()["missing_count"] == 0
    assert after.summary()["orphan_count"] < before.summary()["orphan_count"]
    manifest = load_manifest(lake / "events")[0]
    assert "region=west/day=2025-01-03/part-missing-000.parquet" not in manifest.paths

    signature = tree_signature(lake)
    repeat = apply_plan(lake, plan, ledger, before=before, policy=policy)
    assert repeat["status"] == "already_completed"
    assert repeat["completed_actions"] == 0
    assert repeat["skipped_actions"] == len(plan["actions"])
    assert tree_signature(lake) == signature

    tasks = ledger.list_tasks(plan["plan_hash"])
    assert len(tasks) == len(plan["actions"])
    assert {item["status"] for item in tasks} == {"completed"}
    assert all(item["output_hash"] for item in tasks)
    assert all(item["duration_ms"] >= 0 for item in tasks)
    assert ledger.get_plan(plan["plan_hash"])["status"] == "completed"
