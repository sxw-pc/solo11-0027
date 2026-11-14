"""Clustered-merge compaction: explicit sort contracts end to end."""
import copy
import hashlib
from collections import Counter

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakewatch.catalog.manifest import load_manifest
from lakewatch.catalog.snapshot import load_snapshots
from lakewatch.executor import ApplyError, apply_plan
from lakewatch.ledger.db import Ledger
from lakewatch.planner.plan import PlanError, build_plan, validate_plan
from lakewatch.planner.policy import Policy
from lakewatch.scanners import scan_lake

from conftest import descriptor, tree_signature, write_manifest, write_snapshot

TABLE = "metrics"
PARTITION = "region=a"
CONTRACT = [
    {"name": "k", "direction": "desc", "nulls": "first"},
    {"name": "v", "direction": "asc", "nulls": "last"},
]


class _Desc:
    def __init__(self, value):
        self.value = value

    def __lt__(self, other):
        return self.value > other.value

    def __eq__(self, other):
        return self.value == other.value


def _expected_rows(files, columns):
    """Reference ordering: stable sort of plan-ordered rows by the contract."""
    rows = []
    for name in sorted(files):
        rows.extend(files[name].to_pylist())

    def key(row):
        parts = []
        for col in columns:
            value = row[col["name"]]
            null_rank = (value is not None) if col["nulls"] == "first" else (value is None)
            if value is None:
                parts.append((null_rank, 0))
            elif col["direction"] == "desc":
                parts.append((null_rank, _Desc(value)))
            else:
                parts.append((null_rank, value))
        return tuple(parts)

    return sorted(rows, key=key)


def _policy_dict(columns=None):
    raw = {
        "small_file_bytes": 1_000_000,
        "max_files_per_partition": 50,
        "max_files_listed": 50,
        "max_actions": 50,
        "compact_target_bytes": 16 * 1024 * 1024,
        "compact_max_inputs": 16,
        "retention": {
            "keep_last_snapshots": 5,
            "keep_days": 30,
            "as_of": "2025-03-10T00:00:00Z",
        },
    }
    if columns is not None:
        raw["tables"] = {TABLE: {"cluster": {"columns": columns}}}
    return raw


def _make_lake(root, files, compression="snappy"):
    table_dir = root / TABLE
    descriptors = []
    for name in sorted(files):
        rel = f"{PARTITION}/{name}"
        path = table_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(files[name], path, compression=compression)
        descriptors.append(descriptor(table_dir, rel, rows=files[name].num_rows))
    write_manifest(table_dir, descriptors, generation=1, table=TABLE)
    write_snapshot(table_dir, 1, "2025-03-01T00:00:00Z", descriptors, table=TABLE)
    return table_dir


def _sample_files():
    return {
        "part-0000.parquet": pa.table(
            {
                "k": pa.array([3, None, 1, 2, 2], pa.int64()),
                "v": pa.array(["c", "a", None, "b", "a"]),
                "w": pa.array([1.5, 2.5, 3.5, 4.5, 5.5]),
            }
        ),
        "part-0001.parquet": pa.table(
            {
                "k": pa.array([None, 2, 1, 3], pa.int64()),
                "v": pa.array(["b", None, "a", "d"]),
                "w": pa.array([6.5, 7.5, 8.5, 9.5]),
            }
        ),
        "part-0002.parquet": pa.table(
            {
                "k": pa.array([2, None, 3], pa.int64()),
                "v": pa.array(["a", "c", None]),
                "w": pa.array([10.5, 11.5, 12.5]),
            }
        ),
    }


def _cluster_plan(lake_root, columns=CONTRACT):
    policy = Policy.from_dict(_policy_dict(columns))
    scan = scan_lake(lake_root, policy)
    return build_plan(scan, policy), policy


def _compact_action(plan):
    (action,) = [item for item in plan["actions"] if item["kind"] == "compact"]
    return action


def _output_path(lake_root, action):
    return lake_root / TABLE / action["expected"]["output_path"]


def test_clustered_compaction_sorts_and_preserves_rows(tmp_path):
    lake_root = tmp_path / "lake"
    files = _sample_files()
    table_dir = _make_lake(lake_root, files)
    plan, _ = _cluster_plan(lake_root)
    action = _compact_action(plan)
    assert action["cluster"] == {"columns": CONTRACT}
    input_bytes = sum(item["bytes"] for item in action["inputs"])
    input_rows = sum(item["rows"] for item in action["inputs"])
    ledger = Ledger(tmp_path / "ledger.sqlite3")

    result = apply_plan(lake_root, plan, ledger)

    assert result["status"] == "completed"
    output = _output_path(lake_root, action)
    assert output.stat().st_size == input_bytes
    produced = pq.read_table(output).to_pylist()
    assert produced == _expected_rows(files, CONTRACT)
    assert len(produced) == input_rows
    # Every input row survives exactly once, including duplicate sort keys.
    multiset = Counter(
        tuple(sorted((key, repr(value)) for key, value in row.items()))
        for row in produced
    )
    expected_multiset = Counter(
        tuple(sorted((key, repr(value)) for key, value in row.items()))
        for name in sorted(files)
        for row in files[name].to_pylist()
    )
    assert multiset == expected_multiset
    manifest, _ = load_manifest(table_dir)
    assert manifest.generation == 2
    assert action["expected"]["output_path"] in manifest.paths
    assert all(item["path"] not in manifest.paths for item in action["inputs"])
    assert len(load_snapshots(table_dir)[0]) == 2
    assert all(
        not (table_dir / item["path"]).exists() for item in action["inputs"]
    )

    signature = tree_signature(lake_root)
    repeat = apply_plan(lake_root, plan, ledger)
    assert repeat["status"] == "already_completed"
    assert repeat["skipped_actions"] == len(plan["actions"])
    assert tree_signature(lake_root) == signature


def test_cluster_contract_changes_action_id_and_plan_hash(tmp_path):
    files = _sample_files()
    plain_root = tmp_path / "plain"
    clustered_root = tmp_path / "clustered"
    _make_lake(plain_root, files)
    _make_lake(clustered_root, files)
    plain_policy = Policy.from_dict(_policy_dict())
    plain = build_plan(scan_lake(plain_root, plain_policy), plain_policy)
    clustered, clustered_policy = _cluster_plan(clustered_root)
    clustered_action = _compact_action(clustered)
    plain_action = _compact_action(plain)

    assert "cluster" not in plain_action
    assert clustered_action["cluster"] == {"columns": CONTRACT}
    assert clustered_action["action_id"] != plain_action["action_id"]
    assert clustered["plan_hash"] != plain["plan_hash"]
    assert clustered_policy.policy_hash != plain_policy.policy_hash
    again, _ = _cluster_plan(clustered_root)
    assert again["plan_hash"] == clustered["plan_hash"]
    validate_plan(clustered)


def test_validate_plan_rejects_tampered_cluster_contract(tmp_path):
    lake_root = tmp_path / "lake"
    _make_lake(lake_root, _sample_files())
    plan, _ = _cluster_plan(lake_root)

    tampered = copy.deepcopy(plan)
    tampered["actions"][0]["cluster"]["columns"][0]["direction"] = "asc"
    with pytest.raises(PlanError, match="hash"):
        validate_plan(tampered)

    invalid = copy.deepcopy(plan)
    invalid["actions"][0]["cluster"]["columns"][0]["nulls"] = "middle"
    with pytest.raises(PlanError, match="nulls"):
        validate_plan(invalid)

    not_canonical = copy.deepcopy(plan)
    not_canonical["actions"][0]["cluster"]["columns"][0]["extra"] = True
    with pytest.raises(PlanError):
        validate_plan(not_canonical)


def test_clustered_output_is_reproducible_across_disk_order(tmp_path):
    files = _sample_files()
    results = []
    plans = []
    for name, order in (("forward", sorted(files)), ("reverse", sorted(files, reverse=True))):
        lake_root = tmp_path / name
        table_dir = lake_root / TABLE
        descriptors = []
        for file_name in order:  # create files in different disk order
            rel = f"{PARTITION}/{file_name}"
            path = table_dir / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(files[file_name], path, compression="snappy")
        for file_name in sorted(files):
            rel = f"{PARTITION}/{file_name}"
            descriptors.append(
                descriptor(table_dir, rel, rows=files[file_name].num_rows)
            )
        write_manifest(table_dir, descriptors, generation=1, table=TABLE)
        write_snapshot(table_dir, 1, "2025-03-01T00:00:00Z", descriptors, table=TABLE)
        plan, _ = _cluster_plan(lake_root)
        plans.append(plan)
        ledger = Ledger(tmp_path / f"ledger-{name}.sqlite3")
        results.append(apply_plan(lake_root, plan, ledger))
    assert plans[0]["plan_hash"] == plans[1]["plan_hash"]
    outputs = []
    for name, plan in zip(("forward", "reverse"), plans):
        action = _compact_action(plan)
        outputs.append((tmp_path / name / TABLE / action["expected"]["output_path"]).read_bytes())
    assert outputs[0] == outputs[1]
    assert results[0]["actions"][0]["output_hash"] == results[1]["actions"][0]["output_hash"]


def _assert_untouched(lake_root, table_dir, before):
    assert tree_signature(lake_root) == before
    manifest, _ = load_manifest(table_dir)
    assert manifest.generation == 1
    assert len(load_snapshots(table_dir)[0]) == 1
    assert not list(table_dir.rglob("*.tmp"))
    assert not list(table_dir.rglob("part-compact-*.parquet"))


def test_missing_sort_column_fails_before_any_mutation(tmp_path):
    lake_root = tmp_path / "lake"
    table_dir = _make_lake(lake_root, _sample_files())
    columns = [{"name": "nope", "direction": "asc", "nulls": "first"}]
    plan, _ = _cluster_plan(lake_root, columns)
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    before = tree_signature(lake_root)

    with pytest.raises(ApplyError, match="not in the schema"):
        apply_plan(lake_root, plan, ledger)
    _assert_untouched(lake_root, table_dir, before)
    assert ledger.get_plan(plan["plan_hash"])["status"] == "failed"

    with pytest.raises(ApplyError, match="not in the schema"):
        apply_plan(lake_root, plan, ledger)
    _assert_untouched(lake_root, table_dir, before)


def test_incomparable_column_types_across_inputs_fail_before_mutation(tmp_path):
    lake_root = tmp_path / "lake"
    files = {
        "part-0000.parquet": pa.table(
            {"k": pa.array([1, 2], pa.int64()), "v": pa.array(["a", "b"])}
        ),
        "part-0001.parquet": pa.table(
            {"k": pa.array(["x", "y"]), "v": pa.array(["c", "d"])}
        ),
    }
    table_dir = _make_lake(lake_root, files)
    columns = [{"name": "k", "direction": "asc", "nulls": "first"}]
    plan, _ = _cluster_plan(lake_root, columns)
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    before = tree_signature(lake_root)

    with pytest.raises(ApplyError, match="schema mismatch"):
        apply_plan(lake_root, plan, ledger)
    _assert_untouched(lake_root, table_dir, before)

    with pytest.raises(ApplyError, match="schema mismatch"):
        apply_plan(lake_root, plan, ledger)
    _assert_untouched(lake_root, table_dir, before)


def test_non_orderable_sort_column_fails_before_mutation(tmp_path):
    lake_root = tmp_path / "lake"
    files = {
        "part-0000.parquet": pa.table(
            {
                "k": pa.array([[1, 2], [3]], pa.list_(pa.int64())),
                "v": pa.array(["a", "b"]),
            }
        ),
        "part-0001.parquet": pa.table(
            {
                "k": pa.array([[4], [5, 6]], pa.list_(pa.int64())),
                "v": pa.array(["c", "d"]),
            }
        ),
    }
    table_dir = _make_lake(lake_root, files)
    columns = [{"name": "k", "direction": "asc", "nulls": "first"}]
    plan, _ = _cluster_plan(lake_root, columns)
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    before = tree_signature(lake_root)

    with pytest.raises(ApplyError, match="non-orderable"):
        apply_plan(lake_root, plan, ledger)
    _assert_untouched(lake_root, table_dir, before)


def test_byte_budget_overflow_is_rejected_safely(tmp_path):
    lake_root = tmp_path / "lake"
    # Incompressible values: gzip inputs stay compact, but the executor writes
    # snappy, so the rewritten file cannot fit the input byte budget.
    rows = 2000
    values = [
        hashlib.sha256(f"{index}".encode()).hexdigest()[:32] for index in range(rows)
    ]
    half = rows // 2
    files = {
        "part-0000.parquet": pa.table(
            {
                "k": pa.array(list(range(half)), pa.int64()),
                "v": pa.array(values[:half]),
            }
        ),
        "part-0001.parquet": pa.table(
            {
                "k": pa.array(list(range(half, rows)), pa.int64()),
                "v": pa.array(values[half:]),
            }
        ),
    }
    table_dir = _make_lake(lake_root, files, compression="gzip")
    columns = [{"name": "k", "direction": "asc", "nulls": "first"}]
    plan, _ = _cluster_plan(lake_root, columns)
    action = _compact_action(plan)
    budget = sum(item["bytes"] for item in action["inputs"])
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    before = tree_signature(lake_root)

    with pytest.raises(ApplyError, match="byte budget"):
        apply_plan(lake_root, plan, ledger)
    _assert_untouched(lake_root, table_dir, before)
    assert ledger.get_plan(plan["plan_hash"])["status"] == "failed"

    # A retry sees the same untouched inputs and fails the same way.
    with pytest.raises(ApplyError, match="byte budget"):
        apply_plan(lake_root, plan, ledger)
    _assert_untouched(lake_root, table_dir, before)
    assert sum(item["bytes"] for item in action["inputs"]) == budget


def test_plan_without_cluster_has_no_contract_keys(lake, policy):
    plan = build_plan(scan_lake(lake, policy), policy)
    assert plan["actions"]
    assert all("cluster" not in item for item in plan["actions"])
