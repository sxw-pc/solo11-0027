import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakewatch.executor import ApplyError, apply_plan
from lakewatch.executor import compact as compact_module
from lakewatch.executor.compact import (
    CONTENT_CHECK_ALGORITHM,
    CompactionError,
    _encode_value,
    _multiset_digest,
    _row_multiset,
    execute_compaction,
    verify_content_parity,
)
from lakewatch.ledger.db import Ledger
from lakewatch.planner.plan import build_plan
from lakewatch.scanners import scan_lake

from conftest import descriptor, write_manifest


def _build_dup_lake(root):
    """A two-file table whose rows contain deliberate duplicates."""
    table_dir = root / "dup"
    table_dir.mkdir(parents=True)
    first = pa.table(
        {
            "id": pa.array([1, 1, 2], pa.int64()),
            "label": pa.array(["a", "a", "b"], pa.string()),
            "amount": pa.array([1.5, 1.5, 2.5], pa.float64()),
        }
    )
    second = pa.table(
        {
            "id": pa.array([3, 3], pa.int64()),
            "label": pa.array(["c", "c"], pa.string()),
            "amount": pa.array([3.5, 3.5], pa.float64()),
        }
    )
    pq.write_table(first, table_dir / "part-0000.parquet")
    pq.write_table(second, table_dir / "part-0001.parquet")
    files = [
        descriptor(table_dir, "part-0000.parquet", rows=3),
        descriptor(table_dir, "part-0001.parquet", rows=2),
    ]
    write_manifest(table_dir, files=files, generation=1)
    action = {
        "action_id": "compact-dup",
        "kind": "compact",
        "table": "dup",
        "partition": "",
        "inputs": [
            {"path": item["path"], "sha256": item["sha256"]} for item in files
        ],
        "expected": {"output_path": "part-merged.parquet", "rows": 5},
    }
    return table_dir, action


def _write_tampered_writer(order_for):
    real_writer = compact_module._write_exact_parquet

    def fake(table, destination, target_bytes, action_id):
        return real_writer(
            table.take(order_for(table.num_rows)), destination, target_bytes, action_id
        )

    return fake


def test_encode_value_distinguishes_null_empty_binary_and_types():
    values = [
        None, "", b"", 0, 0.0, False, "0", b"0", 1, 1.0, True, "1",
    ]
    encodings = {_encode_value(value) for value in values}
    assert len(encodings) == len(values)


def test_encode_value_distinguishes_float_specials():
    specials = [0.0, -0.0, float("nan"), float("inf"), float("-inf"), 1.0, -1.0]
    encodings = {_encode_value(value) for value in specials}
    assert len(encodings) == len(specials)


def test_encode_value_nested_values_are_deterministic():
    assert _encode_value({"a": 1, "b": [None, ""]}) == _encode_value(
        {"b": [None, ""], "a": 1}
    )
    assert _encode_value([1, "1"]) != _encode_value(["1", 1])
    assert _encode_value((None,)) != _encode_value([None, None])


def test_encode_value_rejects_unknown_types():
    with pytest.raises(CompactionError, match="cannot fingerprint"):
        _encode_value(object())


def test_row_multiset_ignores_row_order():
    table = pa.table(
        {
            "id": pa.array([1, 2, 2, 3], pa.int64()),
            "label": pa.array(["x", "", "y", None], pa.string()),
            "blob": pa.array([b"", b"\x00", b"\x00", None], pa.binary()),
            "amount": pa.array([0.0, -0.0, float("nan"), float("inf")], pa.float64()),
            "flag": pa.array([True, False, True, None], pa.bool_()),
        }
    )
    shuffled = table.take([3, 1, 0, 2])
    assert _row_multiset(table) == _row_multiset(shuffled)
    assert _multiset_digest(_row_multiset(table)) == _multiset_digest(
        _row_multiset(shuffled)
    )


def test_row_multiset_tracks_duplicate_multiplicity():
    left = pa.table({"id": pa.array([1, 1, 2], pa.int64())})
    right = pa.table({"id": pa.array([1, 2, 2], pa.int64())})
    assert _row_multiset(left) != _row_multiset(right)
    assert _multiset_digest(_row_multiset(left)) != _multiset_digest(
        _row_multiset(right)
    )


def test_verify_content_parity_passes_for_exact_rewrite(tmp_path):
    table = pa.table(
        {
            "id": pa.array([1, 1, 2], pa.int64()),
            "label": pa.array(["a", "a", None], pa.string()),
        }
    )
    path = tmp_path / "candidate.parquet"
    pq.write_table(table, path)
    check = verify_content_parity(table, path)
    assert check["status"] == "passed"
    assert check["algorithm"] == CONTENT_CHECK_ALGORITHM
    assert check["rows_in"] == check["rows_out"] == 3
    assert check["distinct_rows"] == 2
    assert check["input_digest"] == check["output_digest"]


def test_verify_content_parity_allows_reordered_rows(tmp_path):
    table = pa.table({"id": pa.array([1, 2, 3, 4], pa.int64())})
    path = tmp_path / "reordered.parquet"
    pq.write_table(table.take([3, 1, 0, 2]), path)
    check = verify_content_parity(table, path)
    assert check["status"] == "passed"
    assert check["input_digest"] == check["output_digest"]


def test_verify_content_parity_rejects_value_swap(tmp_path):
    table = pa.table({"id": pa.array([1, 2, 3], pa.int64())})
    path = tmp_path / "swapped.parquet"
    pq.write_table(pa.table({"id": pa.array([1, 2, 4], pa.int64())}), path)
    with pytest.raises(CompactionError, match="row multiset differs"):
        verify_content_parity(table, path)


def test_verify_content_parity_rejects_multiplicity_change(tmp_path):
    table = pa.table({"id": pa.array([1, 1, 2], pa.int64())})
    path = tmp_path / "multiplicity.parquet"
    pq.write_table(pa.table({"id": pa.array([1, 2, 2], pa.int64())}), path)
    with pytest.raises(CompactionError, match="row multiset differs"):
        verify_content_parity(table, path)


def test_verify_content_parity_rejects_schema_field_reorder(tmp_path):
    table = pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "label": pa.array(["x"], pa.string()),
        }
    )
    swapped = pa.table(
        {
            "label": pa.array(["x"], pa.string()),
            "id": pa.array([1], pa.int64()),
        }
    )
    path = tmp_path / "schema-swapped.parquet"
    pq.write_table(swapped, path)
    with pytest.raises(CompactionError, match="schema differs"):
        verify_content_parity(table, path)


def test_null_empty_and_binary_survive_parquet_roundtrip(tmp_path):
    table = pa.table(
        {
            "s": pa.array([None, "", "x"], pa.string()),
            "b": pa.array([None, b"", b"x"], pa.binary()),
        }
    )
    path = tmp_path / "nullable.parquet"
    pq.write_table(table, path)
    check = verify_content_parity(table, path)
    assert check["status"] == "passed"
    assert check["distinct_rows"] == 3

    nulled = pa.table(
        {
            "s": pa.array([None, None, "x"], pa.string()),
            "b": pa.array([None, b"", b"x"], pa.binary()),
        }
    )
    other = tmp_path / "nulled.parquet"
    pq.write_table(nulled, other)
    with pytest.raises(CompactionError, match="row multiset differs"):
        verify_content_parity(table, other)


def test_float_specials_survive_parquet_roundtrip(tmp_path):
    table = pa.table(
        {
            "f": pa.array(
                [0.0, -0.0, float("nan"), float("inf"), float("-inf")], pa.float64()
            )
        }
    )
    path = tmp_path / "specials.parquet"
    pq.write_table(table, path)
    check = verify_content_parity(table, path)
    assert check["status"] == "passed"
    assert check["distinct_rows"] == 5


def test_execute_compaction_records_content_check(tmp_path):
    lake = tmp_path / "lake"
    table_dir, action = _build_dup_lake(lake)
    result = execute_compaction(lake, action)
    check = result["content_check"]
    assert check["status"] == "passed"
    assert check["algorithm"] == CONTENT_CHECK_ALGORITHM
    assert check["rows_in"] == check["rows_out"] == 5
    assert check["distinct_rows"] == 3
    assert check["input_digest"] == check["output_digest"]
    assert result["bytes_in"] == result["bytes_out"]
    produced = pq.read_table(table_dir / "part-merged.parquet")
    assert produced.num_rows == 5
    ids = produced.column("id").to_pylist()
    assert ids.count(1) == 2
    assert ids.count(3) == 2


def test_execute_compaction_accepts_reordered_rows(tmp_path, monkeypatch):
    lake = tmp_path / "lake"
    table_dir, action = _build_dup_lake(lake)
    monkeypatch.setattr(
        compact_module,
        "_write_exact_parquet",
        _write_tampered_writer(lambda rows: list(reversed(range(rows)))),
    )
    result = execute_compaction(lake, action)
    check = result["content_check"]
    assert check["status"] == "passed"
    assert check["input_digest"] == check["output_digest"]
    assert pq.read_table(table_dir / "part-merged.parquet").num_rows == 5


def test_execute_compaction_content_failure_preserves_state(tmp_path, monkeypatch):
    lake = tmp_path / "lake"
    table_dir, action = _build_dup_lake(lake)
    manifest_bytes = (table_dir / "_metadata" / "manifest.json").read_bytes()
    input_hashes = {item["path"]: item["sha256"] for item in action["inputs"]}
    # Drop row 0 and duplicate row 2: same row count, same byte budget, but
    # the duplicate multiplicities no longer match the inputs.
    monkeypatch.setattr(
        compact_module,
        "_write_exact_parquet",
        _write_tampered_writer(lambda rows: list(range(1, rows)) + [2]),
    )
    with pytest.raises(CompactionError, match="content check failed"):
        execute_compaction(lake, action)

    assert not (table_dir / "part-merged.parquet").exists()
    for rel, sha in input_hashes.items():
        path = table_dir / rel
        assert path.is_file()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == sha
    assert (table_dir / "_metadata" / "manifest.json").read_bytes() == manifest_bytes
    assert not (table_dir / "_metadata" / "snapshots").exists()


def test_apply_records_content_check_in_ledger(lake, policy, tmp_path):
    before = scan_lake(lake, policy)
    plan = build_plan(before, policy)
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    result = apply_plan(lake, plan, ledger, before=before, policy=policy)
    assert result["status"] == "completed"

    compact_outcomes = [
        item for item in result["actions"] if item["kind"] == "compact"
    ]
    assert compact_outcomes
    for outcome in compact_outcomes:
        assert outcome["content_check"] == "passed"
        check = outcome["result"]["content_check"]
        assert check["algorithm"] == CONTENT_CHECK_ALGORITHM
        assert check["rows_in"] == check["rows_out"]
        assert check["input_digest"] == check["output_digest"]

    tasks = {item["action_id"]: item for item in ledger.list_tasks(plan["plan_hash"])}
    for outcome in compact_outcomes:
        stored = json.loads(tasks[outcome["action_id"]]["result_json"])
        stored_check = stored["content_check"]
        assert stored_check["status"] == "passed"
        assert stored_check["input_digest"] == stored_check["output_digest"]


def test_apply_content_failure_goes_to_ledger(lake, policy, tmp_path, monkeypatch):
    before = scan_lake(lake, policy)
    plan = build_plan(before, policy)
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    # Every compaction output drops its first row and duplicates its second.
    monkeypatch.setattr(
        compact_module,
        "_write_exact_parquet",
        _write_tampered_writer(lambda rows: list(range(1, rows)) + [1]),
    )
    with pytest.raises(ApplyError, match="content check failed"):
        apply_plan(lake, plan, ledger, before=before, policy=policy)

    assert ledger.get_plan(plan["plan_hash"])["status"] == "failed"
    failed = [
        item for item in ledger.list_tasks(plan["plan_hash"])
        if item["status"] == "failed"
    ]
    assert len(failed) == 1
    assert "content check failed" in failed[0]["error"]

    action = next(
        item for item in plan["actions"] if item["action_id"] == failed[0]["action_id"]
    )
    table_dir = lake / action["table"]
    for item in action["inputs"]:
        assert (table_dir / item["path"]).is_file()
    assert not (table_dir / action["expected"]["output_path"]).exists()
