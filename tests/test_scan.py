from pathlib import Path

from lakewatch.scanners import scan_lake

from conftest import descriptor, policy_with, write_manifest, write_parquet


def _custom_table(tmp_path: Path, count: int = 4) -> Path:
    root = tmp_path / "threshold-lake"
    table = root / "tiny"
    files = []
    for index in range(count):
        rel = f"part={index % 2}/part-{index:04d}.parquet"
        write_parquet(table / rel, rows=6 + index, seed=20 + index)
        files.append(descriptor(table, rel, rows=6 + index))
    write_manifest(table, files=files)
    return root


def test_fixture_small_files_orphans_and_missing(lake, policy):
    result = scan_lake(lake, policy)
    summary = result.summary()
    assert summary["file_count"] == 47
    assert summary["small_file_count"] == 45
    assert summary["missing_count"] == 1
    assert summary["orphan_count"] == 3
    events = result.consistency["events"]
    assert events["missing_count"] == 1
    assert events["orphan_referenced_count"] == 1
    assert events["orphan_unreferenced_count"] == 2
    categories = {item["category"] for item in result.diagnostics}
    assert {"missing_file", "orphan_snapshot_referenced", "orphan_unreferenced",
            "small_file", "partition_skew"} <= categories
    orphan = next(
        item for item in result.diagnostics if item["category"] == "orphan_snapshot_referenced"
    )
    assert orphan["files"] == ["region=north/day=2025-01-01/part-legacy-000.parquet"]


def test_small_file_threshold_boundaries(tmp_path, policy):
    root = _custom_table(tmp_path)
    size = (root / "tiny" / "part=0" / "part-0000.parquet").stat().st_size
    equal = scan_lake(root, policy_with(policy, small_file_bytes=size))
    assert equal.tables[0]["small_file_count"] == 0
    above = scan_lake(root, policy_with(policy, small_file_bytes=size + 1))
    assert above.tables[0]["small_file_count"] >= 1
    below = scan_lake(root, policy_with(policy, small_file_bytes=size - 1))
    assert below.tables[0]["small_file_count"] == 0


def test_partition_skew_boundary_and_file_list_truncation(tmp_path, policy):
    root = _custom_table(tmp_path)
    equal = scan_lake(root, policy_with(policy, max_files_per_partition=2,
                                        table_overrides={}))
    assert not [d for d in equal.diagnostics if d["category"] == "partition_skew"]
    threshold = scan_lake(root, policy_with(policy, max_files_per_partition=1,
                                            table_overrides={}))
    skew = [d for d in threshold.diagnostics if d["category"] == "partition_skew"]
    assert len(skew) == 2
    assert all(item["count"] == 2 for item in skew)

    truncated = scan_lake(
        root,
        policy_with(policy, small_file_bytes=10_000, max_files_listed=2,
                    table_overrides={}),
    )
    small = [d for d in truncated.diagnostics if d["category"] == "small_file"]
    assert len(small) == 2
    assert all(len(item["files"]) == 2 for item in small)
    assert all(item["count"] == 2 for item in small)
    assert all(item["details"]["hidden_files"] == 0 for item in small)
