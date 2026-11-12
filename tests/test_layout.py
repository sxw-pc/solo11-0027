from pathlib import Path

import pytest

from lakewatch.catalog.layout import build_partitions, discover_lake
from lakewatch.utils import LakewatchError


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"fixture")


def test_fixture_layout_is_parsed_without_silent_skips(lake):
    layouts = {item.name: item for item in discover_lake(lake)}
    assert set(layouts) == {"events", "orders", "customers", "stray_export"}
    events = layouts["events"]
    assert events.has_metadata
    assert len(events.files) == 21
    assert events.partition_keys == ("region", "day")
    assert any(item.partition == "raw_dump" for item in events.files)
    assert any(not item.valid_partition for item in events.files)
    assert any(issue["category"] == "layout_bad_partition" for issue in events.issues)
    assert not layouts["stray_export"].has_metadata
    assert any(
        issue["category"] == "layout_missing_metadata"
        for issue in layouts["stray_export"].issues
    )


def test_partition_summary_and_key_mismatch(tmp_path):
    root = tmp_path / "lake"
    table = root / "accounts"
    _touch(table / "_metadata" / "manifest.json")
    _touch(table / "region=us" / "part-000.parquet")
    _touch(table / "region=us" / "day=1" / "part-001.parquet")
    _touch(table / "=broken" / "part-002.parquet")
    _touch(table / "not-a-partition" / "part-003.parquet")
    _touch(table / "notes.txt")
    layouts = discover_lake(root)
    assert len(layouts) == 1
    layout = layouts[0]
    assert len(layout.files) == 4
    assert {item.partition for item in layout.files} >= {"region=us", "region=us/day=1"}
    cats = {item["category"] for item in layout.issues}
    assert "layout_key_mismatch" in cats
    assert "layout_bad_partition" in cats
    assert "layout_non_parquet_file" in cats
    summaries = {item["name"]: item for item in build_partitions(layout)}
    assert summaries["region=us"]["file_count"] == 1
    assert summaries["region=us"]["avg_file_bytes"] > 0


def test_bad_table_name_missing_metadata_and_missing_root(tmp_path):
    root = tmp_path / "lake"
    _touch(root / "bad name" / "region=us" / "part.parquet")
    layouts = discover_lake(root)
    layout = layouts[0]
    cats = {item["category"] for item in layout.issues}
    assert "layout_bad_table_name" in cats
    assert "layout_missing_metadata" in cats
    with pytest.raises(LakewatchError):
        discover_lake(tmp_path / "does-not-exist")
