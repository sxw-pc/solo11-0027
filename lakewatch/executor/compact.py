"""Parquet compaction with exact physical-byte parity.

Small inputs are rewritten as one valid Parquet file.  The rewrite can be
smaller than the sum of the inputs because each Parquet file carries a footer;
Lakewatch inserts an unreferenced zero gap immediately before the final footer
so the on-disk byte budget is exactly conserved.  Readers locate the footer from
the end of the file, so the file remains readable by pyarrow and other readers.
"""
from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ..catalog.manifest import Manifest, file_descriptor, load_manifest, write_manifest
from ..catalog.snapshot import load_snapshots, next_sequence, write_snapshot
from ..utils import (
    LakewatchError,
    digest,
    resolve_under,
    sha256_file,
    utc_now_iso,
)


class CompactionError(LakewatchError):
    """Raised when a compaction cannot safely complete."""


def _orderable(data_type: pa.DataType) -> bool:
    checks = (
        pa.types.is_boolean, pa.types.is_integer, pa.types.is_floating,
        pa.types.is_decimal, pa.types.is_string, pa.types.is_large_string,
        pa.types.is_binary, pa.types.is_large_binary,
        pa.types.is_date32, pa.types.is_date64, pa.types.is_timestamp,
        pa.types.is_time32, pa.types.is_time64, pa.types.is_duration,
    )
    return any(check(data_type) for check in checks)


def _helper_name(existing: set[str], stem: str) -> str:
    name = stem
    while name in existing:
        name = "_" + name
    existing.add(name)
    return name


def _cluster_table(table: pa.Table, columns: list[dict]) -> pa.Table:
    """Reorder rows by the saved cluster contract, deterministically.

    Every sort column is validated before anything is written.  NULL placement
    is realized with explicit indicator columns so each key can place NULLs
    first or last independently.  A monotonic sequence over the plan-ordered
    concatenation is appended as the final tie-break, so equal keys keep every
    row and the full output order is reproducible regardless of how the input
    files were enumerated on disk.
    """
    existing = set(table.column_names)
    helpers: list[str] = []
    sort_keys: list[tuple[str, str]] = []
    augmented = table
    for index, spec in enumerate(columns):
        name = spec["name"]
        if name not in table.column_names:
            raise CompactionError(f"cluster sort column is not in the schema: {name}")
        column = augmented.column(name)
        if pa.types.is_dictionary(column.type):
            column = column.dictionary_decode()
            field_index = augmented.schema.get_field_index(name)
            augmented = augmented.set_column(
                field_index,
                augmented.schema.field(field_index).with_type(column.type),
                column,
            )
        data_type = augmented.column(name).type
        if not _orderable(data_type):
            raise CompactionError(
                f"cluster sort column {name} has non-orderable type {data_type}"
            )
        null_name = _helper_name(existing, f"__lakewatch_cluster_null_{index}")
        augmented = augmented.append_column(
            null_name, pc.is_null(augmented.column(name))
        )
        helpers.append(null_name)
        sort_keys.append(
            (null_name, "descending" if spec["nulls"] == "first" else "ascending")
        )
        sort_keys.append(
            (name, "ascending" if spec["direction"] == "asc" else "descending")
        )
    seq_name = _helper_name(existing, "__lakewatch_cluster_seq")
    augmented = augmented.append_column(
        seq_name, pa.array(range(augmented.num_rows), pa.int64())
    )
    helpers.append(seq_name)
    sort_keys.append((seq_name, "ascending"))
    order = pc.sort_indices(augmented, sort_keys=sort_keys)
    return augmented.take(order).drop(helpers)


def _read_inputs(table_dir: Path, inputs: list[dict]) -> tuple[list[dict], pa.Table]:
    verified = []
    tables = []
    for item in inputs:
        path = resolve_under(table_dir, item["path"])
        if not path.is_file():
            raise CompactionError(f"compaction input is missing: {item['path']}")
        actual = sha256_file(path)
        if item.get("sha256") and item["sha256"] != actual:
            raise CompactionError(f"compaction input changed: {item['path']}")
        verified.append(
            {"path": item["path"], "bytes": path.stat().st_size, "sha256": actual}
        )
        tables.append(pq.read_table(path))
    if not tables:
        raise CompactionError("compaction requires at least one input")
    schema = tables[0].schema
    for index, table in enumerate(tables[1:], start=1):
        if table.schema.remove_metadata() != schema.remove_metadata():
            raise CompactionError(
                f"input schema mismatch at index {index}; refusing to merge"
            )
    return verified, pa.concat_tables(tables)


def _write_exact_parquet(table: pa.Table, destination: Path, target_bytes: int,
                         action_id: str) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise CompactionError(f"compaction output already exists: {destination.name}")
    metadata = dict(table.schema.metadata or {})
    metadata[b"lakewatch.compaction"] = action_id.encode("utf-8")
    prepared = table.replace_schema_metadata(metadata)
    temp = destination.with_name(destination.name + ".tmp")
    try:
        pq.write_table(prepared, temp, compression="snappy", write_statistics=True)
        natural = temp.stat().st_size
        if natural > target_bytes:
            raise CompactionError(
                f"rewritten parquet is {natural} bytes, above byte budget "
                f"{target_bytes}; refusing to violate byte parity"
            )
        data = temp.read_bytes()
        if len(data) < 8 or data[-4:] != b"PAR1":
            raise CompactionError("rewritten parquet has no footer magic")
        footer_len = int.from_bytes(data[-8:-4], "little")
        split = len(data) - 8 - footer_len
        if split <= 4 or data[:4] != b"PAR1":
            raise CompactionError("rewritten parquet footer layout is invalid")
        padding = target_bytes - natural
        if padding:
            data = data[:split] + (b"\x00" * padding) + data[split:]
        temp.write_bytes(data)
        if len(data) != target_bytes:
            raise CompactionError("byte-parity padding did not produce the target size")
        os.replace(temp, destination)
        return target_bytes
    finally:
        if temp.exists():
            temp.unlink(missing_ok=True)


def execute_compaction(root: Path, action: dict) -> dict:
    table_name = action["table"]
    table_dir = (root / table_name).resolve()
    manifest, issues = load_manifest(table_dir)
    if manifest is None:
        raise CompactionError(f"cannot compact {table_name}: manifest unavailable")
    current = {item.path: item for item in manifest.files}
    for item in action["inputs"]:
        if item["path"] not in current:
            raise CompactionError(
                f"compaction input is not in the current manifest: {item['path']}"
            )
    verified, combined = _read_inputs(table_dir, action["inputs"])
    cluster = action.get("cluster")
    if cluster is not None:
        # Replay the exact contract stored in the plan; never re-derive it.
        combined = _cluster_table(combined, cluster["columns"])
    output = action["expected"]["output_path"]
    if "/" not in output:
        output_dir = table_dir
    else:
        output_dir = resolve_under(table_dir, output.rsplit("/", 1)[0])
        if not output_dir.is_dir():
            output_dir.mkdir(parents=True, exist_ok=True)
    destination = resolve_under(table_dir, output)
    if destination.parent != output_dir:
        raise CompactionError("compaction output is outside its partition")
    total_bytes = sum(item["bytes"] for item in verified)
    _write_exact_parquet(combined, destination, total_bytes, action["action_id"])
    descriptor = file_descriptor(table_dir, output, combined.num_rows)
    remove_paths = {item["path"] for item in verified}
    new_files = [item for item in manifest.files if item.path not in remove_paths]
    new_files.append(descriptor)
    new_files.sort(key=lambda item: item.path)
    updated_at = utc_now_iso()
    updated_manifest = Manifest(
        table=table_name,
        generation=manifest.generation + 1,
        updated_at=updated_at,
        files=tuple(new_files),
        path=manifest.path,
    )
    existing_snapshots, _ = load_snapshots(table_dir)
    snapshot = None
    try:
        snapshot = write_snapshot(
            table_dir, new_files, updated_at, next_sequence(existing_snapshots)
        )
        write_manifest(table_dir, updated_manifest)
    except Exception:
        if snapshot and snapshot.path.exists():
            snapshot.path.unlink(missing_ok=True)
        destination.unlink(missing_ok=True)
        raise
    for item in verified:
        resolve_under(table_dir, item["path"]).unlink(missing_ok=False)
    output_hash = digest(
        {
            "outputs": [{"path": descriptor.path, "sha256": descriptor.sha256}],
            "manifest_generation": updated_manifest.generation,
        }
    )
    return {
        "status": "completed",
        "outputs": [descriptor.as_dict()],
        "deleted_inputs": [item["path"] for item in verified],
        "bytes_in": total_bytes,
        "bytes_out": destination.stat().st_size,
        "rows_in": combined.num_rows,
        "rows_out": combined.num_rows,
        "manifest_generation": updated_manifest.generation,
        "snapshot_id": snapshot.snapshot_id if snapshot else None,
        "output_hash": output_hash,
        "warnings": [item["reason"] for item in issues],
    }
