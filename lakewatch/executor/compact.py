"""Parquet compaction with exact physical-byte parity.

Small inputs are rewritten as one valid Parquet file.  The rewrite can be
smaller than the sum of the inputs because each Parquet file carries a footer;
Lakewatch inserts an unreferenced zero gap immediately before the final footer
so the on-disk byte budget is exactly conserved.  Readers locate the footer from
the end of the file, so the file remains readable by pyarrow and other readers.

Byte and row-count parity alone cannot prove the rewrite kept the data: a
swapped value or a changed duplicate multiplicity conserves both.  Before the
new manifest is published and the inputs are deleted, the candidate output is
read back and its typed row multiset is compared against the inputs.  Every
distinct row — with NULL, empty string, binary and float-special values kept
distinct — must occur with the same multiplicity; row order may change.  The
verdict is returned in the result and recorded in the task ledger.
"""
from __future__ import annotations

import datetime
import decimal
import hashlib
import os
import struct
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow as pa
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


CONTENT_CHECK_ALGORITHM = "typed-row-multiset-sha256/v1"


def _frame(payload: bytes) -> bytes:
    """Length-prefix a payload so concatenated encodings stay unambiguous."""
    return len(payload).to_bytes(8, "big") + payload


def _encode_value(value: Any) -> bytes:
    """Encode one Python value so differently-typed values never collide.

    Every encoding carries a one-byte family tag and is self-delimiting, so
    NULL, empty strings and empty binary values stay distinct, and float
    specials keep their exact IEEE-754 bit pattern (NaN, +/-inf and -0.0 can
    never alias each other or any other value).
    """
    if value is None:
        return b"N"
    if isinstance(value, bool):
        return b"B\x01" if value else b"B\x00"
    if isinstance(value, int):
        return b"I" + _frame(str(value).encode("ascii"))
    if isinstance(value, float):
        return b"F" + struct.pack(">d", value)
    if isinstance(value, str):
        return b"S" + _frame(value.encode("utf-8"))
    if isinstance(value, (bytes, bytearray, memoryview)):
        return b"Y" + _frame(bytes(value))
    if isinstance(value, decimal.Decimal):
        return b"D" + _frame(str(value).encode("ascii"))
    if isinstance(value, datetime.datetime):
        return b"T" + _frame(value.isoformat().encode("ascii"))
    if isinstance(value, datetime.date):
        return b"A" + _frame(value.isoformat().encode("ascii"))
    if isinstance(value, datetime.time):
        return b"M" + _frame(value.isoformat().encode("ascii"))
    if isinstance(value, datetime.timedelta):
        return b"W" + _frame(str(value).encode("ascii"))
    if isinstance(value, uuid.UUID):
        return b"U" + _frame(str(value).encode("ascii"))
    if isinstance(value, (list, tuple)):
        return b"L" + _frame(b"".join(_encode_value(item) for item in value))
    if isinstance(value, dict):
        items = sorted(
            (_encode_value(key), _encode_value(item)) for key, item in value.items()
        )
        return b"G" + _frame(b"".join(key + item for key, item in items))
    raise CompactionError(
        f"content check cannot fingerprint value of type {type(value).__name__}"
    )


def _row_multiset(table: pa.Table) -> Counter:
    """Hash every row as (field name, field type, typed value) in schema order.

    The returned Counter is a true multiset: duplicate rows keep their
    multiplicity, and rows hash under their schema position so field order is
    respected.  Row order in the table does not affect the result.
    """
    columns = [table.column(index).to_pylist() for index in range(table.num_columns)]
    schema = table.schema
    counts: Counter = Counter()
    for row_index in range(table.num_rows):
        blob = b"".join(
            _frame(field.name.encode("utf-8"))
            + _frame(str(field.type).encode("utf-8"))
            + _encode_value(columns[field_index][row_index])
            for field_index, field in enumerate(schema)
        )
        counts[hashlib.sha256(blob).digest()] += 1
    return counts


def _multiset_digest(counts: Counter) -> str:
    """Order-independent digest of the full multiset, multiplicities included."""
    return digest([[key.hex(), count] for key, count in sorted(counts.items())])


def verify_content_parity(inputs: pa.Table, output_path: Path) -> dict:
    """Prove the candidate output carries exactly the input row multiset.

    The output is read back from disk so the check covers the bytes that would
    actually be published, zero-padding included.  Raises CompactionError
    unless every distinct row occurs with the same multiplicity on both sides;
    row order is ignored so legal rewrites that reorder rows still pass.
    """
    try:
        produced = pq.read_table(output_path)
    except Exception as exc:
        raise CompactionError(
            f"content check cannot read candidate output: {exc}"
        ) from exc
    if not produced.schema.equals(inputs.schema, check_metadata=False):
        raise CompactionError(
            "content check failed: candidate output schema differs from inputs"
        )
    expected = _row_multiset(inputs)
    actual = _row_multiset(produced)
    if expected != actual:
        missing = expected - actual
        unexpected = actual - expected
        raise CompactionError(
            "content check failed: candidate output row multiset differs "
            f"({sum(missing.values())} row(s) missing, "
            f"{sum(unexpected.values())} row(s) unexpected)"
        )
    return {
        "status": "passed",
        "algorithm": CONTENT_CHECK_ALGORITHM,
        "rows_in": inputs.num_rows,
        "rows_out": produced.num_rows,
        "distinct_rows": len(expected),
        "input_digest": _multiset_digest(expected),
        "output_digest": _multiset_digest(actual),
    }


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
    try:
        content_check = verify_content_parity(combined, destination)
    except Exception:
        # The candidate output is unreferenced; drop it so the inputs and the
        # current manifest remain the only usable state.
        destination.unlink(missing_ok=True)
        raise
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
        "content_check": content_check,
        "manifest_generation": updated_manifest.generation,
        "snapshot_id": snapshot.snapshot_id if snapshot else None,
        "output_hash": output_hash,
        "warnings": [item["reason"] for item in issues],
    }
