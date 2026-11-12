"""Small, dependency-free helpers shared across Lakewatch."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any


class LakewatchError(ValueError):
    """Base error for invalid configuration or lake state."""


def canonical_json(value: Any) -> str:
    """Serialize a value in a stable form used by every digest in the project."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def digest(value: Any) -> str:
    return sha256_text(canonical_json(value))


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(chunk_size), b""):
            h.update(block)
    return h.hexdigest()


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def utc_now_iso() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and require an explicit timezone."""
    if not isinstance(value, str) or not value.strip():
        raise LakewatchError("timestamp must be a non-empty ISO-8601 string")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise LakewatchError(f"invalid ISO-8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise LakewatchError(f"timestamp must include a timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def safe_relpath(value: Any) -> str:
    """Validate a manifest path and normalize it to POSIX separators."""
    if not isinstance(value, str) or not value.strip():
        raise LakewatchError("path must be a non-empty string")
    text = value.replace("\\", "/")
    pure = PurePosixPath(text)
    if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts):
        raise LakewatchError(f"unsafe relative path: {value!r}")
    if ":" in pure.parts[0]:
        raise LakewatchError(f"unsafe relative path: {value!r}")
    return pure.as_posix()


def resolve_under(root: Path, relative: str) -> Path:
    rel = safe_relpath(relative)
    candidate = (root / Path(rel)).resolve()
    base = root.resolve()
    if candidate != base and base not in candidate.parents:
        raise LakewatchError(f"path escapes root: {relative!r}")
    return candidate


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def json_text(value: Any, *, pretty: bool = True) -> str:
    if pretty:
        return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    return canonical_json(value)


def make_issue(
    category: str,
    severity: str,
    table: str,
    partition: str,
    reason: str,
    *,
    files: list[str] | None = None,
    **details: Any,
) -> dict[str, Any]:
    """Create the single diagnostic shape used by every scanner."""
    paths = list(files or [])
    return {
        "id": f"{category}:{table}:{partition}".rstrip(":"),
        "category": category,
        "severity": severity,
        "table": table,
        "partition": partition,
        "reason": reason,
        "files": paths,
        "count": int(details.pop("count", len(paths))),
        "details": details,
    }


def human_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024 or unit == "GiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size} B"
