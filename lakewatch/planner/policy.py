"""Declarative maintenance policy loading and strict validation."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml

from ..utils import LakewatchError, digest, iso_z, parse_utc, safe_relpath


class PolicyError(LakewatchError):
    """Raised when a policy is missing fields or contains invalid values."""


REQUIRED = {
    "small_file_bytes",
    "max_files_per_partition",
    "max_files_listed",
    "max_actions",
    "retention",
}
OPTIONAL = {
    "compact_target_bytes": 8 * 1024 * 1024,
    "compact_max_inputs": 16,
    "compact_small_files": True,
    "vacuum_expired": True,
    "repair_missing": True,
    "adopt_enabled": False,
    "adopt_candidates": [],
    "tables": {},
}
TABLE_FIELDS = {
    "small_file_bytes",
    "max_files_per_partition",
    "max_files_listed",
    "compact_target_bytes",
    "compact_max_inputs",
    "compact_small_files",
    "vacuum_expired",
    "repair_missing",
    "adopt_enabled",
    "adopt_candidates",
    "retention",
    "enabled",
}
NONNEG = {"max_actions": 0, "compact_target_bytes": 1, "max_files_per_partition": 1,
          "max_files_listed": 1, "small_file_bytes": 1, "compact_max_inputs": 2}


@dataclass(frozen=True)
class AdoptCandidate:
    """A single orphan file the operator explicitly asked to adopt."""

    table: str
    path: str

    def as_dict(self) -> dict:
        return {"table": self.table, "path": self.path}


@dataclass(frozen=True)
class Settings:
    small_file_bytes: int
    max_files_per_partition: int
    max_files_listed: int
    compact_target_bytes: int
    compact_max_inputs: int
    compact_small_files: bool
    vacuum_expired: bool
    repair_missing: bool
    adopt_enabled: bool
    keep_last_snapshots: int
    keep_days: int
    enabled: bool


@dataclass(frozen=True)
class Policy:
    small_file_bytes: int
    max_files_per_partition: int
    max_files_listed: int
    max_actions: int
    compact_target_bytes: int
    compact_max_inputs: int
    compact_small_files: bool
    vacuum_expired: bool
    repair_missing: bool
    adopt_enabled: bool
    adopt_candidates: tuple[AdoptCandidate, ...]
    keep_last_snapshots: int
    keep_days: int
    as_of: datetime
    table_overrides: Mapping[str, dict]
    source: str = "<memory>"

    def settings(self, table: str) -> Settings:
        base = {
            "small_file_bytes": self.small_file_bytes,
            "max_files_per_partition": self.max_files_per_partition,
            "max_files_listed": self.max_files_listed,
            "compact_target_bytes": self.compact_target_bytes,
            "compact_max_inputs": self.compact_max_inputs,
            "compact_small_files": self.compact_small_files,
            "vacuum_expired": self.vacuum_expired,
            "repair_missing": self.repair_missing,
            "adopt_enabled": self.adopt_enabled,
            "keep_last_snapshots": self.keep_last_snapshots,
            "keep_days": self.keep_days,
            "enabled": True,
        }
        override = dict(self.table_overrides.get(table, {}))
        retention = override.pop("retention", {})
        base.update(override)
        base.update(retention)
        return Settings(**base)

    def candidates_for(self, table: str) -> tuple[str, ...]:
        """Return the explicitly declared adopt paths for one table."""
        return tuple(sorted({
            item.path for item in self.adopt_candidates if item.table == table
        }))

    @property
    def as_of_iso(self) -> str:
        return iso_z(self.as_of)

    @property
    def policy_hash(self) -> str:
        return digest(self.to_dict())

    def to_dict(self) -> dict:
        return {
            "small_file_bytes": self.small_file_bytes,
            "max_files_per_partition": self.max_files_per_partition,
            "max_files_listed": self.max_files_listed,
            "max_actions": self.max_actions,
            "compact_target_bytes": self.compact_target_bytes,
            "compact_max_inputs": self.compact_max_inputs,
            "compact_small_files": self.compact_small_files,
            "vacuum_expired": self.vacuum_expired,
            "repair_missing": self.repair_missing,
            "adopt_enabled": self.adopt_enabled,
            "adopt_candidates": [
                item.as_dict() for item in self.adopt_candidates
            ],
            "retention": {
                "keep_last_snapshots": self.keep_last_snapshots,
                "keep_days": self.keep_days,
                "as_of": self.as_of_iso,
            },
            "tables": {key: dict(value) for key, value in sorted(self.table_overrides.items())},
        }

    @classmethod
    def from_dict(cls, raw: Any, source: str = "<memory>") -> "Policy":
        if not isinstance(raw, Mapping):
            raise PolicyError("policy must be a YAML mapping")
        data = dict(raw)
        if "policy" in data:
            data = data["policy"]
            if not isinstance(data, Mapping):
                raise PolicyError("policy section must be a mapping")
            data = dict(data)
        unknown = set(data) - REQUIRED - set(OPTIONAL)
        if unknown:
            raise PolicyError(f"unknown policy field(s): {', '.join(sorted(unknown))}")
        missing = REQUIRED - set(data)
        if missing:
            raise PolicyError(f"missing policy field(s): {', '.join(sorted(missing))}")
        values = dict(OPTIONAL)
        values.update(data)
        _check_int(values, "small_file_bytes", 1)
        _check_int(values, "max_files_per_partition", 1)
        _check_int(values, "max_files_listed", 1)
        _check_int(values, "max_actions", 0)
        _check_int(values, "compact_target_bytes", 1)
        _check_int(values, "compact_max_inputs", 2)
        _check_bool(values, "compact_small_files")
        _check_bool(values, "vacuum_expired")
        _check_bool(values, "repair_missing")
        _check_bool(values, "adopt_enabled")
        candidates = _normalize_candidates(values["adopt_candidates"], "adopt_candidates")
        retention = values["retention"]
        if not isinstance(retention, Mapping):
            raise PolicyError("retention must be a mapping")
        unknown_ret = set(retention) - {"keep_last_snapshots", "keep_days", "as_of"}
        if unknown_ret:
            raise PolicyError(
                f"unknown retention field(s): {', '.join(sorted(unknown_ret))}"
            )
        if "keep_last_snapshots" not in retention or "keep_days" not in retention:
            raise PolicyError("retention requires keep_last_snapshots and keep_days")
        keep_last = retention["keep_last_snapshots"]
        keep_days = retention["keep_days"]
        if not _is_int(keep_last) or keep_last < 0:
            raise PolicyError("retention.keep_last_snapshots must be an integer >= 0")
        if not _is_int(keep_days) or keep_days < 0:
            raise PolicyError("retention.keep_days must be an integer >= 0")
        if "as_of" in retention:
            try:
                as_of = parse_utc(retention["as_of"])
            except LakewatchError as exc:
                raise PolicyError(f"retention.as_of: {exc}") from exc
        else:
            # Retention is day-granular, so day flooring keeps runs deterministic.
            as_of = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        tables = values["tables"]
        if not isinstance(tables, Mapping):
            raise PolicyError("tables must be a mapping")
        overrides = _normalize_overrides(tables)
        # Table-level candidate declarations are merged into the flat list.
        table_candidates = []
        for table, item in overrides.items():
            for rel in item.get("adopt_candidates", []):
                table_candidates.append(AdoptCandidate(table=table, path=rel))
        merged = sorted(
            set(candidates) | set(table_candidates),
            key=lambda entry: (entry.table, entry.path),
        )
        return cls(
            small_file_bytes=values["small_file_bytes"],
            max_files_per_partition=values["max_files_per_partition"],
            max_files_listed=values["max_files_listed"],
            max_actions=values["max_actions"],
            compact_target_bytes=values["compact_target_bytes"],
            compact_max_inputs=values["compact_max_inputs"],
            compact_small_files=values["compact_small_files"],
            vacuum_expired=values["vacuum_expired"],
            repair_missing=values["repair_missing"],
            adopt_enabled=values["adopt_enabled"],
            adopt_candidates=tuple(merged),
            keep_last_snapshots=keep_last,
            keep_days=keep_days,
            as_of=as_of,
            table_overrides=overrides,
            source=source,
        )

    @classmethod
    def from_yaml(cls, path: Path | str) -> "Policy":
        config_path = Path(path)
        if not config_path.exists():
            raise PolicyError(f"policy file does not exist: {config_path}")
        try:
            raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            raise PolicyError(f"cannot read policy: {exc}") from exc
        return cls.from_dict(raw, source=str(config_path))


def load_policy(source: Policy | Mapping | Path | str) -> Policy:
    if isinstance(source, Policy):
        return source
    if isinstance(source, Mapping):
        return Policy.from_dict(source)
    return Policy.from_yaml(source)


def cutoff_for(policy: Policy) -> datetime:
    return policy.as_of - timedelta(days=policy.keep_days)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_int(values: Mapping, key: str, minimum: int) -> None:
    value = values[key]
    if not _is_int(value) or value < minimum:
        raise PolicyError(f"{key} must be an integer >= {minimum}")


def _check_bool(values: Mapping, key: str) -> None:
    if not isinstance(values[key], bool):
        raise PolicyError(f"{key} must be a boolean")


def _normalize_candidates(raw: Any, field: str,
                           default_table: str | None = None) -> set[AdoptCandidate]:
    """Validate explicit adopt declarations: a list of table/path objects.

    Accepts ``[{"table": "events", "path": "region=east/part.parquet"}]`` and the
    shorthand string ``"events:region=east/part.parquet"``.  Inside a table
    override the table name is implied, so a bare relative path is also accepted.
    Nothing is inferred: files that are not named here are never adopted.
    """
    if raw is None:
        return set()
    if not isinstance(raw, (list, tuple)):
        raise PolicyError(f"{field} must be a list")
    result: set[AdoptCandidate] = set()
    for index, entry in enumerate(raw):
        table: Any = None
        path: Any = None
        if isinstance(entry, str):
            if ":" in entry:
                table, path = entry.split(":", 1)
            elif default_table is not None:
                table, path = default_table, entry
            else:
                raise PolicyError(
                    f"{field}[{index}] must be 'table:path' or a mapping"
                )
        elif isinstance(entry, Mapping):
            unknown = set(entry) - {"table", "path"}
            if unknown:
                raise PolicyError(
                    f"{field}[{index}] has unknown field(s): "
                    + ", ".join(sorted(unknown))
                )
            table = entry.get("table", default_table)
            path = entry.get("path")
        else:
            raise PolicyError(
                f"{field}[{index}] must be a mapping with table/path or a string"
            )
        if not isinstance(table, str) or not table.strip():
            raise PolicyError(f"{field}[{index}].table must be a non-empty string")
        try:
            rel = safe_relpath(path)
        except LakewatchError as exc:
            raise PolicyError(f"{field}[{index}].path: {exc}") from exc
        if not rel.endswith(".parquet"):
            raise PolicyError(f"{field}[{index}].path must end with .parquet")
        result.add(AdoptCandidate(table=table.strip(), path=rel))
    return result


def _normalize_overrides(tables: Mapping) -> dict[str, dict]:
    normalized: dict[str, dict] = {}
    for table, raw in sorted(tables.items()):
        if not isinstance(table, str) or not table:
            raise PolicyError("table override keys must be non-empty strings")
        if not isinstance(raw, Mapping):
            raise PolicyError(f"table override for {table} must be a mapping")
        item = dict(raw)
        unknown = set(item) - TABLE_FIELDS
        if unknown:
            raise PolicyError(
                f"unknown field(s) for table {table}: {', '.join(sorted(unknown))}"
            )
        if "retention" in item:
            retention = item["retention"]
            if not isinstance(retention, Mapping):
                raise PolicyError(f"table {table} retention must be a mapping")
            allowed = {"keep_last_snapshots", "keep_days"}
            unknown_ret = set(retention) - allowed
            if unknown_ret:
                raise PolicyError(
                    f"unknown retention field(s) for table {table}: "
                    + ", ".join(sorted(unknown_ret))
                )
            clean_ret: dict[str, int] = {}
            for key in allowed:
                if key in retention:
                    value = retention[key]
                    if not _is_int(value) or value < 0:
                        raise PolicyError(f"table {table} retention.{key} must be >= 0")
                    clean_ret[key] = value
            item["retention"] = clean_ret
        for key in ("small_file_bytes", "max_files_per_partition", "max_files_listed",
                    "compact_target_bytes", "compact_max_inputs"):
            if key in item and (not _is_int(item[key]) or item[key] < NONNEG[key]):
                raise PolicyError(f"table {table} {key} must be >= {NONNEG[key]}")
        for key in ("compact_small_files", "vacuum_expired", "repair_missing",
                    "adopt_enabled", "enabled"):
            if key in item and not isinstance(item[key], bool):
                raise PolicyError(f"table {table} {key} must be a boolean")
        if "adopt_candidates" in item:
            clean_paths = sorted(
                entry.path
                for entry in _normalize_candidates(
                    item["adopt_candidates"],
                    f"tables.{table}.adopt_candidates", default_table=table)
            )
            item["adopt_candidates"] = clean_paths
        normalized[table] = item
    return normalized
