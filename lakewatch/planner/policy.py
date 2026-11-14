"""Declarative maintenance policy loading and strict validation."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml

from ..utils import LakewatchError, digest, iso_z, parse_utc


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
    "retention",
    "enabled",
    "cluster",
}
CLUSTER_DIRECTIONS = ("asc", "desc")
CLUSTER_NULLS = ("first", "last")
NONNEG = {"max_actions": 0, "compact_target_bytes": 1, "max_files_per_partition": 1,
          "max_files_listed": 1, "small_file_bytes": 1, "compact_max_inputs": 2}


@dataclass(frozen=True)
class ClusterColumn:
    """One explicit sort-key entry of a clustered-compaction contract."""

    name: str
    direction: str
    nulls: str

    def as_dict(self) -> dict:
        return {"name": self.name, "direction": self.direction, "nulls": self.nulls}


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
    keep_last_snapshots: int
    keep_days: int
    enabled: bool
    cluster: tuple[ClusterColumn, ...] | None = None


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
            "keep_last_snapshots": self.keep_last_snapshots,
            "keep_days": self.keep_days,
            "enabled": True,
        }
        override = dict(self.table_overrides.get(table, {}))
        retention = override.pop("retention", {})
        cluster = override.pop("cluster", None)
        base.update(override)
        base.update(retention)
        if cluster is not None:
            base["cluster"] = tuple(
                ClusterColumn(**entry) for entry in cluster["columns"]
            )
        return Settings(**base)

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


def normalize_cluster(raw: Any, context: str) -> dict:
    """Validate an explicit clustered-merge contract into its canonical form.

    The contract is fully explicit: every column must name its sort direction
    and NULL placement, so the planner, the saved plan, and the executor all
    operate on byte-identical semantics.
    """
    if not isinstance(raw, Mapping):
        raise PolicyError(f"{context} must be a mapping")
    unknown = set(raw) - {"columns"}
    if unknown:
        raise PolicyError(f"{context} has unknown field(s): {', '.join(sorted(unknown))}")
    columns = raw.get("columns")
    if not isinstance(columns, list) or not columns:
        raise PolicyError(f"{context}.columns must be a non-empty list")
    seen: set[str] = set()
    normalized = []
    for index, entry in enumerate(columns):
        where = f"{context}.columns[{index}]"
        if not isinstance(entry, Mapping):
            raise PolicyError(f"{where} must be a mapping")
        unknown_entry = set(entry) - {"name", "direction", "nulls"}
        if unknown_entry:
            raise PolicyError(
                f"{where} has unknown field(s): {', '.join(sorted(unknown_entry))}"
            )
        missing = {"name", "direction", "nulls"} - set(entry)
        if missing:
            raise PolicyError(
                f"{where} is missing field(s): {', '.join(sorted(missing))}"
            )
        name = entry["name"]
        if not isinstance(name, str) or not name.strip():
            raise PolicyError(f"{where}.name must be a non-empty string")
        if name in seen:
            raise PolicyError(f"{context} lists column {name!r} more than once")
        seen.add(name)
        direction = entry["direction"]
        if direction not in CLUSTER_DIRECTIONS:
            raise PolicyError(
                f"{where}.direction must be one of {', '.join(CLUSTER_DIRECTIONS)}"
            )
        nulls = entry["nulls"]
        if nulls not in CLUSTER_NULLS:
            raise PolicyError(
                f"{where}.nulls must be one of {', '.join(CLUSTER_NULLS)}"
            )
        normalized.append({"name": name, "direction": direction, "nulls": nulls})
    return {"columns": normalized}


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
        for key in ("compact_small_files", "vacuum_expired", "repair_missing", "enabled"):
            if key in item and not isinstance(item[key], bool):
                raise PolicyError(f"table {table} {key} must be a boolean")
        if "cluster" in item:
            item["cluster"] = normalize_cluster(item["cluster"], f"table {table} cluster")
        normalized[table] = item
    return normalized
