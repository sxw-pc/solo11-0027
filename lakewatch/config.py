"""Application configuration for the local workbench."""
from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping

import yaml

from .planner.policy import Policy, PolicyError
from .utils import LakewatchError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_FIELDS = {
    "lake_root",
    "ledger_path",
    "reports_dir",
    "host",
    "port",
    "policy",
    "policy_path",
}
REQUIRED = {"lake_root"}


class ConfigError(LakewatchError):
    """Raised when the application config is malformed."""


@dataclass(frozen=True)
class AppConfig:
    source: Path
    lake_root: Path
    ledger_path: Path
    reports_dir: Path
    host: str
    port: int
    policy: Policy

    def with_overrides(self, **changes: Any) -> "AppConfig":
        return replace(self, **changes)

    def public(self) -> dict:
        return {
            "source": str(self.source),
            "lake_root": str(self.lake_root),
            "ledger_path": str(self.ledger_path),
            "reports_dir": str(self.reports_dir),
            "host": self.host,
            "port": self.port,
            "policy_hash": self.policy.policy_hash,
            "as_of": self.policy.as_of_iso,
        }


def default_config_path() -> Path:
    return PROJECT_ROOT / "configs" / "default.yaml"


def _resolve_path(raw: Any, config_path: Path, field: str) -> Path:
    if not isinstance(raw, str) or not raw.strip():
        raise ConfigError(f"{field} must be a non-empty path string")
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    try:
        config_path.resolve().relative_to(PROJECT_ROOT)
    except ValueError:
        base = config_path.resolve().parent
    else:
        base = PROJECT_ROOT
    return (base / path).resolve()


def load_config(path: Path | str) -> AppConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"config file does not exist: {config_path}")
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"cannot read config: {exc}") from exc
    if not isinstance(raw, Mapping):
        raise ConfigError("config must be a YAML mapping")
    data = dict(raw)
    unknown = set(data) - CONFIG_FIELDS
    if unknown:
        raise ConfigError(f"unknown config field(s): {', '.join(sorted(unknown))}")
    missing = REQUIRED - set(data)
    if missing:
        raise ConfigError(f"missing config field(s): {', '.join(sorted(missing))}")
    if "policy" in data and "policy_path" in data:
        raise ConfigError("use either policy or policy_path, not both")

    lake_root = _resolve_path(data["lake_root"], config_path, "lake_root")
    if not lake_root.is_dir():
        raise ConfigError(f"lake_root is not a directory: {lake_root}")
    ledger_path = _resolve_path(
        data.get("ledger_path", ".lakewatch/ledger.sqlite3"), config_path, "ledger_path"
    )
    reports_dir = _resolve_path(
        data.get("reports_dir", "reports"), config_path, "reports_dir"
    )
    host = data.get("host", "127.0.0.1")
    if not isinstance(host, str) or not host.strip():
        raise ConfigError("host must be a non-empty string")
    port = data.get("port", 7311)
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise ConfigError("port must be an integer between 1 and 65535")

    try:
        if "policy_path" in data:
            policy_path = _resolve_path(data["policy_path"], config_path, "policy_path")
            policy = Policy.from_yaml(policy_path)
        elif "policy" in data:
            policy = Policy.from_dict(data["policy"], source=str(config_path))
        else:
            raise ConfigError("config requires a policy mapping or policy_path")
    except PolicyError as exc:
        raise ConfigError(f"invalid policy: {exc}") from exc
    return AppConfig(
        source=config_path.resolve(),
        lake_root=lake_root,
        ledger_path=ledger_path,
        reports_dir=reports_dir,
        host=host,
        port=port,
        policy=policy,
    )
