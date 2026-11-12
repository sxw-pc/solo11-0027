"""SQLite task ledger with a small, explicit schema migration."""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from ..utils import LakewatchError, json_text, sha256_text, utc_now_iso

SCHEMA_VERSION = 1


class LedgerError(LakewatchError):
    """Raised for invalid ledger operations."""


SCHEMA = """
CREATE TABLE IF NOT EXISTS plans (
  plan_hash TEXT PRIMARY KEY,
  format TEXT NOT NULL,
  status TEXT NOT NULL,
  action_count INTEGER NOT NULL DEFAULT 0,
  completed_actions INTEGER NOT NULL DEFAULT 0,
  skipped_actions INTEGER NOT NULL DEFAULT 0,
  before_json TEXT,
  after_json TEXT,
  report_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY,
  plan_hash TEXT NOT NULL REFERENCES plans(plan_hash) ON DELETE CASCADE,
  action_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  table_name TEXT NOT NULL,
  partition_name TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  input_hash TEXT,
  output_hash TEXT,
  duration_ms REAL NOT NULL DEFAULT 0,
  result_json TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(plan_hash, action_id)
);
CREATE TABLE IF NOT EXISTS reports (
  report_id TEXT PRIMARY KEY,
  plan_hash TEXT NOT NULL,
  json_path TEXT NOT NULL,
  markdown_path TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_plan ON tasks(plan_hash);
CREATE INDEX IF NOT EXISTS idx_reports_plan ON reports(plan_hash);
"""


def task_id_for(plan_hash: str, action_id: str) -> str:
    return "task-" + sha256_text(f"{plan_hash}:{action_id}")[:24]


class Ledger:
    def __init__(self, path: Path | str):
        self.path = Path(path)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def migrate(self) -> None:
        with self._connect() as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version < 1:
                conn.executescript(SCHEMA)
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                conn.commit()
            elif version > SCHEMA_VERSION:
                raise LedgerError(
                    f"ledger schema {version} is newer than supported {SCHEMA_VERSION}"
                )

    def ensure_plan(self, plan: dict) -> dict:
        now = utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO plans
                   (plan_hash, format, status, action_count, created_at, updated_at)
                   VALUES (?, ?, 'registered', ?, ?, ?)
                   ON CONFLICT(plan_hash) DO NOTHING""",
                (
                    plan["plan_hash"], plan["format"], len(plan["actions"]), now, now
                ),
            )
            conn.commit()
        return self.get_plan(plan["plan_hash"])

    def get_plan(self, plan_hash: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM plans WHERE plan_hash = ?", (plan_hash,)
            ).fetchone()
        return dict(row) if row else None

    def set_plan_status(self, plan_hash: str, status: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE plans SET status = ?, updated_at = ? WHERE plan_hash = ?",
                (status, utc_now_iso(), plan_hash),
            )
            conn.commit()

    def get_task(self, plan_hash: str, action_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE plan_hash = ? AND action_id = ?",
                (plan_hash, action_id),
            ).fetchone()
        return dict(row) if row else None

    def start_task(self, plan_hash: str, action: dict) -> str:
        task_id = task_id_for(plan_hash, action["action_id"])
        now = utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO tasks
                   (task_id, plan_hash, action_id, kind, table_name, partition_name,
                    reason, status, input_hash, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'running', ?, ?, ?)
                   ON CONFLICT(task_id) DO UPDATE SET
                     status = 'running', error = NULL, updated_at = excluded.updated_at""",
                (
                    task_id, plan_hash, action["action_id"], action["kind"],
                    action["table"], action.get("partition", ""),
                    action.get("reason", ""), action.get("input_hash"), now, now,
                ),
            )
            conn.commit()
        return task_id

    def complete_task(
        self, task_id: str, output_hash: str, duration_ms: float,
        result: dict | None = None
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE tasks SET status = 'completed', output_hash = ?,
                   duration_ms = ?, result_json = ?, error = NULL, updated_at = ?
                   WHERE task_id = ?""",
                (
                    output_hash, round(duration_ms, 3),
                    json_text(result or {}), utc_now_iso(), task_id
                ),
            )
            conn.commit()

    def fail_task(self, task_id: str, error: str, duration_ms: float = 0) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE tasks SET status = 'failed', error = ?, duration_ms = ?,
                   updated_at = ? WHERE task_id = ?""",
                (error, round(duration_ms, 3), utc_now_iso(), task_id),
            )
            conn.commit()

    def finish_plan(
        self, plan_hash: str, status: str, completed: int, skipped: int,
        before: dict | None = None, after: dict | None = None,
        report: dict | None = None
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE plans SET status = ?, completed_actions = ?,
                   skipped_actions = ?, before_json = ?, after_json = ?,
                   report_json = ?, updated_at = ? WHERE plan_hash = ?""",
                (
                    status, completed, skipped,
                    json_text(before) if before is not None else None,
                    json_text(after) if after is not None else None,
                    json_text(report) if report is not None else None,
                    utc_now_iso(), plan_hash,
                ),
            )
            conn.commit()

    def list_tasks(self, plan_hash: str | None = None, limit: int = 200) -> list[dict]:
        sql = "SELECT * FROM tasks"
        params: list[Any] = []
        if plan_hash:
            sql += " WHERE plan_hash = ?"
            params.append(plan_hash)
        sql += " ORDER BY created_at DESC, task_id ASC LIMIT ?"
        params.append(int(limit))
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def list_plans(self, limit: int = 50) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM plans ORDER BY created_at DESC, plan_hash ASC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [dict(row) for row in rows]

    def latest_plan(self, status: str | None = None) -> dict | None:
        sql = "SELECT * FROM plans"
        params: list[Any] = []
        if status:
            sql += " WHERE status = ?"
            params.append(status)
        sql += " ORDER BY updated_at DESC, rowid DESC LIMIT 1"
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def record_report(
        self, report_id: str, plan_hash: str, json_path: str,
        markdown_path: str, payload: dict
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO reports
                   (report_id, plan_hash, json_path, markdown_path, payload_json,
                    created_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(report_id) DO UPDATE SET
                     json_path = excluded.json_path,
                     markdown_path = excluded.markdown_path,
                     payload_json = excluded.payload_json,
                     created_at = excluded.created_at""",
                (
                    report_id, plan_hash, json_path, markdown_path,
                    json_text(payload), utc_now_iso(),
                ),
            )
            conn.commit()

    def list_reports(self, limit: int = 50) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM reports ORDER BY created_at DESC, rowid DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_report(self, report_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM reports WHERE report_id = ?", (report_id,)
            ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["payload"] = json.loads(result.pop("payload_json"))
        return result
