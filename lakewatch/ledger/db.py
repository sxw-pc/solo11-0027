"""SQLite task ledger with a small, explicit schema migration."""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from ..utils import LakewatchError, json_text, sha256_text, utc_now_iso

SCHEMA_VERSION = 2

# How long SQLite itself waits on a held write lock before giving up.
BUSY_TIMEOUT_SECONDS = 10.0
# In-process retries when another writer still wins the lock after waiting.
_BUSY_RETRIES = 5
_BUSY_RETRY_DELAY = 0.2


class LedgerError(LakewatchError):
    """Raised for invalid ledger operations."""


class LedgerBusyError(LedgerError):
    """Raised when SQLite cannot acquire its lock within the busy timeout.

    This is a transient, retryable condition -- never a business failure.
    """


def _is_busy(exc: sqlite3.Error) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "database table is locked" in message


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

# Added in schema v2: per-action execution claims.  Claim state lives only in
# the ledger -- it is never part of the plan file or the plan_hash.
CLAIMS_TABLE = """
CREATE TABLE IF NOT EXISTS action_claims (
  task_id TEXT PRIMARY KEY REFERENCES tasks(task_id) ON DELETE CASCADE,
  plan_hash TEXT NOT NULL,
  action_id TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 1,
  claimed_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL,
  lease_expires_at TEXT NOT NULL,
  released_at TEXT,
  UNIQUE(plan_hash, action_id)
);
CREATE INDEX IF NOT EXISTS idx_claims_plan ON action_claims(plan_hash);
CREATE INDEX IF NOT EXISTS idx_claims_expiry
  ON action_claims(lease_expires_at);
"""


def task_id_for(plan_hash: str, action_id: str) -> str:
    return "task-" + sha256_text(f"{plan_hash}:{action_id}")[:24]


class Ledger:
    def __init__(self, path: Path | str):
        self.path = Path(path)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=BUSY_TIMEOUT_SECONDS)
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

    def _transaction(self, write: callable):  # type: ignore[type-arg]
        """Run ``write(conn)`` inside a transaction, retrying lock contention.

        SQLite lock/timeout failures are converted to :class:`LedgerBusyError`
        so callers can treat them as transient rather than business failures.
        """
        last_busy: sqlite3.OperationalError | None = None
        for attempt in range(_BUSY_RETRIES):
            try:
                with self._connect() as conn:
                    return write(conn)
            except sqlite3.OperationalError as exc:
                if not _is_busy(exc):
                    raise
                last_busy = exc
                time.sleep(_BUSY_RETRY_DELAY * (attempt + 1))
        raise LedgerBusyError(
            f"ledger locked after {_BUSY_RETRIES} retries: {last_busy}"
        ) from last_busy

    def migrate(self) -> None:
        def _run(conn: sqlite3.Connection) -> None:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:
                raise LedgerError(
                    f"ledger schema {version} is newer than supported "
                    f"{SCHEMA_VERSION}"
                )
            if version < 1:
                conn.executescript(SCHEMA)
            if version < 2:
                # v1 -> v2: add execution-claim tables to an existing database.
                conn.executescript(CLAIMS_TABLE)
            if version != SCHEMA_VERSION:
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

        self._transaction(_run)

    def ensure_plan(self, plan: dict) -> dict:
        now = utc_now_iso()

        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """INSERT INTO plans
                   (plan_hash, format, status, action_count, created_at, updated_at)
                   VALUES (?, ?, 'registered', ?, ?, ?)
                   ON CONFLICT(plan_hash) DO NOTHING""",
                (
                    plan["plan_hash"], plan["format"], len(plan["actions"]), now, now
                ),
            )

        self._transaction(_write)
        return self.get_plan(plan["plan_hash"])

    def get_plan(self, plan_hash: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM plans WHERE plan_hash = ?", (plan_hash,)
            ).fetchone()
        return dict(row) if row else None

    def set_plan_status(self, plan_hash: str, status: str) -> None:
        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE plans SET status = ?, updated_at = ? WHERE plan_hash = ?",
                (status, utc_now_iso(), plan_hash),
            )

        self._transaction(_write)

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

        def _write(conn: sqlite3.Connection) -> None:
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

        self._transaction(_write)
        return task_id

    def complete_task(
        self, task_id: str, output_hash: str, duration_ms: float,
        result: dict | None = None
    ) -> None:
        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """UPDATE tasks SET status = 'completed', output_hash = ?,
                   duration_ms = ?, result_json = ?, error = NULL, updated_at = ?
                   WHERE task_id = ?""",
                (
                    output_hash, round(duration_ms, 3),
                    json_text(result or {}), utc_now_iso(), task_id
                ),
            )

        self._transaction(_write)

    def fail_task(self, task_id: str, error: str, duration_ms: float = 0) -> None:
        def _write(conn: sqlite3.Connection) -> None:
            conn.execute(
                """UPDATE tasks SET status = 'failed', error = ?, duration_ms = ?,
                   updated_at = ? WHERE task_id = ?""",
                (error, round(duration_ms, 3), utc_now_iso(), task_id),
            )

        self._transaction(_write)

    def finish_plan(
        self, plan_hash: str, status: str, completed: int, skipped: int,
        before: dict | None = None, after: dict | None = None,
        report: dict | None = None
    ) -> None:
        def _write(conn: sqlite3.Connection) -> None:
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

        self._transaction(_write)

    # ------------------------------------------------------------------
    # Execution-time claims (schema v2)
    # ------------------------------------------------------------------
    def claim_task(
        self, task_id: str, plan_hash: str, action: dict, owner_id: str,
        claimed_at: str, lease_expires_at: str,
    ) -> bool:
        """Atomically claim one action.

        Returns ``True`` when this owner now holds the claim.  An existing
        non-terminal claim whose lease has not expired blocks the takeover;
        an expired lease (zombie left by a crashed process) is taken over in
        the same statement, bumping ``attempts`` and replacing the owner.

        The winning transaction also owns the task row (status ``claimed``),
        so no other process can create it; :meth:`start_task` flips it to
        ``running`` immediately before the executor runs.
        """
        action_id = action["action_id"]

        def _write(conn: sqlite3.Connection) -> int:
            conn.execute(
                """INSERT INTO tasks
                   (task_id, plan_hash, action_id, kind, table_name,
                    partition_name, reason, status, input_hash,
                    created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'claimed', ?, ?, ?)
                   ON CONFLICT(task_id) DO NOTHING""",
                (
                    task_id, plan_hash, action_id, action["kind"],
                    action["table"], action.get("partition", ""),
                    action.get("reason", ""), action.get("input_hash"),
                    claimed_at, claimed_at,
                ),
            )
            conn.execute(
                """INSERT INTO action_claims
                   (task_id, plan_hash, action_id, owner_id, status, attempts,
                    claimed_at, heartbeat_at, lease_expires_at)
                   VALUES (?, ?, ?, ?, 'held', 1, ?, ?, ?)
                   ON CONFLICT(task_id) DO UPDATE SET
                     owner_id = excluded.owner_id,
                     status = 'held',
                     attempts = action_claims.attempts + 1,
                     claimed_at = excluded.claimed_at,
                     heartbeat_at = excluded.heartbeat_at,
                     lease_expires_at = excluded.lease_expires_at,
                     released_at = NULL
                   WHERE action_claims.status = 'released'
                      OR (action_claims.status = 'held'
                          AND action_claims.lease_expires_at <= ?)""",
                (
                    task_id, plan_hash, action_id, owner_id,
                    claimed_at, claimed_at, lease_expires_at, claimed_at,
                ),
            )
            return conn.execute(
                "SELECT changes()",
            ).fetchone()[0]

        return self._transaction(_write) > 0

    def get_claim(self, plan_hash: str, action_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM action_claims WHERE plan_hash = ? AND action_id = ?",
                (plan_hash, action_id),
            ).fetchone()
        return dict(row) if row else None

    def heartbeat_claim(
        self, plan_hash: str, action_id: str, owner_id: str,
        heartbeat_at: str, lease_expires_at: str,
    ) -> bool:
        """Renew a lease held by ``owner_id``; ``False`` if it was stolen."""
        def _write(conn: sqlite3.Connection) -> int:
            conn.execute(
                """UPDATE action_claims SET heartbeat_at = ?,
                   lease_expires_at = ?
                   WHERE plan_hash = ? AND action_id = ? AND owner_id = ?
                     AND status = 'held'""",
                (
                    heartbeat_at, lease_expires_at,
                    plan_hash, action_id, owner_id,
                ),
            )
            return conn.execute("SELECT changes()").fetchone()[0]

        return self._transaction(_write) > 0

    def release_claim(
        self, plan_hash: str, action_id: str, owner_id: str,
        released_at: str | None = None,
    ) -> bool:
        """Mark the claim released, but only while this owner still holds it."""
        def _write(conn: sqlite3.Connection) -> int:
            conn.execute(
                """UPDATE action_claims SET status = 'released',
                   released_at = ?, lease_expires_at = ?
                   WHERE plan_hash = ? AND action_id = ? AND owner_id = ?
                     AND status = 'held'""",
                (
                    released_at or utc_now_iso(), released_at or utc_now_iso(),
                    plan_hash, action_id, owner_id,
                ),
            )
            return conn.execute("SELECT changes()").fetchone()[0]

        return self._transaction(_write) > 0

    def reap_stale_claims(self, now: str | None = None) -> list[dict]:
        """Release every held claim whose lease has expired.

        Lazy takeover in :meth:`claim_task` already handles zombies; this is
        the explicit sweep for operators and tests.  Returns reaped rows.
        """
        moment = now or utc_now_iso()

        def _write(conn: sqlite3.Connection) -> list[dict]:
            rows = conn.execute(
                """SELECT * FROM action_claims
                   WHERE status = 'held' AND lease_expires_at <= ?""",
                (moment,),
            ).fetchall()
            conn.execute(
                """UPDATE action_claims SET status = 'released',
                   released_at = ?, lease_expires_at = ?
                   WHERE status = 'held' AND lease_expires_at <= ?""",
                (moment, moment, moment),
            )
            return [dict(row) for row in rows]

        return self._transaction(_write)

    def list_claims(self, plan_hash: str | None = None) -> list[dict]:
        sql = "SELECT * FROM action_claims"
        params: list[Any] = []
        if plan_hash:
            sql += " WHERE plan_hash = ?"
            params.append(plan_hash)
        sql += " ORDER BY claimed_at ASC, task_id ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

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
        def _write(conn: sqlite3.Connection) -> None:
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

        self._transaction(_write)

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
