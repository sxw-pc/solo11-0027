"""Execution-time, per-action claims built on the SQLite ledger.

Two processes applying the same plan cannot safely rely on the ledger's
completed-task records alone: both can observe "not completed" before either
of them writes one.  Every action is therefore *claimed* atomically before
its executor runs, using a single conditional UPSERT.

Liveness model
--------------
A claim is a time-bounded lease, not a permanent lock.  The holder runs a
daemon heartbeat thread that renews the lease while its action executes.  A
process that dies stops heartbeating, so after ``lease_ttl_seconds`` the
lease expires and another process may take it over (the ``attempts`` counter
and a fresh ``owner_id`` record every takeover -- a lightweight fencing
token).  PIDs are deliberately not used: they are unreliable across
containers, restarts and PID reuse.

Zombies are reclaimed lazily on the next claim attempt;
:meth:`Ledger.reap_stale_claims` provides an explicit sweep as well.
"""
from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable

from ..utils import iso_z, parse_utc, utc_now
from .db import Ledger, LedgerBusyError, task_id_for

# Defaults can be overridden per call (e.g. in tests) or via environment.
DEFAULT_LEASE_TTL_SECONDS = 30.0
DEFAULT_WAIT_TIMEOUT_SECONDS = 30.0
DEFAULT_POLL_INTERVAL_SECONDS = 0.25

_ENV_TTL = "LAKEWATCH_CLAIM_TTL_SECONDS"
_ENV_WAIT = "LAKEWATCH_CLAIM_WAIT_SECONDS"
_ENV_POLL = "LAKEWATCH_CLAIM_POLL_SECONDS"


class ClaimError(LedgerBusyError):
    """Raised when claim bookkeeping itself fails with a retryable error."""


def new_owner_id() -> str:
    """Identify one applying process for the lifetime of one ``apply_plan``."""
    return f"owner-{os.getpid()}-{uuid.uuid4().hex[:12]}"


def _env_float(name: str, fallback: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return fallback
    try:
        value = float(raw)
    except ValueError:
        return fallback
    return value if value > 0 else fallback


def lease_settings(
    lease_ttl_seconds: float | None = None,
    wait_timeout_seconds: float | None = None,
    poll_interval_seconds: float | None = None,
) -> tuple[float, float, float]:
    return (
        lease_ttl_seconds
        if lease_ttl_seconds is not None
        else _env_float(_ENV_TTL, DEFAULT_LEASE_TTL_SECONDS),
        wait_timeout_seconds
        if wait_timeout_seconds is not None
        else _env_float(_ENV_WAIT, DEFAULT_WAIT_TIMEOUT_SECONDS),
        poll_interval_seconds
        if poll_interval_seconds is not None
        else _env_float(_ENV_POLL, DEFAULT_POLL_INTERVAL_SECONDS),
    )


@dataclass
class ClaimLease:
    """A held claim with a self-renewing heartbeat.

    Use as a context manager around the action executor.  The heartbeat stops
    and the claim is released on normal exit; on a crash nothing runs, which
    is exactly what expiry-based takeover is for.
    """

    ledger: Ledger
    plan_hash: str
    action_id: str
    owner_id: str
    ttl_seconds: float
    _stop: threading.Event | None = None
    _thread: threading.Thread | None = None
    _lost: bool = False

    @property
    def lost(self) -> bool:
        """True once a heartbeat found the lease had been taken over."""
        return self._lost

    def _renew(self) -> None:
        now = utc_now()
        ok = self.ledger.heartbeat_claim(
            self.plan_hash, self.action_id, self.owner_id,
            iso_z(now), iso_z(now + timedelta(seconds=self.ttl_seconds)),
        )
        if not ok:
            self._lost = True

    def _run_heartbeat(self) -> None:
        interval = max(0.05, self.ttl_seconds / 3.0)
        while self._stop is not None and not self._stop.wait(interval):
            try:
                self._renew()
            except LedgerBusyError:
                # A missed heartbeat is tolerable: the next one (before the
                # lease expires, with 3x headroom) will catch up.
                continue
            if self._lost:
                return

    def close(self) -> None:
        """Stop heartbeating and release the lease (idempotent)."""
        if self._stop is not None:
            self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        if not self._lost:
            # Best-effort release; expiry handles it if this write fails.
            try:
                self.ledger.release_claim(
                    self.plan_hash, self.action_id, self.owner_id
                )
            except LedgerBusyError:
                pass

    def __enter__(self) -> "ClaimLease":
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run_heartbeat,
            name=f"lakewatch-lease-{self.action_id[:12]}",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def acquire_claim(
    ledger: Ledger,
    plan_hash: str,
    action: dict,
    owner_id: str,
    *,
    ttl_seconds: float,
    wait_timeout_seconds: float,
    poll_interval_seconds: float,
    sleep: Callable[[float], None] = None,
) -> tuple[ClaimLease | None, dict | None]:
    """Claim one action, waiting out a live holder up to a bounded budget.

    Returns ``(lease, None)`` on success.  If a live holder still owns the
    action when the budget is spent, returns ``(None, holder_row)`` so the
    caller can record a skip with the reason.  An expired lease is taken over
    immediately inside the same atomic UPSERT.
    """
    action_id = action["action_id"]
    do_sleep = sleep or __import__("time").sleep
    task_id = task_id_for(plan_hash, action_id)
    deadline = utc_now().timestamp() + max(0.0, wait_timeout_seconds)

    while True:
        now = utc_now()
        claimed = ledger.claim_task(
            task_id, plan_hash, action, owner_id,
            iso_z(now), iso_z(now + timedelta(seconds=ttl_seconds)),
        )
        if claimed:
            return (
                ClaimLease(
                    ledger=ledger,
                    plan_hash=plan_hash,
                    action_id=action_id,
                    owner_id=owner_id,
                    ttl_seconds=ttl_seconds,
                ),
                None,
            )
        holder = ledger.get_claim(plan_hash, action_id)
        remaining = deadline - utc_now().timestamp()
        if holder is None or remaining <= 0:
            return None, holder
        # Only wait while the holder's lease is still in force.
        expires = parse_utc(holder["lease_expires_at"]).timestamp()
        if expires <= utc_now().timestamp():
            continue  # lease just expired: loop and take over immediately
        do_sleep(min(poll_interval_seconds, max(0.01, remaining)))


__all__ = [
    "ClaimError",
    "ClaimLease",
    "acquire_claim",
    "lease_settings",
    "new_owner_id",
]
