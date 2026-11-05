"""L0 heartbeat: write and purge rows on the primary writer (FR-01).

These are the only writes the tool makes (NFR-01, NFR-05). Each beat uses a new
connection from the caller's `connect` factory, which must return a connection
to the primary writer using `sslmode=verify-full` and credentials from Secrets
Manager or IAM DB auth (NFR-02, NFR-03).

A beat never raises for a database failure. It returns a `BeatResult` that says
which step failed, so that a heartbeat that could not be written stays distinct
from a lag breach (FR-03) and a failure never reads as healthy (NFR-09).
"""

import threading
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Final
from uuid import UUID, uuid4

import psycopg

type Connection = psycopg.Connection[tuple[Any, ...]]

HEARTBEAT_INTERVAL: Final = timedelta(seconds=30)
HEARTBEAT_RETENTION: Final = timedelta(days=7)
DEFAULT_STATEMENT_TIMEOUT: Final = timedelta(minutes=30)  # NFR-05

_SET_STATEMENT_TIMEOUT: Final = "SELECT set_config('statement_timeout', %s, true)"
_INSERT_HEARTBEAT: Final = """
    INSERT INTO aurora_mirrorcheck.heartbeat (nonce, written_at)
    VALUES (%s, clock_timestamp())
    RETURNING written_at
"""
_PURGE_HEARTBEATS: Final = """
    DELETE FROM aurora_mirrorcheck.heartbeat
    WHERE written_at < now() - %s::interval
"""


class BeatFailure(StrEnum):
    """The step of a beat that failed."""

    WRITE = "write"
    PURGE = "purge"


@dataclass(frozen=True)
class Heartbeat:
    """A committed heartbeat row. `written_at` comes from the primary's clock."""

    nonce: UUID
    written_at: datetime


@dataclass(frozen=True)
class BeatResult:
    """The outcome of one beat.

    After a purge failure `heartbeat` is still set, because the row was committed
    and can still be polled for on the secondary.
    """

    heartbeat: Heartbeat | None = None
    purged: int | None = None
    failure: BeatFailure | None = None
    detail: str | None = None

    @property
    def ok(self) -> bool:
        return self.failure is None and self.heartbeat is not None and self.purged is not None


def _timeout_ms(statement_timeout: timedelta) -> str:
    milliseconds = statement_timeout // timedelta(milliseconds=1)
    if milliseconds < 1:
        # PostgreSQL treats 0 as no timeout, and NFR-05 requires one on every query.
        raise ValueError("statement_timeout must be at least 1 ms")
    return str(milliseconds)


def _describe(exc: Exception) -> str:
    """Name the error without its message, which may carry connection details (NFR-02)."""
    sqlstate = getattr(exc, "sqlstate", None)
    return type(exc).__name__ if sqlstate is None else f"{type(exc).__name__} ({sqlstate})"


def _write(conn: Connection, timeout_ms: str) -> Heartbeat:
    nonce = uuid4()
    with conn.transaction():
        conn.execute(_SET_STATEMENT_TIMEOUT, (timeout_ms,))
        row = conn.execute(_INSERT_HEARTBEAT, (nonce,)).fetchone()
        if row is None:
            raise psycopg.DataError("heartbeat INSERT returned no row")
    return Heartbeat(nonce=nonce, written_at=row[0])


def _purge(conn: Connection, timeout_ms: str) -> int:
    with conn.transaction():
        conn.execute(_SET_STATEMENT_TIMEOUT, (timeout_ms,))
        return conn.execute(_PURGE_HEARTBEATS, (HEARTBEAT_RETENTION,)).rowcount


def beat(
    connect: Callable[[], Connection],
    *,
    statement_timeout: timedelta = DEFAULT_STATEMENT_TIMEOUT,
) -> BeatResult:
    """Write one heartbeat, then purge expired rows, in separate transactions."""
    timeout_ms = _timeout_ms(statement_timeout)

    try:
        conn = connect()
    except Exception as exc:
        # Credential lookups raise more than psycopg errors, and any failure here
        # means that no heartbeat was written.
        return BeatResult(failure=BeatFailure.WRITE, detail=_describe(exc))

    with closing(conn):
        try:
            heartbeat = _write(conn, timeout_ms)
        except psycopg.Error as exc:
            return BeatResult(failure=BeatFailure.WRITE, detail=_describe(exc))
        try:
            purged = _purge(conn, timeout_ms)
        except psycopg.Error as exc:
            return BeatResult(heartbeat=heartbeat, failure=BeatFailure.PURGE, detail=_describe(exc))
    return BeatResult(heartbeat=heartbeat, purged=purged)


def run_heartbeat_loop(
    connect: Callable[[], Connection],
    report: Callable[[BeatResult], None],
    stop: threading.Event,
    *,
    statement_timeout: timedelta = DEFAULT_STATEMENT_TIMEOUT,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Beat every 30 s until `stop` is set, passing each result to `report`.

    A failed beat does not end the loop, because the next one may succeed. Beats
    stay on a fixed 30 s grid, and a beat that overruns skips the slots it missed
    rather than bursting to catch up.
    """
    interval = HEARTBEAT_INTERVAL.total_seconds()
    started = clock()
    while not stop.is_set():
        report(beat(connect, statement_timeout=statement_timeout))
        stop.wait(interval - ((clock() - started) % interval))
