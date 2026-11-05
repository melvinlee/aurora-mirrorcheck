"""L0 heartbeat: write and purge rows on the primary writer (FR-01).

These are the only writes the tool makes (NFR-01, NFR-05). Each beat uses a new
connection from the caller's `connect` factory, which must return a connection
to the primary writer using `sslmode=verify-full` and credentials from Secrets
Manager or IAM DB auth (NFR-02, NFR-03).

The factory must return a connection with no transaction open. Inside an open
transaction `conn.transaction()` is only a savepoint, so closing the connection
would discard a row that had been reported as written. A beat refuses such a
connection and reports a write failure.

The factory must also set `connect_timeout`, and either `tcp_user_timeout` or
keepalives. `statement_timeout` and `lock_timeout` are enforced by the server, so
they can't help when the network drops packets, and without these settings a
lost peer holds the beat's worker until the operating system gives up.

Those two server timeouts cover statements, not COMMIT, and psycopg waits for a
COMMIT with no client-side limit. A commit can block for as long as the server
holds it, for example when `rds.global_db_rpo` or synchronous replication is
waiting for a standby. So each beat runs in a daemon worker thread with a
client-side deadline (default 25 s, inside the 30 s interval).

A beat that passes its deadline reports `BeatFailure.UNKNOWN`, because its row may
still commit later: it is neither a success nor a definite write failure. Its
worker is abandoned, not cancelled. The worker closes its own connection whenever
it finishes, and its late result is discarded so that it can never pass for a
later slot's beat. Until then the loop starts no new beat, so a server that holds
commits never collects a pile of waiting connections.

The heartbeat's 10 s `statement_timeout` is a deliberate departure from the 30 min
default in NFR-05, for the heartbeat only. A beat runs every 30 s, and a stuck one
must be reported as a failure (FR-03) rather than go silent for 30 minutes.

A beat never raises for a database failure. It returns a `BeatResult` that says
which step failed, so that a heartbeat that could not be written stays distinct
from a lag breach (FR-03) and a failure never reads as healthy (NFR-09).
"""

import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, wait
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
DEFAULT_STATEMENT_TIMEOUT: Final = timedelta(seconds=10)  # Per statement; see module docstring.
DEFAULT_LOCK_TIMEOUT: Final = timedelta(seconds=5)  # Fails a queued lock sooner than the above.
DEFAULT_DEADLINE: Final = timedelta(seconds=25)  # Client-side limit on a whole beat, COMMIT too.

_STOP_POLL_SECONDS: Final = 0.1  # How quickly the wait for a beat notices `stop`.

_SET_TIMEOUTS: Final = """
    SELECT set_config('statement_timeout', %s, true), set_config('lock_timeout', %s, true)
"""
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
    UNKNOWN = "unknown"  # No outcome by the deadline; the row may still commit later.


@dataclass(frozen=True)
class Heartbeat:
    """A committed heartbeat row. `written_at` comes from the primary's clock."""

    nonce: UUID
    written_at: datetime


@dataclass(frozen=True)
class BeatResult:
    """The outcome of one beat.

    After a purge failure `heartbeat` is still set, because the row was committed
    and can still be polled for on the secondary. After an unknown failure it is
    `None` even though a row may exist, because nothing is known about that row.
    """

    heartbeat: Heartbeat | None = None
    purged: int | None = None
    failure: BeatFailure | None = None
    detail: str | None = None

    @property
    def ok(self) -> bool:
        return self.failure is None and self.heartbeat is not None and self.purged is not None


def _timeout_ms(name: str, timeout: timedelta) -> str:
    milliseconds = timeout // timedelta(milliseconds=1)
    if milliseconds < 1:
        # PostgreSQL treats 0 as no timeout, and NFR-05 requires one on every query.
        raise ValueError(f"{name} must be at least 1 ms")
    return str(milliseconds)


def _timeouts(statement_timeout: timedelta, lock_timeout: timedelta) -> tuple[str, str]:
    return (
        _timeout_ms("statement_timeout", statement_timeout),
        _timeout_ms("lock_timeout", lock_timeout),
    )


def _deadline_seconds(deadline: timedelta) -> float:
    _timeout_ms("deadline", deadline)
    return deadline.total_seconds()


def _unknown(reason: str) -> BeatResult:
    return BeatResult(
        failure=BeatFailure.UNKNOWN,
        detail=f"{reason}: outcome unknown, the row may still commit later",
    )


def _describe(exc: Exception) -> str:
    """Name the error without its message, which may carry connection details (NFR-02)."""
    sqlstate = getattr(exc, "sqlstate", None)
    return type(exc).__name__ if sqlstate is None else f"{type(exc).__name__} ({sqlstate})"


def _write(conn: Connection, timeouts: tuple[str, str]) -> Heartbeat:
    nonce = uuid4()
    with conn.transaction():
        conn.execute(_SET_TIMEOUTS, timeouts)
        row = conn.execute(_INSERT_HEARTBEAT, (nonce,)).fetchone()
        if row is None:
            raise psycopg.DataError("heartbeat INSERT returned no row")
    return Heartbeat(nonce=nonce, written_at=row[0])


def _purge(conn: Connection, timeouts: tuple[str, str]) -> int:
    with conn.transaction():
        conn.execute(_SET_TIMEOUTS, timeouts)
        return conn.execute(_PURGE_HEARTBEATS, (HEARTBEAT_RETENTION,)).rowcount


def _run_beat(connect: Callable[[], Connection], timeouts: tuple[str, str]) -> BeatResult:
    """Write one heartbeat, then purge expired rows, in separate transactions."""
    try:
        conn = connect()
    except Exception as exc:
        # Credential lookups raise more than psycopg errors, and any failure here
        # means that no heartbeat was written.
        return BeatResult(failure=BeatFailure.WRITE, detail=_describe(exc))

    with closing(conn):
        status = conn.info.transaction_status
        if status != psycopg.pq.TransactionStatus.IDLE:
            return BeatResult(
                failure=BeatFailure.WRITE, detail=f"connection not idle ({status.name})"
            )
        try:
            heartbeat = _write(conn, timeouts)
        except psycopg.Error as exc:
            return BeatResult(failure=BeatFailure.WRITE, detail=_describe(exc))
        try:
            purged = _purge(conn, timeouts)
        except psycopg.Error as exc:
            return BeatResult(heartbeat=heartbeat, failure=BeatFailure.PURGE, detail=_describe(exc))
    return BeatResult(heartbeat=heartbeat, purged=purged)


def _start_beat(connect: Callable[[], Connection], timeouts: tuple[str, str]) -> Future[BeatResult]:
    """Start a beat on a daemon thread, which can't hold up process exit.

    This is a bare thread because `ThreadPoolExecutor` joins its workers at exit.
    """
    future: Future[BeatResult] = Future()

    def work() -> None:
        try:
            future.set_result(_run_beat(connect, timeouts))
        except BaseException as exc:
            future.set_exception(exc)  # A bug must reach the caller, not die with this thread.

    threading.Thread(target=work, name="aurora-mirrorcheck-beat", daemon=True).start()
    return future


def _await_beat(
    future: Future[BeatResult], deadline: float, stop: threading.Event | None = None
) -> BeatResult:
    """Return the beat's result, or an unknown failure at the deadline or on `stop`."""
    expires = time.monotonic() + deadline
    while True:
        remaining = expires - time.monotonic()
        done, _ = wait([future], timeout=max(0.0, min(remaining, _STOP_POLL_SECONDS)))
        if done:
            return future.result()
        if stop is not None and stop.is_set():
            return _unknown("stopped during the beat")
        if remaining <= 0:
            return _unknown("the beat passed its deadline")


def beat(
    connect: Callable[[], Connection],
    *,
    statement_timeout: timedelta = DEFAULT_STATEMENT_TIMEOUT,
    lock_timeout: timedelta = DEFAULT_LOCK_TIMEOUT,
    deadline: timedelta = DEFAULT_DEADLINE,
) -> BeatResult:
    """Run one beat in a worker thread, and wait for it for at most `deadline`.

    This is a single beat. Calling it again after an unknown failure can overlap
    the earlier one, so repeat beats through `run_heartbeat_loop`.
    """
    timeouts = _timeouts(statement_timeout, lock_timeout)
    seconds = _deadline_seconds(deadline)
    return _await_beat(_start_beat(connect, timeouts), seconds)


def run_heartbeat_loop(
    connect: Callable[[], Connection],
    report: Callable[[BeatResult], None],
    stop: threading.Event,
    *,
    statement_timeout: timedelta = DEFAULT_STATEMENT_TIMEOUT,
    lock_timeout: timedelta = DEFAULT_LOCK_TIMEOUT,
    deadline: timedelta = DEFAULT_DEADLINE,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Beat every 30 s until `stop` is set, passing each result to `report`.

    A failed beat does not end the loop, because the next one may succeed. Beats
    stay on a fixed 30 s grid. `deadline` must be under the interval, so that a
    stuck beat can't silently swallow a slot.

    Beats never overlap. While an earlier worker is still running, each slot
    reports an unknown failure and starts nothing, and `stop` and `report` keep
    working. A worker's late result is discarded, but a late unexpected error is
    raised at the next slot, as it would have been on time.
    """
    timeouts = _timeouts(statement_timeout, lock_timeout)
    seconds = _deadline_seconds(deadline)
    interval = HEARTBEAT_INTERVAL.total_seconds()
    if seconds >= interval:
        raise ValueError(f"deadline must be under the {interval:g} s interval")

    started = clock()
    worker: Future[BeatResult] | None = None
    while not stop.is_set():
        if worker is not None and not worker.done():
            report(_unknown("an earlier beat is still running, so no beat started"))
        else:
            if worker is not None:
                worker.result()
            worker = _start_beat(connect, timeouts)
            report(_await_beat(worker, seconds, stop))
        stop.wait(interval - ((clock() - started) % interval))
