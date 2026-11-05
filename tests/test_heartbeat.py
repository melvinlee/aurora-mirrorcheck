import os
import subprocess
import sys
import threading
import time
import traceback
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Self, cast
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.pq import TransactionStatus

from aurora_mirrorcheck.heartbeat import (
    DEFAULT_DEADLINE,
    DEFAULT_LOCK_TIMEOUT,
    DEFAULT_STATEMENT_TIMEOUT,
    HEARTBEAT_RETENTION,
    BeatFailure,
    BeatResult,
    Connection,
    Heartbeat,
    beat,
    run_heartbeat_loop,
)

WRITTEN_AT = datetime(2026, 1, 1, tzinfo=UTC)
SCHEMA_SQL = Path(__file__).parents[1] / "sql" / "heartbeat.sql"
DSN_VARIABLE = "AURORA_MIRRORCHECK_TEST_DSN"
LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
# Each of these can send libpq somewhere other than the `host` in the DSN.
HOST_OVERRIDES = frozenset({"hostaddr", "service"})
HOST_OVERRIDE_VARIABLES = ("PGHOSTADDR", "PGSERVICE")
SHORT_DEADLINE = timedelta(milliseconds=50)
# A test's blocked commit gives up after this long, so a bug fails it instead of hanging it.
BLOCK_LIMIT_SECONDS = 10.0
# The missing-standby test's value. Only a value this exact is safe to clear.
STANDBY_SENTINEL = "no_such_standby"
SHOW_STANDBY_NAMES = "SHOW synchronous_standby_names"
# The database that the `pg-podman` agent creates. `ALTER SYSTEM` runs nowhere else.
TEST_DATABASE = "mirrorcheck_test"


def _dsn_refusal() -> str | None:
    """Say why the DSN is refused, or return `None` if it can reach only this machine.

    This returns rather than raises so that no frame holding the DSN, or the
    password parsed out of it, is on the traceback for pytest to render (NFR-02).
    """
    try:
        params = conninfo_to_dict(os.environ.get(DSN_VARIABLE, ""))
    except psycopg.Error, ValueError:  # A non-UTF-8 byte raises UnicodeEncodeError.
        return "it could not be parsed"

    if params.get("host") not in LOCAL_HOSTS:
        return f"its host is not one of {sorted(LOCAL_HOSTS)}"
    if HOST_OVERRIDES & params.keys():
        return f"it sets {sorted(HOST_OVERRIDES & params.keys())}"
    overrides = [name for name in HOST_OVERRIDE_VARIABLES if name in os.environ]
    if overrides:
        return f"the environment sets {overrides}"
    return None


def require_local_dsn() -> str:
    """Return the test DSN only if it can reach nothing but this machine.

    The `database` tests drop the `aurora_mirrorcheck` schema, so anything else
    raises. It reads the variable itself so that the DSN is never an argument,
    which pytest prints in a failure. Don't keep the result in a local either.
    """
    refusal = _dsn_refusal()
    if refusal is not None:
        raise ValueError(f"{DSN_VARIABLE} refused: {refusal}")
    return os.environ[DSN_VARIABLE]


def connect_to_test_server(**kwargs: Any) -> Connection:
    """Connect without letting a failure render psycopg's frames, which hold the DSN (NFR-02)."""
    try:
        return psycopg.connect(require_local_dsn(), **kwargs)
    except psycopg.Error as exc:
        raise ConnectionError(f"could not connect to the test server: {_error_name(exc)}") from None


def _error_name(exc: psycopg.Error) -> str:
    return type(exc).__name__ if exc.sqlstate is None else f"{type(exc).__name__} ({exc.sqlstate})"


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None, rowcount: int) -> None:
        self._row = row
        self.rowcount = rowcount

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class FakeTransaction:
    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    def __enter__(self) -> Self:
        self._conn.log.append("BEGIN")
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if not exc_type:
            self._conn.before_commit()
        self._conn.log.append("ROLLBACK" if exc_type else "COMMIT")


class FakeConnection:
    """Records statements in order, and fails any statement containing `fail_on`.

    `status` is what the connection reports on arrival.
    """

    def __init__(
        self,
        fail_on: str | None = None,
        error: Exception | None = None,
        insert_row: tuple[Any, ...] | None = (WRITTEN_AT,),
        purged: int = 0,
        status: TransactionStatus = TransactionStatus.IDLE,
    ) -> None:
        self.info = SimpleNamespace(transaction_status=status)
        self.log: list[str] = []
        self.params: list[tuple[Any, ...]] = []
        self.closed = False
        self._fail_on = fail_on
        self._error = error or psycopg.OperationalError("boom")
        self._insert_row = insert_row
        self._purged = purged

    def before_commit(self) -> None:
        """Runs where a real COMMIT would wait, outside any server timeout."""

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

    def execute(self, query: str, params: tuple[Any, ...]) -> FakeCursor:
        self.log.append(query)
        self.params.append(params)
        if self._fail_on is not None and self._fail_on in query:
            raise self._error
        return FakeCursor(self._insert_row if "INSERT" in query else None, self._purged)

    def close(self) -> None:
        self.closed = True


class StuckCommitConnection(FakeConnection):
    """Blocks in its first COMMIT until `release` is set, then raises `late_error` if given."""

    def __init__(self, late_error: Exception | None = None) -> None:
        super().__init__()
        self.release = threading.Event()
        self.committing = threading.Event()
        self.late_error = late_error

    def before_commit(self) -> None:
        self.committing.set()
        if not self.release.wait(BLOCK_LIMIT_SECONDS):
            raise AssertionError("nothing released the blocked commit")
        if self.late_error is not None:
            raise self.late_error


def connector(conn: FakeConnection) -> Callable[[], Connection]:
    return lambda: cast(Connection, conn)


def counting_connector(*conns: FakeConnection) -> tuple[Callable[[], Connection], list[None]]:
    """Hand out `conns` in turn, and record one entry for each connection opened."""
    remaining = iter(conns)
    opened: list[None] = []

    def connect() -> Connection:
        opened.append(None)
        return cast(Connection, next(remaining))

    return connect, opened


def wait_until(condition: Callable[[], bool]) -> None:
    limit = time.monotonic() + BLOCK_LIMIT_SECONDS
    while not condition():
        assert time.monotonic() < limit, "condition not met in time"
        time.sleep(0.01)


def test_beat_writes_then_purges_in_separate_committed_transactions() -> None:
    conn = FakeConnection(purged=3)

    result = beat(connector(conn))

    assert result.ok
    assert result.failure is None
    assert result.purged == 3
    assert result.heartbeat is not None
    assert result.heartbeat.written_at == WRITTEN_AT
    assert conn.log[0] == "BEGIN"
    assert "set_config" in conn.log[1]
    assert "INSERT INTO aurora_mirrorcheck.heartbeat" in conn.log[2]
    assert conn.log[3:5] == ["COMMIT", "BEGIN"]
    assert "set_config" in conn.log[5]
    assert "DELETE FROM aurora_mirrorcheck.heartbeat" in conn.log[6]
    assert conn.log[7] == "COMMIT"
    assert conn.closed


def test_only_statements_are_timeout_insert_and_delete_on_the_heartbeat_table() -> None:
    conn = FakeConnection()

    beat(connector(conn))

    statements = [q for q in conn.log if q not in ("BEGIN", "COMMIT")]
    assert len(statements) == 4
    for query in statements:
        assert "set_config" in query or "aurora_mirrorcheck.heartbeat" in query


def test_each_beat_uses_a_new_nonce_bound_as_a_parameter() -> None:
    first, second = FakeConnection(), FakeConnection()

    results = [beat(connector(first)), beat(connector(second))]

    heartbeats = [r.heartbeat for r in results]
    assert all(isinstance(h, Heartbeat) for h in heartbeats)
    nonces = [h.nonce for h in heartbeats if h is not None]
    assert len(set(nonces)) == 2
    assert first.params[1] == (nonces[0],)
    assert second.params[1] == (nonces[1],)


def test_purge_binds_the_seven_day_retention() -> None:
    conn = FakeConnection()

    beat(connector(conn))

    assert HEARTBEAT_RETENTION == timedelta(days=7)
    assert conn.params[3] == (HEARTBEAT_RETENTION,)


def test_heartbeat_timeouts_default_to_10_and_5_seconds() -> None:
    conn = FakeConnection()

    beat(connector(conn))

    assert DEFAULT_STATEMENT_TIMEOUT == timedelta(seconds=10)
    assert DEFAULT_LOCK_TIMEOUT == timedelta(seconds=5)
    assert conn.params[0] == ("10000", "5000")


def test_both_timeouts_apply_to_every_transaction() -> None:
    conn = FakeConnection()

    beat(
        connector(conn),
        statement_timeout=timedelta(seconds=7),
        lock_timeout=timedelta(seconds=3),
    )

    assert "'statement_timeout'" in conn.log[1]
    assert "'lock_timeout'" in conn.log[1]
    assert conn.log[5] == conn.log[1]
    assert conn.params[0] == ("7000", "3000")
    assert conn.params[2] == ("7000", "3000")


@pytest.mark.parametrize("name", ["statement_timeout", "lock_timeout", "deadline"])
@pytest.mark.parametrize("timeout", [timedelta(0), timedelta(microseconds=1), timedelta(-1)])
def test_a_timeout_that_would_disable_the_limit_is_rejected(name: str, timeout: timedelta) -> None:
    conn = FakeConnection()

    with pytest.raises(ValueError, match=name):
        beat(connector(conn), **{name: timeout})

    assert conn.log == []


@pytest.mark.parametrize("name", ["statement_timeout", "lock_timeout", "deadline"])
def test_the_loop_rejects_a_bad_timeout_before_starting_a_beat(name: str) -> None:
    conn = FakeConnection()

    with pytest.raises(ValueError, match=name):
        run_heartbeat_loop(
            connector(conn), lambda _: None, CountdownStop(ticks=1), **{name: timedelta(0)}
        )

    assert conn.log == []


@pytest.mark.parametrize("deadline", [timedelta(seconds=30), timedelta(minutes=5)])
def test_the_loop_rejects_a_deadline_that_could_swallow_a_slot(deadline: timedelta) -> None:
    conn = FakeConnection()

    with pytest.raises(ValueError, match="deadline must be under"):
        run_heartbeat_loop(
            connector(conn), lambda _: None, CountdownStop(ticks=1), deadline=deadline
        )

    assert conn.log == []


@pytest.mark.parametrize(
    "status",
    [
        TransactionStatus.INTRANS,
        TransactionStatus.INERROR,
        TransactionStatus.ACTIVE,
        TransactionStatus.UNKNOWN,
    ],
)
def test_a_connection_that_is_not_idle_is_a_write_failure(status: TransactionStatus) -> None:
    conn = FakeConnection(status=status)

    result = beat(connector(conn))

    assert not result.ok
    assert result.failure is BeatFailure.WRITE
    assert result.heartbeat is None
    assert result.detail == f"connection not idle ({status.name})"
    assert conn.log == []
    assert conn.closed


def test_failed_connection_is_a_write_failure_and_not_ok() -> None:
    def connect() -> Connection:
        raise psycopg.OperationalError("password=hunter2 rejected")

    result = beat(connect)

    assert not result.ok
    assert result.failure is BeatFailure.WRITE
    assert result.heartbeat is None
    assert result.detail == "OperationalError"


def test_credential_lookup_failure_is_a_write_failure() -> None:
    def connect() -> Connection:
        raise RuntimeError("secret value: hunter2")

    result = beat(connect)

    assert result.failure is BeatFailure.WRITE
    assert result.detail == "RuntimeError"


def test_failed_insert_is_a_write_failure_and_skips_the_purge() -> None:
    conn = FakeConnection(fail_on="INSERT")

    result = beat(connector(conn))

    assert not result.ok
    assert result.failure is BeatFailure.WRITE
    assert result.heartbeat is None
    assert "ROLLBACK" in conn.log
    assert not any("DELETE" in q for q in conn.log)
    assert conn.closed


def test_insert_on_a_read_only_endpoint_reports_the_sqlstate() -> None:
    conn = FakeConnection(fail_on="INSERT", error=psycopg.errors.ReadOnlySqlTransaction("nope"))

    result = beat(connector(conn))

    assert result.failure is BeatFailure.WRITE
    assert result.detail == "ReadOnlySqlTransaction (25006)"


def test_insert_returning_no_row_is_a_write_failure() -> None:
    conn = FakeConnection(insert_row=None)

    result = beat(connector(conn))

    assert not result.ok
    assert result.failure is BeatFailure.WRITE
    assert "COMMIT" not in conn.log


def test_failed_purge_keeps_the_heartbeat_but_is_not_ok() -> None:
    conn = FakeConnection(fail_on="DELETE")

    result = beat(connector(conn))

    assert not result.ok
    assert result.failure is BeatFailure.PURGE
    assert result.heartbeat is not None
    assert result.purged is None
    assert conn.closed


def test_a_result_with_no_failure_but_no_row_is_not_ok() -> None:
    assert not BeatResult().ok


def test_unexpected_errors_propagate_instead_of_reading_as_healthy() -> None:
    conn = FakeConnection(fail_on="INSERT", error=KeyError("bug"))

    with pytest.raises(KeyError):
        beat(connector(conn))

    assert conn.closed


class CountdownStop(threading.Event):
    """Records waits instead of sleeping, and stops after `ticks` of them."""

    def __init__(self, ticks: int) -> None:
        super().__init__()
        self.ticks = ticks
        self.waits: list[float | None] = []

    def wait(self, timeout: float | None = None) -> bool:
        self.waits.append(timeout)
        if len(self.waits) >= self.ticks:
            self.set()
        return self.is_set()


def test_loop_beats_every_30_seconds_and_reports_each_result() -> None:
    results: list[BeatResult] = []
    stop = CountdownStop(ticks=3)

    run_heartbeat_loop(connector(FakeConnection()), results.append, stop, clock=lambda: 0.0)

    assert len(results) == 3
    assert stop.waits == [30.0, 30.0, 30.0]


def test_loop_subtracts_beat_duration_and_skips_overrun_slots() -> None:
    times = iter([0.0, 2.0, 37.0])
    stop = CountdownStop(ticks=2)

    run_heartbeat_loop(connector(FakeConnection()), lambda _: None, stop, clock=lambda: next(times))

    assert stop.waits == [28.0, 23.0]


def test_loop_continues_after_a_failed_beat() -> None:
    conns = iter([FakeConnection(fail_on="INSERT"), FakeConnection()])
    results: list[BeatResult] = []

    run_heartbeat_loop(
        lambda: cast(Connection, next(conns)),
        results.append,
        CountdownStop(ticks=2),
        clock=lambda: 0.0,
    )

    assert [r.failure for r in results] == [BeatFailure.WRITE, None]


def test_loop_passes_both_timeouts_to_each_beat() -> None:
    conn = FakeConnection()

    run_heartbeat_loop(
        connector(conn),
        lambda _: None,
        CountdownStop(ticks=1),
        statement_timeout=timedelta(seconds=7),
        lock_timeout=timedelta(seconds=3),
        clock=lambda: 0.0,
    )

    assert conn.params[0] == ("7000", "3000")


def test_loop_does_not_beat_once_stopped() -> None:
    conn = FakeConnection()
    stop = threading.Event()
    stop.set()

    run_heartbeat_loop(connector(conn), lambda _: None, stop)

    assert conn.log == []


def test_deadline_defaults_to_inside_the_30_second_interval() -> None:
    assert DEFAULT_DEADLINE == timedelta(seconds=25)


def test_a_commit_stuck_past_the_deadline_is_unknown_and_never_ok() -> None:
    conn = StuckCommitConnection()

    result = beat(connector(conn), deadline=SHORT_DEADLINE)

    assert conn.committing.is_set()
    assert not conn.closed
    assert not result.ok
    assert result.failure is BeatFailure.UNKNOWN
    assert result.heartbeat is None
    assert result.purged is None
    assert result.detail is not None
    assert "unknown" in result.detail
    assert "may still commit" in result.detail

    conn.release.set()
    wait_until(lambda: conn.closed)


def test_a_beat_that_beats_the_deadline_still_reports_its_result() -> None:
    conn = StuckCommitConnection()
    conn.release.set()

    assert beat(connector(conn), deadline=timedelta(seconds=5)).ok


def test_loop_reports_each_slot_while_stuck_and_never_overlaps_a_beat() -> None:
    stuck = StuckCommitConnection()
    connect, opened = counting_connector(stuck, FakeConnection())
    results: list[BeatResult] = []
    stop = CountdownStop(ticks=4)

    run_heartbeat_loop(connect, results.append, stop, deadline=SHORT_DEADLINE, clock=lambda: 0.0)
    stuck.release.set()

    assert len(opened) == 1
    assert stop.waits == [30.0, 30.0, 30.0, 30.0]
    assert [r.failure for r in results] == [BeatFailure.UNKNOWN] * 4
    assert not any(r.ok for r in results)
    assert all(r.detail is not None and "unknown" in r.detail for r in results)
    assert results[0].detail != results[1].detail
    wait_until(lambda: stuck.closed)


def test_loop_honours_stop_during_a_stuck_commit_without_waiting_for_the_deadline() -> None:
    stuck = StuckCommitConnection()
    results: list[BeatResult] = []
    stop = threading.Event()
    loop = threading.Thread(
        target=run_heartbeat_loop, args=(connector(stuck), results.append, stop), daemon=True
    )

    loop.start()
    assert stuck.committing.wait(BLOCK_LIMIT_SECONDS)
    stop.set()
    loop.join(5)
    stopped = not loop.is_alive()
    stuck.release.set()

    assert stopped
    assert [r.failure for r in results] == [BeatFailure.UNKNOWN]
    wait_until(lambda: stuck.closed)


def test_a_late_result_is_dropped_and_never_stands_in_for_a_later_slot() -> None:
    stuck = StuckCommitConnection()
    fresh = FakeConnection()
    connect, opened = counting_connector(stuck, fresh)
    results: list[BeatResult] = []

    class ReleaseAfterFirstSlot(CountdownStop):
        def wait(self, timeout: float | None = None) -> bool:
            if not self.waits:
                stuck.release.set()
                wait_until(lambda: stuck.closed)
            return super().wait(timeout)

    run_heartbeat_loop(
        connect,
        results.append,
        ReleaseAfterFirstSlot(ticks=2),
        deadline=SHORT_DEADLINE,
        clock=lambda: 0.0,
    )

    assert len(opened) == 2
    assert [r.failure for r in results] == [BeatFailure.UNKNOWN, None]
    late_nonce = stuck.params[1][0]
    assert results[1].heartbeat is not None
    assert results[1].heartbeat.nonce == fresh.params[1][0]
    assert results[1].heartbeat.nonce != late_nonce


def test_a_late_unexpected_error_is_raised_at_the_next_slot() -> None:
    stuck = StuckCommitConnection(late_error=KeyError("bug"))
    connect, opened = counting_connector(stuck, FakeConnection())

    class ReleaseAfterFirstSlot(CountdownStop):
        def wait(self, timeout: float | None = None) -> bool:
            stuck.release.set()
            wait_until(lambda: stuck.closed)
            return super().wait(timeout)

    with pytest.raises(KeyError):
        run_heartbeat_loop(
            connect,
            lambda _: None,
            ReleaseAfterFirstSlot(ticks=2),
            deadline=SHORT_DEADLINE,
            clock=lambda: 0.0,
        )

    assert len(opened) == 1


def test_a_stuck_worker_does_not_hold_up_process_exit() -> None:
    script = """
import threading
from datetime import timedelta
from aurora_mirrorcheck.heartbeat import run_heartbeat_loop

class Stop(threading.Event):
    def wait(self, timeout=None):
        self.set()
        return True

def connect():
    threading.Event().wait()

run_heartbeat_loop(connect, print, Stop(), deadline=timedelta(milliseconds=50))
"""
    finished = subprocess.run(  # noqa: S603 - fixed script, run with this interpreter
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=20, check=True
    )

    assert "unknown" in finished.stdout


@pytest.fixture
def no_host_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in HOST_OVERRIDE_VARIABLES:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def no_connections(monkeypatch: pytest.MonkeyPatch) -> None:
    def connect(*args: Any, **kwargs: Any) -> Connection:
        pytest.fail("a refused DSN reached psycopg.connect")

    monkeypatch.setattr(psycopg, "connect", connect)


@pytest.mark.usefixtures("no_host_overrides")
@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://postgres:secret@127.0.0.1:55433/mirrorcheck_test",
        "postgresql://postgres:secret@localhost/mirrorcheck_test",
        "postgresql://postgres:secret@[::1]:55433/mirrorcheck_test",
        "host=127.0.0.1 port=55433 dbname=mirrorcheck_test",
        "host=::1 dbname=mirrorcheck_test",
    ],
)
def test_guard_accepts_a_loopback_dsn(dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(DSN_VARIABLE, dsn)

    assert require_local_dsn() == dsn


@pytest.mark.usefixtures("no_host_overrides")
@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://postgres:secret@db.example.com/prod",
        "postgresql://postgres:secret@writer.cluster-x.us-east-1.rds.amazonaws.com/prod",
        "host=10.0.0.1 dbname=prod",
        "host=127.0.0.2 dbname=prod",
        "host=LOCALHOST dbname=prod",
        "host=localhost.example.com dbname=prod",
        "postgresql://postgres:secret@127.0.0.1.example.com/prod",
        # Several hosts: libpq would fall through to the remote one.
        "host=localhost,db.example.com dbname=prod",
        "postgresql://postgres:secret@localhost,db.example.com/prod",
        # The query string overrides the authority.
        "postgresql://postgres:secret@localhost/prod?host=db.example.com",
        # No host: libpq falls back to PGHOST or a socket, whatever those are today.
        "",
        "dbname=prod",
        "postgresql:///prod",
        "host=/var/run/postgresql dbname=prod",
    ],
)
def test_guard_refuses_any_other_host(dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(DSN_VARIABLE, dsn)

    with pytest.raises(ValueError, match="refused: its host"):
        require_local_dsn()


@pytest.mark.usefixtures("no_host_overrides")
def test_guard_refuses_an_unset_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(DSN_VARIABLE, raising=False)

    with pytest.raises(ValueError, match="refused: its host"):
        require_local_dsn()


@pytest.mark.usefixtures("no_host_overrides")
@pytest.mark.parametrize(
    "dsn",
    [
        "host=localhost hostaddr=10.0.0.1 dbname=prod",
        "postgresql://postgres:secret@localhost/prod?hostaddr=10.0.0.1",
        "host=localhost service=prod",
    ],
)
def test_guard_refuses_a_local_host_that_another_setting_overrides(
    dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(DSN_VARIABLE, dsn)

    with pytest.raises(ValueError, match="refused: it sets"):
        require_local_dsn()


@pytest.mark.parametrize("name", HOST_OVERRIDE_VARIABLES)
def test_guard_refuses_a_host_override_in_the_environment(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(name, "10.0.0.1")
    monkeypatch.setenv(DSN_VARIABLE, "host=127.0.0.1 dbname=mirrorcheck_test")

    with pytest.raises(ValueError, match=name):
        require_local_dsn()


@pytest.mark.parametrize(
    "dsn",
    ["postgresql://postgres:hunter2@db.example.com/prod", "hunter2"],
)
def test_a_refusal_never_quotes_the_password(dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(DSN_VARIABLE, dsn)

    with pytest.raises(ValueError, match="refused") as refused:
        require_local_dsn()

    assert "hunter2" not in "".join(traceback.format_exception(refused.value))


@pytest.mark.usefixtures("no_connections")
def test_the_database_fixture_fails_on_a_refused_dsn_instead_of_skipping(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(DSN_VARIABLE, "postgresql://postgres:secret@db.example.com/prod")

    # `pytest.skip` raises a `BaseException`, which `pytest.raises` would let through
    # as a skip, so this test would read as SKIPPED rather than fail.
    with pytest.raises(ValueError, match="refused"):
        try:
            request.getfixturevalue("database")
        except pytest.skip.Exception:
            pytest.fail("a refused DSN skipped the database tests instead of failing them")


@pytest.mark.usefixtures("no_connections")
def test_direct_admin_queries_refuse_a_remote_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(DSN_VARIABLE, "postgresql://postgres:secret@db.example.com/prod")

    with pytest.raises(ValueError, match="refused"):
        admin_rows("SELECT 1")


@pytest.fixture
def dsn_is_set() -> None:
    if DSN_VARIABLE not in os.environ:
        pytest.skip(f"{DSN_VARIABLE} is not set")


@pytest.fixture
def database(dsn_is_set: None) -> Iterator[Callable[..., Connection]]:
    """Connect as a role with only the grants FR-01 needs, on a disposable local server."""
    clear_standby_setting_left_by_a_killed_run()

    role = "mirrorcheck_test"
    with connect_to_test_server(autocommit=True) as admin:
        admin.execute("DROP SCHEMA IF EXISTS aurora_mirrorcheck CASCADE")
        admin.execute(f"DROP ROLE IF EXISTS {role}")
        admin.execute(f"CREATE ROLE {role} LOGIN")
        admin.execute(SCHEMA_SQL.read_text().encode())
        admin.execute(f"GRANT USAGE ON SCHEMA aurora_mirrorcheck TO {role}")
        admin.execute(f"GRANT SELECT, INSERT, DELETE ON aurora_mirrorcheck.heartbeat TO {role}")

    def connect(**kwargs: Any) -> Connection:
        conn = connect_to_test_server(**kwargs)
        conn.execute(f"SET SESSION AUTHORIZATION {role}")
        conn.commit()
        return conn

    yield connect

    with connect_to_test_server(autocommit=True) as admin:
        admin.execute("DROP SCHEMA aurora_mirrorcheck CASCADE")
        admin.execute(f"DROP ROLE {role}")


def admin_rows(sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with connect_to_test_server(autocommit=True) as admin:
        return admin.execute(sql.encode(), params).fetchall()


@pytest.mark.database
def test_real_beat_commits_a_row_written_by_the_server(
    database: Callable[..., Connection],
) -> None:
    result = beat(database)

    assert result.ok
    assert result.heartbeat is not None
    rows = admin_rows("SELECT nonce, written_at FROM aurora_mirrorcheck.heartbeat")
    assert rows == [(result.heartbeat.nonce, result.heartbeat.written_at)]


@pytest.mark.database
def test_real_beat_commits_under_autocommit_too(database: Callable[..., Connection]) -> None:
    result = beat(lambda: database(autocommit=True))

    assert result.ok
    assert len(admin_rows("SELECT 1 FROM aurora_mirrorcheck.heartbeat")) == 1


@pytest.mark.database
def test_real_purge_removes_only_rows_older_than_seven_days(
    database: Callable[..., Connection],
) -> None:
    expired, kept = uuid4(), uuid4()
    admin_rows(
        """
        INSERT INTO aurora_mirrorcheck.heartbeat (nonce, written_at) VALUES
            (%s, now() - interval '7 days 1 minute'),
            (%s, now() - interval '6 days 23 hours')
        RETURNING 1
        """,
        (expired, kept),
    )

    result = beat(database)

    assert result.ok
    assert result.purged == 1
    assert result.heartbeat is not None
    remaining = {r[0] for r in admin_rows("SELECT nonce FROM aurora_mirrorcheck.heartbeat")}
    assert remaining == {kept, result.heartbeat.nonce}


@pytest.mark.database
def test_real_read_only_session_is_a_write_failure(database: Callable[..., Connection]) -> None:
    def connect() -> Connection:
        conn = database()
        conn.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        conn.commit()
        return conn

    result = beat(connect)

    assert result.failure is BeatFailure.WRITE
    assert result.detail == "ReadOnlySqlTransaction (25006)"
    assert admin_rows("SELECT 1 FROM aurora_mirrorcheck.heartbeat") == []


@pytest.mark.database
def test_real_open_transaction_is_a_write_failure_and_adds_no_row(
    database: Callable[..., Connection],
) -> None:
    def connect() -> Connection:
        conn = database()
        conn.execute("SELECT 1")
        return conn

    result = beat(connect)

    assert not result.ok
    assert result.failure is BeatFailure.WRITE
    assert result.heartbeat is None
    assert result.detail == "connection not idle (INTRANS)"
    assert admin_rows("SELECT 1 FROM aurora_mirrorcheck.heartbeat") == []


@pytest.mark.database
def test_real_statement_timeout_cancels_a_blocked_write(
    database: Callable[..., Connection],
) -> None:
    with connect_to_test_server() as locker:
        locker.execute("LOCK TABLE aurora_mirrorcheck.heartbeat")

        result = beat(database, statement_timeout=timedelta(milliseconds=200))

    assert result.failure is BeatFailure.WRITE
    assert result.detail == "QueryCanceled (57014)"


@pytest.mark.database
def test_real_lock_timeout_fails_a_queued_lock_before_the_statement_timeout(
    database: Callable[..., Connection],
) -> None:
    with connect_to_test_server() as locker:
        locker.execute("LOCK TABLE aurora_mirrorcheck.heartbeat")

        result = beat(
            database,
            statement_timeout=timedelta(seconds=30),
            lock_timeout=timedelta(milliseconds=200),
        )

    assert result.failure is BeatFailure.WRITE
    assert result.detail == "LockNotAvailable (55P03)"


def refuse_unless_on_the_test_database(admin: Connection) -> None:
    """Fail unless `admin` is on the `pg-podman` database.

    A loopback tunnel to a real server passes the host guard, and `ALTER SYSTEM`
    there could block every commit. Failing rather than skipping keeps that visible.
    """
    row = admin.execute("SELECT current_database()").fetchone()
    if row != (TEST_DATABASE,):
        pytest.fail(f"refused ALTER SYSTEM: this is not the {TEST_DATABASE} database: {row}")


def set_synchronous_standby_names(value: str) -> None:
    """Change the server-wide setting, and return once new sessions see it."""
    with connect_to_test_server(autocommit=True) as admin:
        refuse_unless_on_the_test_database(admin)
        if value:
            admin.execute(f"ALTER SYSTEM SET synchronous_standby_names = '{value}'".encode())
        else:
            admin.execute("ALTER SYSTEM RESET synchronous_standby_names")
        admin.execute("SELECT pg_reload_conf()")

    wait_until(lambda: admin_rows(SHOW_STANDBY_NAMES) == [(value,)])


def a_commit_finishes(limit: float = BLOCK_LIMIT_SECONDS) -> bool:
    """Say whether a commit that wrote WAL gets through in time.

    Only such a commit waits for a standby, so a temporary table is the smallest
    probe that can block. The wait ends once COMMIT sees the reset, which proves that
    the reset took effect. It runs in a daemon thread because psycopg won't limit it.
    """
    finished = threading.Event()

    def commit() -> None:
        with connect_to_test_server(autocommit=True) as conn:
            conn.execute("CREATE TEMP TABLE commit_probe ()")
        finished.set()

    threading.Thread(target=commit, daemon=True).start()
    return finished.wait(limit)


def clear_standby_setting_left_by_a_killed_run() -> None:
    """Undo the missing-standby test's setting, which would block every later commit.

    Any other value isn't ours to change, so it stops the run before a commit can hang.
    """
    value = admin_rows(SHOW_STANDBY_NAMES)[0][0]
    if value == STANDBY_SENTINEL:
        set_synchronous_standby_names("")
        if not a_commit_finishes():
            pytest.fail("commits still block after resetting synchronous_standby_names")
    elif value:
        pytest.fail(
            f"synchronous_standby_names is {value!r}, which these tests did not set, so "
            "commits may block forever. It was left unchanged: clear it or use another server."
        )


@contextmanager
def standby_setting_in_force(value: str) -> Iterator[None]:
    """Hold `value` for the block, and always reset it afterwards.

    If the block hangs in a commit that waits for the standby, a watchdog resets the
    setting after `BLOCK_LIMIT_SECONDS`, and that fails the test instead of hanging it.
    """
    released_a_hang = threading.Event()

    def release() -> None:
        released_a_hang.set()
        set_synchronous_standby_names("")

    watchdog = threading.Timer(BLOCK_LIMIT_SECONDS, release)
    try:
        set_synchronous_standby_names(value)
        time.sleep(1)  # COMMIT lags the setting: see the missing-standby test.
        watchdog.start()
        yield
        assert not released_a_hang.is_set(), "the block hung until the watchdog released it"
    finally:
        watchdog.cancel()
        set_synchronous_standby_names("")


@pytest.mark.database
def test_real_deadline_fires_while_commit_waits_for_a_missing_standby(
    database: Callable[..., Connection],
) -> None:
    opened: list[Connection] = []

    def connect() -> Connection:
        opened.append(database())
        return opened[-1]

    try:
        set_synchronous_standby_names(STANDBY_SENTINEL)
        # Sessions read the setting at once, but COMMIT follows a flag that the
        # checkpointer sets after its own reload, and nothing shows when that happens.
        time.sleep(1)

        before = time.monotonic()
        result = beat(
            connect,
            statement_timeout=timedelta(milliseconds=200),
            lock_timeout=timedelta(milliseconds=200),
            deadline=timedelta(seconds=2),
        )
        elapsed = time.monotonic() - before
        waiting = admin_rows("SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep'")
    finally:
        set_synchronous_standby_names("")
        if opened:
            wait_until(lambda: opened[0].closed)

    assert result.failure is BeatFailure.UNKNOWN
    assert not result.ok
    assert 2 <= elapsed < 5
    assert waiting == [(1,)]
    # Reported as unknown, and it did commit once the wait ended: "may still commit".
    assert len(admin_rows("SELECT 1 FROM aurora_mirrorcheck.heartbeat")) == 1


@pytest.mark.database
@pytest.mark.usefixtures("dsn_is_set")
def test_real_database_fixture_recovers_from_a_killed_run(request: pytest.FixtureRequest) -> None:
    with standby_setting_in_force(STANDBY_SENTINEL):
        assert not a_commit_finishes(limit=0.5)

        connect = request.getfixturevalue("database")

        assert admin_rows(SHOW_STANDBY_NAMES) == [("",)]
        assert beat(connect, deadline=timedelta(seconds=2)).ok


@pytest.mark.database
@pytest.mark.usefixtures("dsn_is_set")
def test_real_database_fixture_leaves_any_other_standby_setting_alone(
    request: pytest.FixtureRequest,
) -> None:
    other = "someone_elses_standby"

    with standby_setting_in_force(other):
        with pytest.raises(pytest.fail.Exception, match=f"{other}.*left unchanged"):
            request.getfixturevalue("database")

        assert admin_rows(SHOW_STANDBY_NAMES) == [(other,)]


@pytest.mark.database
@pytest.mark.usefixtures("dsn_is_set")
def test_real_alter_system_is_refused_outside_the_test_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with standby_setting_in_force(STANDBY_SENTINEL), monkeypatch.context() as outside:
        outside.setenv(DSN_VARIABLE, make_conninfo(require_local_dsn(), dbname="postgres"))

        with pytest.raises(pytest.fail.Exception, match="refused ALTER SYSTEM"):
            clear_standby_setting_left_by_a_killed_run()
        with pytest.raises(pytest.fail.Exception, match="refused ALTER SYSTEM"):
            set_synchronous_standby_names("")

        assert admin_rows(SHOW_STANDBY_NAMES) == [(STANDBY_SENTINEL,)]


@pytest.mark.database
def test_real_role_cannot_update_heartbeats(database: Callable[..., Connection]) -> None:
    with closing(database()) as conn, pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute("UPDATE aurora_mirrorcheck.heartbeat SET written_at = now()")
