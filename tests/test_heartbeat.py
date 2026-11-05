import os
import threading
from collections.abc import Callable, Iterator
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Self, cast
from uuid import uuid4

import psycopg
import pytest

from aurora_mirrorcheck.heartbeat import (
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
        self._conn.log.append("ROLLBACK" if exc_type else "COMMIT")


class FakeConnection:
    """Records statements in order, and fails any statement containing `fail_on`."""

    def __init__(
        self,
        fail_on: str | None = None,
        error: Exception | None = None,
        insert_row: tuple[Any, ...] | None = (WRITTEN_AT,),
        purged: int = 0,
    ) -> None:
        self.log: list[str] = []
        self.params: list[tuple[Any, ...]] = []
        self.closed = False
        self._fail_on = fail_on
        self._error = error or psycopg.OperationalError("boom")
        self._insert_row = insert_row
        self._purged = purged

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


def connector(conn: FakeConnection) -> Callable[[], Connection]:
    return lambda: cast(Connection, conn)


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


@pytest.mark.parametrize(
    ("timeout", "expected"),
    [(DEFAULT_STATEMENT_TIMEOUT, "1800000"), (timedelta(seconds=5), "5000")],
)
def test_statement_timeout_applies_to_every_transaction(timeout: timedelta, expected: str) -> None:
    conn = FakeConnection()

    beat(connector(conn), statement_timeout=timeout)

    assert conn.params[0] == (expected,)
    assert conn.params[2] == (expected,)


@pytest.mark.parametrize("timeout", [timedelta(0), timedelta(microseconds=1), timedelta(-1)])
def test_a_timeout_that_would_disable_the_limit_is_rejected(timeout: timedelta) -> None:
    conn = FakeConnection()

    with pytest.raises(ValueError, match="statement_timeout"):
        beat(connector(conn), statement_timeout=timeout)

    assert conn.log == []


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


def test_loop_does_not_beat_once_stopped() -> None:
    conn = FakeConnection()
    stop = threading.Event()
    stop.set()

    run_heartbeat_loop(connector(conn), lambda _: None, stop)

    assert conn.log == []


@pytest.fixture
def database() -> Iterator[Callable[..., Connection]]:
    """Connect as a role with only the grants FR-01 needs, on a disposable local server."""
    dsn = os.environ.get("AURORA_MIRRORCHECK_TEST_DSN")
    if dsn is None:
        pytest.skip("AURORA_MIRRORCHECK_TEST_DSN is not set")

    role = "mirrorcheck_test"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute("DROP SCHEMA IF EXISTS aurora_mirrorcheck CASCADE")
        admin.execute(f"DROP ROLE IF EXISTS {role}")
        admin.execute(f"CREATE ROLE {role} LOGIN")
        admin.execute(SCHEMA_SQL.read_text().encode())
        admin.execute(f"GRANT USAGE ON SCHEMA aurora_mirrorcheck TO {role}")
        admin.execute(f"GRANT SELECT, INSERT, DELETE ON aurora_mirrorcheck.heartbeat TO {role}")

    def connect(**kwargs: Any) -> Connection:
        conn = psycopg.connect(dsn, **kwargs)
        conn.execute(f"SET SESSION AUTHORIZATION {role}")
        conn.commit()
        return conn

    yield connect

    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute("DROP SCHEMA aurora_mirrorcheck CASCADE")
        admin.execute(f"DROP ROLE {role}")


def admin_rows(sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    dsn = os.environ["AURORA_MIRRORCHECK_TEST_DSN"]
    with psycopg.connect(dsn, autocommit=True) as admin:
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
def test_real_statement_timeout_cancels_a_blocked_write(
    database: Callable[..., Connection],
) -> None:
    dsn = os.environ["AURORA_MIRRORCHECK_TEST_DSN"]
    with psycopg.connect(dsn) as locker:
        locker.execute("LOCK TABLE aurora_mirrorcheck.heartbeat")

        result = beat(database, statement_timeout=timedelta(milliseconds=200))

    assert result.failure is BeatFailure.WRITE
    assert result.detail == "QueryCanceled (57014)"


@pytest.mark.database
def test_real_role_cannot_update_heartbeats(database: Callable[..., Connection]) -> None:
    with closing(database()) as conn, pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute("UPDATE aurora_mirrorcheck.heartbeat SET written_at = now()")
