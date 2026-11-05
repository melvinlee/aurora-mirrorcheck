"""A test DSN must stay out of pytest's output in every traceback mode (NFR-02).

These run the real `database` tests in a child pytest, because what matters is what
pytest renders for a real refusal. Every value here is fake, and nothing connects.
"""

from pathlib import Path

import pytest

pytest_plugins = ("pytester",)

HEARTBEAT_TESTS = Path(__file__).with_name("test_heartbeat.py")
FAKE_PASSWORD = "fake-password-not-a-secret"  # noqa: S105
FAKE_HOST = "db.invalid"


OPTIONS = pytest.mark.parametrize(
    "options",
    [[], ["--showlocals"], ["--full-trace"], ["--showlocals", "--full-trace"]],
    ids=["plain", "showlocals", "full-trace", "both"],
)


def run_database_tests(pytester: pytest.Pytester, options: list[str]) -> tuple[str, dict[str, int]]:
    result = pytester.runpytest_subprocess(
        str(HEARTBEAT_TESTS), "-m", "database", "-p", "no:cacheprovider", *options
    )
    return result.stdout.str() + result.stderr.str(), result.parseoutcomes()


@pytest.mark.parametrize(
    "dsn",
    [
        f"postgresql://postgres:{FAKE_PASSWORD}@{FAKE_HOST}/prod",
        f"host={FAKE_HOST} password={FAKE_PASSWORD} dbname=prod",
        f"host=localhost hostaddr=192.0.2.1 password={FAKE_PASSWORD}",
        f"host={FAKE_HOST} password={FAKE_PASSWORD} this-does-not-parse",
    ],
    ids=["url", "keywords", "override", "unparsable"],
)
@OPTIONS
def test_a_refusal_prints_no_password_in_any_traceback_mode(
    dsn: str, options: list[str], pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AURORA_MIRRORCHECK_TEST_DSN", dsn)

    output, outcomes = run_database_tests(pytester, options)

    assert outcomes.get("errors", 0) > 0
    assert "passed" not in outcomes
    assert "skipped" not in outcomes
    assert "refused" in output
    assert FAKE_PASSWORD not in output


@OPTIONS
def test_a_failed_connection_prints_no_password_in_any_traceback_mode(
    options: list[str], pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Loopback, so the guard accepts it, on a port where nothing listens.
    dsn = f"postgresql://postgres:{FAKE_PASSWORD}@127.0.0.1:1/prod"
    monkeypatch.setenv("AURORA_MIRRORCHECK_TEST_DSN", dsn)
    for name in ("PGHOSTADDR", "PGSERVICE"):
        monkeypatch.delenv(name, raising=False)

    output, outcomes = run_database_tests(pytester, options)

    assert outcomes.get("errors", 0) > 0
    assert "passed" not in outcomes
    assert "could not connect to the test server" in output
    assert FAKE_PASSWORD not in output
