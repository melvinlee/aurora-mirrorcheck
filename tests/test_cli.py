import pytest

from aurora_mirrorcheck.cli import EXIT_ERROR, main


def test_help_exits_zero() -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["--help"])
    assert excinfo.value.code == 0


def test_missing_command_is_an_error() -> None:
    with pytest.raises(SystemExit) as excinfo:
        main([])
    assert excinfo.value.code == EXIT_ERROR


@pytest.mark.parametrize(
    "argv",
    [
        ["gate"],
        ["gate", "--allow-warn"],
        ["gate", "--no-heartbeat"],
        ["gate", "--dr-test-id", "DR-1", "--operator", "someone"],
        ["attest"],
        ["attest", "--evidence", "abc", "--role", "risk", "--decision", "approve"],
    ],
)
def test_stub_commands_exit_error(argv: list[str], capsys: pytest.CaptureFixture[str]) -> None:
    assert main(argv) == EXIT_ERROR
    assert "not implemented" in capsys.readouterr().err
