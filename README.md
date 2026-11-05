# aurora-mirrorcheck

Proves that the secondary-region Aurora PostgreSQL cluster (Aurora Global Database) matches the primary within the RPO, and produces DR test sign-off evidence.

It validates only. It never fails over, promotes, steers traffic or repairs data, and it covers Aurora PostgreSQL only.

**Status:** early development. Only the package skeleton exists, and `gate` and `attest` are stubs that exit 2 (ERROR). The requirements are in [`PRD/aurora-mirrorcheck-prd.md`](PRD/aurora-mirrorcheck-prd.md).

## Getting started

You need [uv](https://docs.astral.sh/uv/). It installs the Python version in `.python-version` (3.14) if you don't have it.

```bash
git clone https://github.com/melvinlee/aurora-mirrorcheck.git
cd aurora-mirrorcheck
uv sync
uv run aurora-mirrorcheck --help
```

`uv sync` creates `.venv` and installs the locked dependencies from `uv.lock`, including the dev tools. Use `uv run <command>` to run anything inside that environment, so you don't need to activate it.

## Development

Run these before opening a pull request:

```bash
uv run ruff check .           # Lint
uv run ruff format --check .  # Check formatting
uv run mypy src               # Type check
uv run pytest                 # Run tests
```

Tests marked `database` are excluded from the default run. They need a disposable PostgreSQL server, which the `pg-podman` agent can start. Point `AURORA_MIRRORCHECK_TEST_DSN` at it and select them with `-m database`:

```bash
AURORA_MIRRORCHECK_TEST_DSN='postgresql://postgres:mirrorcheck_test@127.0.0.1:55433/mirrorcheck_test' uv run pytest -m database
```

These tests drop and recreate the `aurora_mirrorcheck` schema and the server-wide `mirrorcheck_test` role, and one test briefly sets `synchronous_standby_names` with `ALTER SYSTEM`, so never point them at a server you care about. A test run that is killed during that test leaves the setting in place, and every commit on the server then blocks until you run `ALTER SYSTEM RESET synchronous_standby_names` and reload. The test run fails unless the DSN's host is exactly `localhost`, `127.0.0.1` or `::1`, and it also refuses `hostaddr`, `service`, `PGHOSTADDR` and `PGSERVICE`. If `AURORA_MIRRORCHECK_TEST_DSN` isn't set, the `database` tests skip. The check can't tell what is listening on the port, so don't use it with a tunnel or port-forward to a real database.

To add a dependency, use `uv add <package>`, or `uv add --dev <package>` for a dev tool. Commit the changes to both `pyproject.toml` and `uv.lock`.

Always branch from the latest `main`:

```bash
git switch main
git pull --ff-only origin main
git switch -c <branch>
```

## Troubleshooting

- **`Failed to spawn: pytest` or `mypy`:** the repo probably moved after `.venv` was created, so its scripts point at the old path. Run `rm -rf .venv && uv sync`.
- **`ModuleNotFoundError: No module named 'aurora_mirrorcheck'`:** iCloud can hide files in `.venv` if the repo is in a synced folder such as `~/Documents`. Move the repo out of that folder, or set `UV_PROJECT_ENVIRONMENT` to a path outside iCloud.
