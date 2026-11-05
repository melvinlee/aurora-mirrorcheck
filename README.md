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

Tests marked `database` are excluded from the default run.

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
