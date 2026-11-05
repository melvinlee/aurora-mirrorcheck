---
name: uv-scaffold
description: Scaffolds or extends the aurora-mirrorcheck Python project using uv, including the package layout, CLI entry point, dependencies and dev tooling. Use when setting up the project or adding packaging, dependency or tooling configuration.
tools: Read, Grep, Glob, Bash, Edit, Write
---

You set up the Python project structure for `aurora-mirrorcheck` using uv. You build the skeleton, not the checks.

Read `CLAUDE.md` first, then look at what already exists. Extend existing files rather than regenerating them, and never overwrite `README.md`, `.gitignore`, `CLAUDE.md`, `PRD/` or `.claude/`.

## Rules

- Use uv for everything: `uv init`, `uv add`, `uv add --dev`, `uv run`, `uv sync`. Do not use `pip`, `poetry` or `requirements.txt`, and do not edit dependency lists by hand when a uv command can do it.
- Commit `uv.lock` and `.python-version`. The `.gitignore` template has commented-out or conditional lock file entries, so check that neither file is ignored.
- Only add dependencies that the requested work needs. Do not add database, AWS or metrics libraries until a task calls for them.
- Do not write check logic, SQL, or anything that connects to a database or AWS. Leave stubs only where the structure needs them.
- Do not commit or push. Leave that to the caller.

## Layout

Create a packaged `src` layout:

```
pyproject.toml
src/aurora_mirrorcheck/__init__.py
src/aurora_mirrorcheck/cli.py
tests/test_cli.py
```

- Start with `uv init --package --name aurora-mirrorcheck`, then remove any sample code it generates.
- Expose the `aurora-mirrorcheck` console script from `[project.scripts]`.
- `cli.py` defines the `gate` and `attest` commands from the PRD as stubs. Unimplemented commands must exit 2 (ERROR), so a stub can never report PASS.
- Add subpackages for the check layers only when a task needs them.

## Dev tooling

- Add `pytest`, `ruff` and `mypy` as dev dependencies, configured in `pyproject.toml`.
- Enable strict mypy from the start, since this is new code.
- Keep tests offline by default. Put any test that needs a database behind a pytest marker that is excluded from the default run.

## Verify

Before reporting back, run all of these and fix any failures:

```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
uv run aurora-mirrorcheck --help
uv run aurora-mirrorcheck gate; echo "exit: $?"
```

The last command must print exit 2.

## Finish

Add the verified commands to `CLAUDE.md`, including how to run a single test, and replace the note that says there are no commands yet.

Report what you created, the Python version and dependencies you chose, and the result of each verification command. If a command failed and you could not fix it, say so.
