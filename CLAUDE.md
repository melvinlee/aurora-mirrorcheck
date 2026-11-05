# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

This is a Python project managed with uv. Only the package skeleton exists so far: `gate` and `attest` are stubs that exit 2 (ERROR). The source of truth is `PRD/aurora-mirrorcheck-prd.md`.

Requirement IDs (`FR-xx`, `NFR-xx`) come from the PRD; cite them when implementing. Check the PRD's open questions before hardcoding a value: the RPO budget, `rds.global_db_rpo` setting, approver roles, retention period, L4 table list and off-peak window are all undecided. Whether L3 is built in-house or uses pgCompare is also open, pending a one-week spike.

## Commands

```bash
uv sync                                                # Install dependencies
uv run ruff check .                                    # Lint
uv run ruff format --check .                           # Check formatting
uv run mypy src                                        # Type check
uv run pytest                                          # Run tests
uv run pytest tests/test_cli.py::test_help_exits_zero  # Run a single test
uv run aurora-mirrorcheck --help                       # Run the CLI
```

Tests marked `database` are excluded from the default run.

If the repo is in an iCloud-synced folder such as `~/Documents`, iCloud can hide files in `.venv`. Python then skips the hidden editable-install `.pth` file and fails with `ModuleNotFoundError: No module named 'aurora_mirrorcheck'`. Set `UV_PROJECT_ENVIRONMENT` to a path outside iCloud to avoid this.

## What the tool is

`aurora-mirrorcheck` proves that the secondary-region Aurora PostgreSQL cluster (Aurora Global Database) matches the primary within the RPO, and produces DR test sign-off evidence. It validates only: it never fails over, promotes, steers traffic or repairs data, and it covers Aurora PostgreSQL only.

## Planned architecture

It runs as EKS CronJobs with IRSA in the secondary region. It observes both regions and is never in the data path. Metrics go to VictoriaMetrics, and evidence goes to S3 with Object Lock (compliance mode). Config is one YAML file in Git, deployed via GitOps.

Checks are layered from cheap and continuous to heavy and on demand:

| Layer | Cadence | Purpose |
| --- | --- | --- |
| L0 Heartbeat | 30 s | Write a row on the primary writer, poll the secondary, record visible lag |
| L1 Replication health | 1 min | `aurora_global_db_status()` lag values, `rds.global_db_rpo` check |
| L2 Structural/config parity | Hourly | Schema, role, extension, grant, sequence and parameter group drift |
| L3 Data checksum | Nightly, on demand | Bucketed row counts and hashes, drilled down to primary keys |
| L4 Business reconciliation | 15 min | Configured aggregates compared by key columns |
| L5 Gate | On demand | `gate` runs L0 to L4; `attest` records approvals against the evidence digest |

`gate` exits 0 PASS, 1 FAIL, 2 ERROR. WARN counts as FAIL unless `--allow-warn` is passed.

## Invariants

These cut across layers and are easy to break:

- **Writes:** The only write anywhere is INSERT/DELETE on `aurora_mirrorcheck.heartbeat` on the primary writer. Everything else on the primary uses the in-region reader, and every query has a statement timeout.
- **No false FAILs from lag:** L3 fixes one cutoff per run (primary `now()` minus a settle interval, default 5 min). It queries the secondary only after that side shows a heartbeat written after the primary query, and rechecks mismatched buckets up to 3 times before reporting FAIL. L4 uses the same cutoff on both regions.
- **Evidence contents:** Evidence holds counts, hashes and primary keys, never row contents.
- **Evidence integrity:** Evidence is canonical JSON with a SHA-256 digest. The report and attestations reference that digest, and every run records the tool version and config digest.
- **Failure reporting:** An ERROR never reports as PASS. A crashed run leaves no state behind. A heartbeat that could not be written is reported separately from a lag breach.
- **Connections:** Use `sslmode=verify-full` to both regions. Credentials come from Secrets Manager or IAM DB auth via IRSA, never from config, images or logs.

## Git workflow

Always pull the latest `main` before branching, and always branch from `main`:

```bash
git switch main
git pull --ff-only origin main
git switch -c <branch>
```

Never add a `Co-Authored-By` line to a commit.
