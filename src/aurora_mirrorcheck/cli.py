"""Command line entry point for aurora-mirrorcheck.

`gate` and `attest` are stubs. Unimplemented commands exit 2 (ERROR) so that a
stub can never report PASS (FR-21, NFR-09).
"""

import argparse
import sys
from collections.abc import Sequence

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_ERROR = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aurora-mirrorcheck",
        description="Validate that the secondary Aurora PostgreSQL cluster matches the primary.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    gate = commands.add_parser(
        "gate",
        help="run checks L0 to L4; exits 0 PASS, 1 FAIL, 2 ERROR",
    )
    gate.add_argument("--dr-test-id", help="DR test ID to record in the evidence")
    gate.add_argument("--operator", help="operator name to record in the evidence")
    gate.add_argument("--allow-warn", action="store_true", help="do not count WARN as FAIL")
    gate.add_argument(
        "--no-heartbeat",
        action="store_true",
        help="skip the heartbeat write during a real failover, and record that it was skipped",
    )

    attest = commands.add_parser(
        "attest",
        help="record an approver's decision against an evidence digest",
    )
    attest.add_argument("--evidence", help="SHA-256 digest of the evidence pack")
    attest.add_argument("--role", help="approver role")
    attest.add_argument("--decision", choices=["approve", "reject"], help="approval decision")
    attest.add_argument("--comment", help="free-text comment")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    print(f"ERROR: {args.command} is not implemented", file=sys.stderr)
    return EXIT_ERROR
