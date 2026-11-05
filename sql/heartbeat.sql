-- Heartbeat table on the primary writer (FR-01). Replicated to the secondary by
-- Aurora, so it is created once, on the primary.
--
-- Provision this with an admin role. aurora-mirrorcheck never runs it, because
-- its own role has no DDL rights (NFR-01).
--
-- The tool's role needs SELECT, INSERT and DELETE on this table and USAGE on the
-- schema. SELECT comes with its read-only role, and PostgreSQL requires it for
-- the RETURNING clause and for the WHERE clause of the purge.

CREATE SCHEMA IF NOT EXISTS aurora_mirrorcheck;

CREATE TABLE IF NOT EXISTS aurora_mirrorcheck.heartbeat (
    nonce      uuid PRIMARY KEY,
    written_at timestamptz NOT NULL
);

-- Keeps the purge from scanning the table on the writer every 30 s (NFR-05).
CREATE INDEX IF NOT EXISTS heartbeat_written_at_idx
    ON aurora_mirrorcheck.heartbeat (written_at);
