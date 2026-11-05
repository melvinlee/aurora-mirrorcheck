# Cross-Region Data Validation PRD

## Overview and problem

Aurora Mirrorcheck (`aurora-mirrorcheck`) is an internal tool that proves the secondary-region Aurora PostgreSQL cluster holds the same data as the primary region, within the agreed RPO, and produces evidence a DR test can be signed off against.

The payments platform is moving from single-region to warm standby. Aurora Global Database replicates storage from the primary region to the secondary region. Today nothing independently shows that the replicated data is complete, current and usable after promotion.

Three gaps drive this work:

- **Lag is visible only as AWS metrics.** No check shows that a committed row is actually readable in the secondary region, or how long that takes.
- **No point-in-time integrity proof.** Storage-level replication should be byte-identical, but there is no evidence of it that an auditor can inspect.
- **Promotion readiness is unchecked.** Parameter groups and DB-level settings are not replicated by Global Database and can drift silently between regions.

MAS TRM expects DR capability to be tested and evidenced. A DR test today has no artefact proving data parity, so it cannot be signed off on data integrity.

## Goals and non-goals

**Goals**

1. Measure end-to-end visible replication lag continuously and alert when it breaches the RPO budget.
2. Prove row-level data parity between regions on a schedule and on demand, with zero false alarms caused by lag.
3. Detect schema, role, extension and parameter drift that would make the secondary behave differently after promotion.
4. Reconcile business totals on payment and ledger tables in terms risk and audit teams understand.
5. Produce a tamper-evident evidence pack and a DR test sign-off record for every gate run.
6. Act as a go/no-go step in the DR runbook before the Route 53 weight flip.

**Non-goals**

- Performing failover, promotion or traffic steering. The tool validates; the runbook acts.
- Repairing data. Mismatches are reported with primary keys only; remediation is a separate, manual decision.
- Validating Kafka/MSK, Kong or application state. This PRD covers Aurora PostgreSQL only.
- Replacing AWS-native monitoring. CloudWatch metrics stay; the tool adds independent checks on top.
- Copying payment data out of either database. Evidence holds counts, hashes and keys, never row contents.

## Users and use cases

| User | Use case | How they use it |
| --- | --- | --- |
| Platform engineers (on-call) | Know replication is healthy right now | Grafana dashboard and alerts on lag, heartbeat and parity |
| Platform engineers (DR lead) | Decide go/no-go during a DR test or real failover | Run the gate on demand; read PASS/FAIL and the report |
| DBA / data owners | Investigate a reported mismatch | Drill-down output: table, bucket, primary keys missing, extra or changed |
| Technology risk and audit | Confirm the DR test met data integrity requirements | Evidence pack in S3 with hash, HTML report, sign-off record |
| Approvers (platform lead, service owner, risk) | Sign off a DR test | `attest` command records name, role, decision and the evidence hash |

The gate run is the primary use case. Continuous and scheduled checks exist so the gate result is never the first sign of a problem.

## Context and assumptions

The tool runs on an EKS cluster in the secondary region, reads from a primary-region reader and the secondary read-only instance, and writes only a heartbeat row on the primary writer.

&#91;embedded content: architecture · 2 regions, 1 validator\]

Replication happens in Aurora's storage layer; the validator only observes both ends and never sits in the data path.

| Item | Value |
| --- | --- |
| Replication | Aurora Global Database, native storage-level replication |
| Primary region | Writer + in-region reader |
| Secondary region | Read-only instance, always running |
| Database size | About 20 GB |
| Table shape | Payment tables have primary keys and reliable created\_at / updated\_at columns |
| Target posture | Warm standby, RTO under 4 hours, near-zero RPO for payment data |
| Traffic steering | Manual Route 53 weight flip |
| Runtime | EKS CronJobs with IRSA; metrics to VictoriaMetrics, dashboards in Grafana |
| Sign-off need | Formal sign-off of each DR test |

**Assumptions**

- Storage replication is physical, so data drift should be impossible. The tool's job is to prove that, measure lag and catch what is not replicated.
- Both clusters run the same engine version, so hash functions and text casts give identical output in both regions.
- The secondary instance may serve read traffic. Heavy checks run off-peak or against a custom endpoint.
- DB credentials live in Secrets Manager, replicated to the secondary region.
- 20 GB is small enough for a full-table checksum nightly, so incremental checksumming is not needed in v1.

## Functional requirements

Six layers, from cheap and continuous to heavy and on demand. A gate run executes all of them.

### L0 Heartbeat (every 30 s)

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-01 | Write a heartbeat row (nonce, written\_at) to `aurora_mirrorcheck.heartbeat` on the primary writer every 30 s; purge rows older than 7 days | Must |
| FR-02 | Poll the secondary reader until the row is visible and record visible lag; a timeout at the RPO budget is a FAIL | Must |
| FR-03 | Report a heartbeat that could not be written separately from a lag breach | Must |

### L1 Replication health (every 1 min)

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-04 | Read `durability_lag_in_msec` and `rpo_lag_in_msec` for the secondary region from `aurora_global_db_status()` and alert on thresholds | Must |
| FR-05 | Alert when `last_lag_calculation_time` is older than 5 min | Must |
| FR-06 | Read visible lag per instance from `aurora_global_db_instance_status()` | Should |
| FR-07 | Verify `rds.global_db_rpo` matches the expected value, or is unset by recorded decision | Must |

### L2 Structural and config parity (hourly)

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-08 | Fingerprint and compare tables and columns, indexes, constraints, functions, triggers, extensions with versions, roles with attributes, and grants; list missing, extra and different objects | Must |
| FR-09 | Check every sequence's last\_value in the secondary is no higher than in the primary | Must |
| FR-10 | Diff cluster and instance parameter groups and cluster attributes (engine version, IAM auth, backup retention, log exports, deletion protection) across regions, with an allowlist for intended differences | Must |
| FR-11 | Confirm the secondary cluster is attached to the global cluster and available | Must |

### L3 Data checksum (nightly full, on demand)

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-12 | Discover tables in configured schemas; support per-table overrides (excluded columns, watermark column, bucket size) and an exclude list | Must |
| FR-13 | Fix one cutoff per run (primary `now()` minus a settle interval, default 5 min) and compare only rows whose watermark is before it | Must |
| FR-14 | Bucket each table by primary key range (integer keys) or key hash (other keys); compute row count and sum of row hashes per bucket | Must |
| FR-15 | Query the primary on its in-region reader, never the writer; query the secondary only after it shows a heartbeat written after the primary query | Must |
| FR-16 | Recheck mismatched buckets up to 3 times with a delay; only mismatches that persist count as FAIL | Must |
| FR-17 | Drill persistent mismatches down to primary keys: missing in the secondary, extra, changed; cap at a configured row count | Must |
| FR-18 | Compare tables without a primary key as one bucket and flag them as reduced coverage | Should |

### L4 Business reconciliation (every 15 min)

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-19 | Run configured aggregate queries (count and sum by day, currency, status) on both regions with the same cutoff and compare exactly by key columns | Must |
| FR-20 | Report each differing key with both values and the delta | Must |

### L5 DR test gate (on demand)

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-21 | `gate` command runs L0 to L4 and exits 0 PASS, 1 FAIL, 2 ERROR; WARN counts as FAIL unless `--allow-warn` is passed | Must |
| FR-22 | Accept a DR test ID and operator name and record both in the evidence | Must |
| FR-23 | `attest` command records an approver's name, role, decision and comment against the evidence hash | Must |

## Non-functional requirements

| ID | Area | Requirement |
| --- | --- | --- |
| NFR-01 | Security | Read-only DB role in both regions; the only write grant is INSERT/DELETE on `aurora_mirrorcheck.heartbeat` on the primary writer |
| NFR-02 | Security | Credentials from Secrets Manager or IAM DB auth via IRSA; no secrets in config, images or logs |
| NFR-03 | Security | TLS (`sslmode=verify-full`) to both regions |
| NFR-04 | Data handling | Evidence holds counts, hashes and primary keys only, never row contents |
| NFR-05 | Load | No query on the primary writer except the heartbeat; statement timeout on every query (default 30 min). The heartbeat is the exception: 10 s statement timeout per statement and 5 s lock timeout, both configurable |
| NFR-06 | Load | Full checksum runs off-peak; concurrency configurable, default 2 tables at a time |
| NFR-07 | Performance | A full gate run on 20 GB finishes within 30 min |
| NFR-08 | Correctness | Zero false FAILs caused by replication lag; every FAIL is reproducible from the evidence |
| NFR-09 | Reliability | A crashed run leaves no state behind and the next run starts clean; an ERROR never reports as PASS |
| NFR-10 | Auditability | Evidence stored in S3 with Object Lock (compliance mode); retention set by risk, 7 years assumed |
| NFR-11 | Auditability | Evidence is canonical JSON with a SHA-256 digest; the report and attestations reference that digest |
| NFR-12 | Operability | Config is one YAML file in Git, deployed via GitOps; tool version and config digest recorded in every run |

## Outputs and DR sign-off

Every run emits metrics; every scheduled full run and every gate run also writes an evidence pack. Gate runs add a sign-off record.

**Metrics** (pushed to VictoriaMetrics)

| Metric | Meaning |
| --- | --- |
| `aurora_mirrorcheck_heartbeat_visible_lag_seconds` | Time from primary commit to visibility in the secondary |
| `aurora_mirrorcheck_rpo_lag_ms`, `aurora_mirrorcheck_durability_lag_ms` | Values from `aurora_global_db_status()` |
| `aurora_mirrorcheck_check_status{layer,check}` | 1 PASS, 0.5 WARN, 0 FAIL |
| `aurora_mirrorcheck_mismatched_buckets{table}` | Persistent mismatches after rechecks |
| `aurora_mirrorcheck_rows_compared_total{table}` | Coverage of the checksum |
| `aurora_mirrorcheck_last_success_timestamp{profile}` | Detects runs that stopped running |

**Alerts**

| Alert | Condition | Severity |
| --- | --- | --- |
| Heartbeat lag breach | Visible lag above RPO budget for 2 consecutive checks | Page |
| Heartbeat missing | No heartbeat visible for 3 min | Page |
| Data mismatch | Any persistent checksum or reconciliation mismatch | Page |
| Schema or config drift | Any L2 FAIL | Ticket |
| Stale run | No successful nightly run for 26 h | Ticket |

**Evidence pack** (per run, in S3 with Object Lock)

- `evidence.json`: run ID, profile, DR test ID, operator, tool version, config digest, cutoff, cluster identifiers, every check with status and figures, verdict.
- `evidence.sha256`: digest of the canonical JSON.
- `report.html`: human-readable summary for reviewers, showing the digest and a sign-off block.

**Sign-off flow for a DR test**

1. DR lead runs `aurora-mirrorcheck gate --dr-test-id <id> --operator <name>`.
2. Tool writes the evidence pack and prints the verdict and digest.
3. Each approver (platform lead, service owner, technology risk) reviews the report and runs `aurora-mirrorcheck attest --evidence <digest> --role <role> --decision approve|reject --comment <text>`.
4. Each attestation is stored beside the evidence under Object Lock, bound to the same digest.
5. The DR test counts as signed off on data integrity when the verdict is PASS and every required role has approved.

## Success metrics and acceptance criteria

The tool is done when it catches injected faults reliably, stays silent on a healthy system and produces sign-off evidence the risk team accepts.

**Acceptance tests** (run in a non-production global database)

- [ ] With a restored standalone copy as the secondary, deleting one row makes L3 FAIL and names that primary key
- [ ] Changing one column value makes L3 FAIL and reports the key as changed
- [ ] Adding an index on one side only makes L2 FAIL and names the index
- [ ] Changing a parameter in one region's parameter group makes L2 FAIL and names the parameter
- [ ] Running L3 under sustained write load for 7 nights gives zero false FAILs
- [ ] Stopping replication (test environment) breaches heartbeat lag and pages within 2 min
- [ ] A full gate run on 20 GB finishes within 30 min
- [ ] Writer CPU shows no measurable change during a full run
- [ ] Technology risk accepts a sample evidence pack and attestation as DR test evidence

**Ongoing success metrics**

| Metric | Target |
| --- | --- |
| False FAILs per month | 0 |
| Nightly full-run success rate | 99% or higher |
| Time from data mismatch to page | Under 24 h for checksum, under 15 min for reconciliation |
| DR tests signed off with tool evidence | 100% from first drill after go-live |

## Risks and mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Lag or in-flight updates cause false mismatches | Alert fatigue; real issues ignored | Fixed cutoff, heartbeat catch-up before each secondary query, up to 3 rechecks before FAIL |
| Checksum load slows read traffic on the secondary instance | Degraded reads in the secondary region | Off-peak schedule, statement timeouts, low concurrency; option to add a second reader behind a custom endpoint |
| `updated_at` not maintained on some tables | Persistent false FAILs on those tables | Per-table watermark override; tables without a reliable watermark rely on rechecks |
| Enabling `rds.global_db_rpo` blocks commits on the primary writer if the secondary falls behind | Payment write outage | Decision for the DB and risk owners; tool only verifies the agreed setting |
| Real drift cannot be injected under physical replication | Detection logic untested in production | Acceptance tests use a restored standalone copy as the secondary; quarterly self-test |
| Heartbeat write fails during a primary-region outage | Lag reported as unknown, not as healthy | Missing heartbeat is its own FAIL; gate during a real failover runs with `--no-heartbeat` and records it |
| Evidence deleted or altered | Sign-off cannot be defended | S3 Object Lock in compliance mode; digest in report and attestations |
| Tool becomes stale as schemas change | Coverage gaps | Auto-discovery of tables; coverage metric per table; new tables appear without config change |

## Milestones and rollout

The proposed plan reaches a signed-off DR drill in 8 weeks; each phase ships only after its gate passes.

&#91;embedded content: rollout · 4 phases, 4 gates\]

Phases 1 to 3 run against a non-production global database. Production starts read-only checks first and enables alert paging only after the 7-night soak shows zero false FAILs.

## Open questions

- [ ] What is the RPO budget in seconds for alert thresholds ("near-zero" needs a number)?
- [ ] Will `rds.global_db_rpo` be enabled, and at what value?
- [ ] Which roles must approve a DR test: platform lead, service owner, technology risk, others?
- [ ] Evidence retention period: is 7 years right?
- [ ] Does the secondary instance serve production reads, and what is the off-peak window?
- [ ] Which tables and aggregates define business reconciliation (L4), and who owns that list?
- [ ] Where do pages route (on-call tool and rotation)?
- [ ] Build L3 in-house or adopt pgCompare (Crunchy Data)? Decide after a one-week spike.
