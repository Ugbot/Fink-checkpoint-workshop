# Scenario 10 — Event Time, Watermarks & Late Data (SQL-only)

This scenario is taught **entirely in Flink SQL**, using the SQL Gateway from scenario 06.
There is no Java module. Each `.sql` file below is a self-contained teaching step;
run them in order against the gateway via `scripts/run-scenario-10-sql.sh` or the
interactive SQL Client.

The dataset is the same `topic.in` trade stream as scenarios 01–05 — but instead of
relying on processing time, we declare `trade_time` as the event-time attribute and
study what happens when records arrive out of order or arbitrarily late.

## SQL teaching path

| # | File | Teaches |
|---|------|---------|
| 1 | `01-source-proctime.sql`         | Bare DDL using `PROCTIME()` — the simplest baseline. |
| 2 | `02-source-event-time.sql`       | DDL with `WATERMARK FOR ts AS ts - INTERVAL '5' SECOND` |
| 3 | `03-tumbling-proctime.sql`       | 30s tumbling count per ticker, processing time. |
| 4 | `04-tumbling-event-time.sql`     | Same window, event time — emits only after watermark passes window end. |
| 5 | `05-idle-source-timeout.sql`     | `'scan.watermark.idle-timeout' = '30s'` — keeps watermarks moving when a partition is idle. |
| 6 | `06-allowed-lateness.sql`        | `table.exec.window-allowed-lateness` — retract+revise output for late events. |
| 7 | `07-late-data-side-channel.sql`  | Detect records arriving after the watermark and route them to a dedicated sink. |
| 8 | `08-watermark-alignment.sql`     | `'scan.watermark.alignment.group'` — bound the gap between fast and slow partitions. |

Each step shows both the SQL change and (in comments) what to observe in the
output. The teaching points are deliberately incremental: each file changes one
thing relative to the previous.

## Running it

```bash
# Prereqs: scenario 06 infra is up (the SQL Gateway runs on :18083).
./quickstart.sh 10               # submits the datagen job + tells you where to go next
bash scripts/run-scenario-10-sql.sh
```

The script reads each `.sql` file in turn, POSTs it to the SQL Gateway, and prints
the result. For interactive exploration use:

```bash
bash scripts/sql-client.sh   # scenario-06 SQL Client; paste any of the .sql files in
```

## Deploy via the Ververica Cloud test harness

The scenario also ships as **first-class VVC SQL deployments**. Each teaching step
has a deployable counterpart under [`sql-deployments/`](./sql-deployments/) and a
matching YAML in the harness at
`/Users/bengamble/ververica-test-harness/pipelines/`:

| Step | Harness pipeline                                                  |
|------|-------------------------------------------------------------------|
| 01   | `workshop-scenario-10-01-source-proctime-deploy.yaml`             |
| 02   | `workshop-scenario-10-02-source-event-time-deploy.yaml`           |
| 03   | `workshop-scenario-10-03-tumbling-proctime-deploy.yaml`           |
| 04   | `workshop-scenario-10-04-tumbling-event-time-deploy.yaml`         |
| 05   | `workshop-scenario-10-05-idle-source-timeout-deploy.yaml`         |
| 06   | `workshop-scenario-10-06-allowed-lateness-deploy.yaml`            |
| 07   | `workshop-scenario-10-07-late-data-side-channel-deploy.yaml`      |
| 08   | `workshop-scenario-10-08-watermark-alignment-deploy.yaml`         |
| all  | `workshop-scenario-10-deploy.yaml`                                |

Each pipeline runs `sql.validate` → `deployment.create` (with
`artifact.kind: SQLSCRIPT` and `sqlArtifact.sqlScriptFile` pointing at the
matching `.sql` file on disk) → `deployment.start` → `wait.jobRunning` →
assert. The all-in-one YAML chains all 8 steps in sequence.

The harness's `deployment.create` action loads `sqlScriptFile` at execute time
and substitutes it into `sqlArtifact.sqlScript` before POSTing to VVC, so the
SQL files live in this repo (unduplicated) while the YAMLs live alongside the
other harness pipelines. Override `sqlScriptFile` at run-time to point at a
different file. The classpath builtin `sql-deployment-cicd` documents the
canonical pattern.

---

## Why SQL-only

Watermarks and late-data handling are *exactly* the place where Flink SQL pulls
its weight: the SQL planner generates the right state machine for window
emission, retraction, idle handling, and late-data routing from a few declarative
properties. Showing the same thing in DataStream requires writing a custom
`AssignerWithPunctuatedWatermarks` + side-output plumbing — instructive once but
not how production teams actually build pipelines today.
