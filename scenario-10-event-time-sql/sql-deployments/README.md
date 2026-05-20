# Deployable variants of the scenario-10 SQL teaching path

Each `.sql` file in [`../sql/`](../sql/) is written for the **SQL Client / SQL Gateway**
teaching path — many end with `SHOW CREATE TABLE`, `DESCRIBE`, or a bare `SELECT`,
which is fine when a human is pasting them in step by step but is not directly
deployable as a persistent VVC streaming job.

The files in this directory are the **deployable counterparts**: each one wraps the
matching teaching step with a complete `CREATE TABLE source; CREATE TABLE sink;
INSERT INTO sink SELECT ...;` chain. Most sink into Flink's built-in `'connector' =
'blackhole'` so they run anywhere without external infrastructure.

| Step | Teaching file (`../sql/`)             | Deployable variant (this dir)            |
|------|---------------------------------------|------------------------------------------|
| 01   | `01-source-proctime.sql`              | `01-source-proctime.sql`                 |
| 02   | `02-source-event-time.sql`            | `02-source-event-time.sql`               |
| 03   | `03-tumbling-proctime.sql`            | `03-tumbling-proctime.sql`               |
| 04   | `04-tumbling-event-time.sql`          | `04-tumbling-event-time.sql`             |
| 05   | `05-idle-source-timeout.sql`          | `05-idle-source-timeout.sql`             |
| 06   | `06-allowed-lateness.sql`             | `06-allowed-lateness.sql`                |
| 07   | `07-late-data-side-channel.sql`       | `07-late-data-side-channel.sql`          |
| 08   | `08-watermark-alignment.sql`          | `08-watermark-alignment.sql`             |

Used by the Ververica Cloud test harness pipelines at
`/Users/bengamble/ververica-test-harness/pipelines/workshop-scenario-10-*-deploy.yaml`,
which reference these files via `spec.artifact.sqlArtifact.sqlScriptFile`.
