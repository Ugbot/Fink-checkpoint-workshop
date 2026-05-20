# Deployable variants — Scenario 11

The teaching files in [`../sql/`](../sql/) are written for the SQL Client, where
DDL, `INSERT VALUES`, point-lookups and DESCRIBE all make sense as separate
short commands. The Ververica Cloud test harness wants a *streaming job*, so
deployable variants live here.

For scenario 11, only step 05 (batch aggregation) is a complete streaming
job by itself. The rest of the teaching steps don't fit the deployment
shape (they're DDL, point lookups, or comparison narrative).

| Step | Why no deployable variant |
|------|---------------------------|
| 01 — what-is-fluss          | SHOW CATALOGS — interactive only |
| 02 — database and listing   | DESCRIBE — interactive only |
| 03 — log-table              | one-shot INSERT VALUES + reads |
| 04 — PK table               | one-shot INSERT VALUES + point lookups |
| 05 — batch aggregation      | ✓ shipped as `05-batch-aggregation.sql` |
| 06 — kafka-compacted vs fluss | narrative, not deployable |
| 07 — cleanup                | DDL, interactive only |
