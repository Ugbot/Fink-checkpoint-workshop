# Deployable variants — Scenario 12

The teaching files in [`../sql/`](../sql/) are written for the SQL Client,
where DDL, DESCRIBE, EXPLAIN and SELECT all make sense as separate
interactive commands. The Ververica Cloud test harness wants a *streaming
job* (an INSERT INTO a sink), so deployable variants of the steps that
have a terminal SELECT live here.

| Step | Deployable | Why / why not |
|------|------------|---------------|
| 01 — instrument-master-ddl                       | no  | DESCRIBE + SHOW CREATE TABLE — inspection only, no INSERT |
| 02 — load-instrument-master                      | no  | COUNT(*) + LIMIT preview — interactive only |
| 03 — projection-pushdown                         | yes | The projected SELECT is wrapped in `INSERT INTO blackhole_sink` |
| 04 — filter-pushdown                             | no  | Two EXPLAINs + point lookups — interactive only |
| 05 — batch-agg-wide-table                        | yes | Batch aggregation → blackhole_sink |
| 06 — account-profile-and-correlated-batch-query  | yes | Batch JOIN aggregation → blackhole_sink |

## Running them

The same VVC SQL deployment pattern as scenario 11:

```bash
# Validate
ververica-test-harness run-pipeline sql.validate \
    --sql-script sql-deployments/05-batch-agg-wide-table.sql

# Deploy
ververica-test-harness run-pipeline workshop-scenario-12-05-batch-agg-deploy.yaml
```

The harness YAMLs live alongside the other workshop pipelines in
`/Users/bengamble/ververica-test-harness/pipelines/`.
