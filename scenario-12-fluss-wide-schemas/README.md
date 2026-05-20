# Scenario 12 — Fluss Wide Schemas, Projection & Filter Pushdown

> Second scenario in the Fluss track. Assumes you've done
> [scenario 11](../scenario-11-fluss-fundamentals/) (catalog DDL, log
> tables, PK tables).

This scenario is the deep-dive on **what makes a Fluss table different from
a Kafka topic when the schema is wide**. The vehicle is the workshop's
45-column `instrument_master` security-reference table plus the 30-column
`account_profile`. Both were created and populated by the workshop init
job — there is nothing to (re)create here, just queries to run and EXPLAIN
plans to read.

---

## What you'll learn

| Concept | Where you see it |
|---|---|
| Domain-shaped 45-column wide schemas | `sql/01-instrument-master-ddl.sql` |
| Verifying seed data + on-disk layout | `sql/02-load-instrument-master.sql` |
| **Projection pushdown** in EXPLAIN plans | `sql/03-projection-pushdown.sql` |
| **Filter pushdown** — PK predicates vs the rest | `sql/04-filter-pushdown.sql` |
| Batch aggregation over a wide table | `sql/05-batch-agg-wide-table.sql` |
| Cross-store batch JOIN (Fluss × Postgres) | `sql/06-account-profile-and-correlated-batch-query.sql` |

---

## Prerequisites

The same infra as scenario 11. If you've just done scenario 11 you don't
need to re-run anything.

```bash
podman-compose up -d --build         # Fluss + Postgres + Flink + SQL Gateway
bash scripts/init-fluss.sh           # creates the workshop database + tables
bash scripts/seed-fluss.sh           # seeds them with realistic data
```

The Postgres seed (`scripts/init-postgres.sql` + `AccountSeedJob`) is
applied automatically on first compose-up; you only need to re-run it
manually if you've reset the Postgres volume.

Step 06 joins Fluss with the Postgres `accounts` table. If you've never
run the scenario-07/08 demos, the Postgres table may be empty — run
`bash scripts/seed-accounts.sh` once before step 06 to populate it.

---

## How to run it

The SQL files in [`sql/`](./sql/) are written for the **SQL Client / SQL
Gateway** (scenario 06's gateway works fine — same Flink session cluster).

```bash
bash scripts/sql-client.sh
# Then paste any of the .sql files in order: 01 → 02 → 03 → 04 → 05 → 06.
```

For VVC deployment of the deployable steps (03, 05, 06), see
[`sql-deployments/`](./sql-deployments/) and the matching harness pipelines:

- `workshop-scenario-12-03-projection-pushdown-deploy.yaml`
- `workshop-scenario-12-05-batch-agg-wide-table-deploy.yaml`
- `workshop-scenario-12-06-cross-store-join-deploy.yaml`

---

## The 6 steps

1. **Instrument master DDL** — DESCRIBE the 45-column PK table; read the
   column groupings (identity / classification / trading / risk /
   fundamentals / ESG / lifecycle) and the `bucket.num = 4` option.
2. **Load instrument master** — COUNT + LIMIT preview to confirm the seed
   ran; sidebar on inspecting Fluss's on-disk segments via `podman exec`.
3. **Projection pushdown** — EXPLAIN `SELECT *` vs `SELECT 3 cols`, then
   run both. The TableSourceScan's `project=[...]` list is the headline.
4. **Filter pushdown** — EXPLAIN a PK lookup vs a non-PK filter; observe
   that only the PK predicate is pushed into the storage engine.
5. **Batch aggregation** — `SET 'execution.runtime-mode' = 'batch'` then
   GROUP BY sector. 4 of 45 columns touched; EXPLAIN confirms it.
6. **Cross-store batch join** — temporary JDBC table over Postgres,
   LEFT JOIN against Fluss `account_profile`, GROUP BY tier. One Flink
   query, two backing stores, finite result.

For the deep-dive walkthrough with the "Concept / Story / Observe / Aha /
Pitfall" sections per step, see [`WORKSHOP.md`](./WORKSHOP.md).

---

## Verification

```bash
bash scripts/verify-12.sh
```

The script checks:

- `SHOW CATALOGS` returns `fluss`.
- `SELECT COUNT(*) FROM instrument_master` returns > 0 (seed ran).
- `EXPLAIN SELECT isin, ticker, last_close FROM instrument_master WHERE country = 'US'`
  produces a plan that mentions all three projected columns and **does not**
  mention the full 45-column list — i.e. projection pushdown is happening.

---

## See also

- [`../scenario-11-fluss-fundamentals/`](../scenario-11-fluss-fundamentals/)
  — catalog DDL, log vs PK tables; the prerequisite for this scenario.
- [`../scenario-13-fluss-merge-engines/`](../scenario-13-fluss-merge-engines/)
  — partial updates, merge engine variants on the same wide tables.
- [`../scenario-14-fluss-lookup-joins/`](../scenario-14-fluss-lookup-joins/)
  — the streaming counterpart to step 04's point lookup.
- Apache Fluss docs on pushdown: <https://fluss.apache.org/docs/connectors/flink/>
