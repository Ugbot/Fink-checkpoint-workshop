-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 12 — Step 06: Cross-store batch query — Fluss × Postgres
-- ════════════════════════════════════════════════════════════════════════════
--
-- The workshop has two account stores:
--
--   - Fluss `account_profile` — the streaming-friendly 30-column PK table
--     populated by FlussDatagenJob. This is the rich operational view.
--
--   - Postgres `accounts` — a small slow-changing dimension (4 columns)
--     populated by AccountSeedJob for the scenario 07–09 lookup-join demos.
--     This is the "system of record" copy.
--
-- A natural reconciliation question: "for each customer tier, how many
-- accounts exist in Fluss vs Postgres?" This is a batch analytical query
-- across two storage systems — exactly the case Flink SQL is designed for.
--
-- This step:
--   (a) Defines a temporary JDBC table over the Postgres `accounts` table.
--   (b) Runs a LEFT JOIN aggregation by tier so we can see Fluss rows that
--       have no matching Postgres row (the Fluss seed creates more accounts
--       than the Postgres seed does).
--
-- A note on temporary tables: `CREATE TEMPORARY TABLE` registers the table
-- in the current Flink session only — it doesn't write anything to Fluss
-- or to a catalog. Drop the session and the registration is gone.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

-- ── (a) Postgres bridge ───────────────────────────────────────────────────
-- The JDBC connector is part of the workshop's Flink image. The credentials
-- match what `docker-compose` provisions for the Postgres container.
--
-- Only 4 columns are declared because that's all the Postgres table has.
-- The schema must match positionally — declaring extra columns in Flink
-- that don't exist in Postgres errors at query time.
CREATE TEMPORARY TABLE pg_accounts (
    account_id    STRING,
    account_name  STRING,
    tier          STRING,
    region        STRING
) WITH (
    'connector' = 'jdbc',
    'url'       = 'jdbc:postgresql://workshop-postgres:5432/workshop',
    'table-name' = 'accounts',
    'username'  = 'workshop',
    'password'  = 'workshop'
);

-- ── (b) Cross-store batch aggregation ─────────────────────────────────────
-- Batch mode: the JDBC source is naturally bounded (single SELECT against
-- Postgres) and the Fluss source bounds at its current snapshot. Both
-- sides terminate, the join produces a finite result, the GROUP BY emits
-- one row per tier.
SET 'execution.runtime-mode' = 'batch';

SELECT
  ap.tier,
  COUNT(*)                       AS fluss_count,
  COUNT(pg.account_id)           AS pg_count,
  COUNT(*) - COUNT(pg.account_id) AS missing_in_pg
FROM account_profile AS ap
LEFT JOIN pg_accounts AS pg
  ON ap.account_id = pg.account_id
GROUP BY ap.tier
ORDER BY fluss_count DESC;
-- Expected: one row per tier (GOLD, PLATINUM, SILVER, ...). The
-- `missing_in_pg` column shows how many Fluss accounts have no Postgres
-- twin — typically a large number because the Fluss seed runs continuously
-- while the Postgres seed is one-shot.

-- ── EXPLAIN — confirm projection pushdown applies to both sides ───────────
-- For the Fluss source only `account_id` and `tier` are needed (the join
-- key + group key). For the Postgres source only `account_id` is needed.
-- The EXPLAIN plan should show:
--
--   - TableSourceScan(table=[[fluss, ..., account_profile, ...,
--                             project=[account_id, tier]]])
--   - TableSourceScan(table=[[default_catalog, ..., pg_accounts, ...,
--                             project=[account_id]]])
--   - Join(joinType=[LeftOuterJoin], where=[(account_id = account_id0)])
--   - HashAggregate(group=[tier], select=[tier, COUNT(*), COUNT(account_id)])
EXPLAIN
SELECT
  ap.tier,
  COUNT(*)             AS fluss_count,
  COUNT(pg.account_id) AS pg_count
FROM account_profile AS ap
LEFT JOIN pg_accounts AS pg
  ON ap.account_id = pg.account_id
GROUP BY ap.tier;

-- ── Why this matters ──────────────────────────────────────────────────────
--
-- A "data warehouse" version of this query would require:
--
--   1. A nightly ETL that exports Fluss to Parquet on S3.
--   2. A second nightly ETL that exports Postgres to Parquet on S3.
--   3. A Trino / Spark cluster to run the join.
--
-- With Flink SQL + Fluss + JDBC connector, the same query is ~15 lines and
-- runs in seconds against the current state of both stores. No ETL, no
-- duplicated copies of the data, and the same SQL can be rerun on a cron
-- as part of a reconciliation pipeline.
--
-- Switching `execution.runtime-mode = 'streaming'` would turn this into a
-- continuous join — `pg_accounts` becomes a temporal lookup table and each
-- new Fluss row probes Postgres. That's the path scenario 14 explores in
-- detail (lookup joins).
