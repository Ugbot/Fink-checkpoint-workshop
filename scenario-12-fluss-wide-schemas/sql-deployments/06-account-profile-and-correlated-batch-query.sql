-- Deployable variant of step 06: cross-store batch query against Fluss
-- account_profile + Postgres accounts, written into a blackhole sink.
--
-- Batch mode is essential here — without it the JDBC source would either
-- run once and finish (leaving the Fluss source running forever) or fail
-- because JDBC isn't a streaming source. Batch tells both sides to bound
-- at the current snapshot, run the join, emit the aggregation, terminate.

SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

CREATE TEMPORARY TABLE pg_accounts (
    account_id    STRING,
    account_name  STRING,
    tier          STRING,
    region        STRING
) WITH (
    'connector'  = 'jdbc',
    'url'        = 'jdbc:postgresql://workshop-postgres:5432/workshop',
    'table-name' = 'accounts',
    'username'   = 'workshop',
    'password'   = 'workshop'
);

CREATE TEMPORARY TABLE blackhole_sink (
    tier          STRING,
    fluss_count   BIGINT,
    pg_count      BIGINT,
    missing_in_pg BIGINT
) WITH ('connector' = 'blackhole');

INSERT INTO blackhole_sink
SELECT
  ap.tier,
  COUNT(*)                       AS fluss_count,
  COUNT(pg.account_id)           AS pg_count,
  COUNT(*) - COUNT(pg.account_id) AS missing_in_pg
FROM account_profile AS ap
LEFT JOIN pg_accounts AS pg
  ON ap.account_id = pg.account_id
GROUP BY ap.tier;
