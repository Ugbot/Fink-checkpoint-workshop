-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 16 — Step 04: Query the Paimon mirror directly (no Fluss needed)
-- ════════════════════════════════════════════════════════════════════════════
--
-- The Fluss-tiered data on Paimon is JUST PAIMON. Any engine that can read
-- a Paimon catalog can query it without going through Fluss. This is the
-- promise of "open table formats" — your historical data is not locked in
-- to the streaming engine.
--
-- We prove it by using scenario 06's existing Paimon catalog setup.

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type'                 = 'paimon',
  'warehouse'            = 's3a://paimon/warehouse',
  's3.endpoint'          = 'http://workshop-minio:9000',
  's3.access-key'        = 'workshop',
  's3.secret-key'        = 'workshopsecret',
  's3.path.style.access' = 'true'
);

USE CATALOG paimon;
USE workshop;

SET 'execution.runtime-mode' = 'batch';

-- ── Same aggregation as scenario 16/03, this time on the Paimon side ────────
SELECT
  sector,
  COUNT(*)              AS trade_count,
  SUM(quantity)         AS total_qty,
  SUM(notional_usd)     AS total_notional_usd
FROM trade_log_wide
GROUP BY sector
ORDER BY total_notional_usd DESC;
-- The result is *almost* the same as the Fluss-side query in step 03
-- (the difference is the hot data not yet tiered — at most
-- `table.datalake.freshness` worth of staleness).

-- ── Per-bucket breakdown using the system columns ──────────────────────────
SELECT __bucket, MIN(__timestamp) AS first_tier, MAX(__timestamp) AS last_tier, COUNT(*) AS rows
FROM trade_log_wide
GROUP BY __bucket
ORDER BY __bucket;

-- ── Cross-store join: Paimon + Fluss in one query ──────────────────────────
-- Register the Fluss catalog alongside Paimon so both are reachable from
-- the same session.
CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);

-- Join the tiered (cold) Paimon side against the Fluss live `instrument_master`
-- to enrich each historical trade with current security-master attributes:
SELECT
  t.sector,
  COUNT(*)         AS trade_count,
  AVG(im.beta_1y)  AS avg_beta
FROM paimon.workshop.trade_log_wide t
LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF PROCTIME() AS im
  ON t.isin = im.isin
GROUP BY t.sector
ORDER BY trade_count DESC;
-- This is a hybrid query: the bulk of the data (cold trade history) comes
-- from Paimon; the slow-changing dimension (instrument master) comes from
-- Fluss. Cheap historical scan + fresh dim — the whole pitch of "streaming
-- lakehouse" in one query.
