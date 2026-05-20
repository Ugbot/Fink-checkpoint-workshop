-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 11 — Step 05: Batch-mode aggregation over a Fluss log table
-- ════════════════════════════════════════════════════════════════════════════
--
-- Fluss is queryable in BOTH streaming and batch modes against the same
-- physical table. Switching to batch is one `SET` statement away. This is
-- a feature Kafka does not offer — for Kafka you'd typically:
--    - Tier to a lakehouse and query that (extra storage system),
--    - Or replay through a one-off Flink job + sink.
--
-- This step shows:
--   (a) A batch aggregation over `trade_log_wide` — a one-shot query that
--       reads all available rows and produces a finite result.
--   (b) The EXPLAIN plan, demonstrating that Fluss reports its bounded scan
--       at the source.
--
-- For the EXPLAIN to be useful, run the seed-fluss.sh script first so the
-- table contains thousands of rows.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

-- ── (a) Batch aggregation ──────────────────────────────────────────────────
SET 'execution.runtime-mode' = 'batch';

SELECT
  account_id,
  COUNT(*)        AS trade_count,
  SUM(quantity)   AS total_qty,
  SUM(notional)   AS total_notional,
  MAX(price)      AS max_price,
  MIN(price)      AS min_price
FROM trade_log_wide
GROUP BY account_id
ORDER BY total_notional DESC
LIMIT 10;

-- ── (b) EXPLAIN plan ───────────────────────────────────────────────────────
-- Look for these in the output:
--   - `TableSourceScan(table=[[fluss, workshop, trade_log_wide]])`
--     The planner identified Fluss as the source.
--   - The `runtime-mode` line confirms batch.
--   - The aggregate operator (HashAggregate / SortAggregate).
EXPLAIN
SELECT account_id, COUNT(*) AS trade_count, SUM(notional) AS total_notional
FROM trade_log_wide
GROUP BY account_id;

-- Compare:
--   - On Kafka: the same query would scan the topic from the beginning every
--     time. With Fluss in batch mode you get a finite scan that respects
--     parallelism via buckets.
--   - On Fluss in streaming mode: the same query is *continuous* — counts
--     update as new trades arrive. Same SELECT, different runtime mode.
