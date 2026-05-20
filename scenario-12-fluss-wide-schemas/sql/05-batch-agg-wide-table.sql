-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 12 — Step 05: Batch aggregation over a wide PK table
-- ════════════════════════════════════════════════════════════════════════════
--
-- This step combines projection pushdown (step 03) with Fluss's batch
-- execution mode (scenario 11 step 05) on the widest table in the workshop.
--
-- The query is a standard analytical aggregation: "for each GICS sector,
-- how many instruments, average 1-year beta, average ESG score". It touches
-- 4 of the 45 columns:
--
--    sector          (group key)
--    beta_1y         (numeric)
--    esg_score       (numeric)
--    (COUNT(*)       — no column needed)
--
-- Without projection pushdown this would read 45 columns × ~10k rows from
-- the tablet servers; with pushdown it reads 4 columns × ~10k rows. For a
-- million-row production table that's the difference between hundreds of MB
-- and tens of MB of disk reads.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

-- Batch mode: the query terminates once all currently-committed rows are
-- consumed. (In streaming mode the same query would emit retract+update
-- events forever as new rows arrive.)
SET 'execution.runtime-mode' = 'batch';

-- ── (a) The aggregation ───────────────────────────────────────────────────
SELECT
  sector,
  COUNT(*)            AS instrument_count,
  AVG(beta_1y)        AS avg_beta_1y,
  AVG(esg_score)      AS avg_esg_score
FROM instrument_master
GROUP BY sector
ORDER BY instrument_count DESC;
-- Expected output: ~11 rows (one per GICS sector). The largest count is
-- usually Technology or Financials with several thousand instruments.

-- ── (b) EXPLAIN: confirm projection pushdown ──────────────────────────────
-- The `project=[sector, beta_1y, esg_score]` field on the TableSourceScan
-- is the win. The other 42 columns never leave Fluss.
EXPLAIN
SELECT
  sector,
  COUNT(*)            AS instrument_count,
  AVG(beta_1y)        AS avg_beta_1y,
  AVG(esg_score)      AS avg_esg_score
FROM instrument_master
GROUP BY sector;

-- ── (c) "What would this cost on Kafka?" — back-of-envelope ───────────────
--
-- Imagine the same data lived as a JSON-valued topic `instrument_master.v1`
-- with one record per isin. To compute the same aggregation you'd write:
--
--   CREATE TEMPORARY TABLE kafka_im (
--       isin STRING, sector STRING, beta_1y DOUBLE, esg_score DOUBLE, ...
--   ) WITH ('connector' = 'kafka', 'format' = 'json', ...);
--
--   SELECT sector, COUNT(*), AVG(beta_1y), AVG(esg_score)
--   FROM kafka_im GROUP BY sector;
--
-- What actually happens at runtime:
--
--   1. The Kafka source reads every record's value as a complete JSON blob.
--      The Kafka connector's `JsonRowDataDeserializationSchema` parses the
--      full document for every record — JSON has no per-field offset table,
--      so you can't skip fields you don't need.
--
--   2. After deserialization the planner has typed columns, and projection
--      pushdown at the Flink (not Kafka) layer drops the 42 unneeded ones.
--      But you've already paid the parsing cost.
--
--   3. The scan is unbounded by default. To get a one-shot answer you'd
--      either use the bounded-mode Kafka source (start offset → committed
--      end offset) or run a streaming query and accept that it never
--      terminates.
--
-- Even at modest volume the cost difference is measurable. At 1M records:
--
--   Fluss:     read ~24 bytes × 1M = ~24 MB off disk
--              (3 doubles + 1 string, projection-pushed)
--
--   Kafka JSON: read ~2 KB × 1M    = ~2 GB off disk
--               + parse 1M JSON documents in 45-field schemas
--
-- That's a ~80× difference in IO and a similar order on CPU. This is the
-- engineering reason workshops on streaming analytics keep coming back to
-- "store it in a system the planner understands".
