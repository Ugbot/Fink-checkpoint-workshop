-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 16 — Step 03: Query the Fluss table — hot + cold unified
-- ════════════════════════════════════════════════════════════════════════════
--
-- When `table.datalake.enabled = true`, reads from the Fluss side return
-- BOTH:
--   - hot data still in Fluss segments (not yet tiered)
--   - cold data in the Paimon mirror (already tiered)
--
-- The Fluss connector handles the union automatically. The result is a
-- complete, up-to-date view.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

SET 'execution.runtime-mode' = 'batch';

-- ── A "complete history" query ─────────────────────────────────────────────
SELECT COUNT(*) AS total_rows FROM trade_log_wide;
-- Compare this with step 02's COUNT(*) over the Paimon side. The Fluss
-- side's count is ≥ the Paimon-side count (Fluss = Paimon + hot).

-- ── Aggregation that pulls from both layers ────────────────────────────────
SELECT
  sector,
  COUNT(*)               AS trade_count,
  SUM(quantity)          AS total_qty,
  SUM(notional_usd)      AS total_notional_usd
FROM trade_log_wide
GROUP BY sector
ORDER BY total_notional_usd DESC;
-- The planner reads the relevant columns from the Paimon side (Parquet,
-- projection-pushdown applies) AND from the Fluss side (in-memory segments).

-- ── Time-travel via scan.startup ──────────────────────────────────────────
-- Read from the very beginning of the (Fluss + Paimon) timeline:
-- SELECT COUNT(*) FROM trade_log_wide
--   /*+ OPTIONS('scan.startup.mode'='earliest-offset') */;
--
-- Read only data older than 5 minutes (force-include the tiered layer):
-- SELECT * FROM trade_log_wide
--   /*+ OPTIONS('scan.startup.mode'='timestamp', 'scan.startup.timestamp-millis'='1747900000000') */;

-- ── EXPLAIN — observe the union ────────────────────────────────────────────
EXPLAIN
SELECT sector, COUNT(*) AS n
FROM trade_log_wide
GROUP BY sector;
-- The plan shows the table source reading from the unified Fluss+Paimon
-- backing. Implementation details vary by Fluss version but you should see
-- the table source operator labelled with both Fluss and Paimon coordinates.
