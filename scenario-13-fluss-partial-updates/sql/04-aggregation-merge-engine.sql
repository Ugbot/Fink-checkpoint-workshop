-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 13 — Step 04: Aggregation merge engine — sum/max/min per column
-- ════════════════════════════════════════════════════════════════════════════
--
-- "last_write_wins" is just one of several merge engines Fluss supports.
-- The **aggregation** merge engine treats each column as accumulating
-- with a specified function (sum / max / min / count / first_value / …).
--
-- Use case: running daily aggregates per account. Each incoming trade
-- contributes to total_qty (sum), max_price (max), min_price (min),
-- last_trade_ts (max). You write one row per trade event — Fluss merges
-- them into a single aggregate row per PK.
--
-- This avoids a Flink keyed-aggregate operator entirely. State lives in
-- Fluss, not in Flink.
--
-- The init job created `daily_volumes_agg` with this configuration:
--   'table.merge-engine' = 'aggregation'
--   'fields.total_qty.aggregate-function'      = 'sum'
--   'fields.total_notional.aggregate-function' = 'sum'
--   'fields.max_price.aggregate-function'      = 'max'
--   'fields.min_price.aggregate-function'      = 'min'
--   'fields.last_trade_ts.aggregate-function'  = 'max'

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;
SET 'execution.runtime-mode' = 'batch';

SHOW CREATE TABLE daily_volumes_agg;

-- ── Multiple "trade" inserts for the same (account_id, trade_date) ──────────
-- Each call contributes one trade. The total_qty/total_notional columns
-- accumulate; max_price / min_price track extrema; last_trade_ts tracks
-- the most recent.
INSERT INTO daily_volumes_agg
    (account_id, trade_date, total_qty, total_notional, max_price, min_price, last_trade_ts)
VALUES
    ('ACC-9001', '2026-05-19', 100, 4510.0, 45.10, 45.10, 1747900000000);

INSERT INTO daily_volumes_agg
    (account_id, trade_date, total_qty, total_notional, max_price, min_price, last_trade_ts)
VALUES
    ('ACC-9001', '2026-05-19',  50, 2257.5, 45.15, 45.10, 1747900100000);

INSERT INTO daily_volumes_agg
    (account_id, trade_date, total_qty, total_notional, max_price, min_price, last_trade_ts)
VALUES
    ('ACC-9001', '2026-05-19', 200, 9020.0, 45.10, 44.90, 1747900200000);

-- ── Inspect the merged row ─────────────────────────────────────────────────
SELECT account_id, trade_date, total_qty, total_notional, max_price, min_price, last_trade_ts
FROM daily_volumes_agg
WHERE account_id = 'ACC-9001' AND trade_date = '2026-05-19';
-- ↑ Expect:
--    total_qty       350         (sum of 100 + 50 + 200)
--    total_notional  15787.5     (sum of 4510.0 + 2257.5 + 9020.0)
--    max_price       45.15       (max of 45.10, 45.15, 45.10)
--    min_price       44.90       (min of 45.10, 45.10, 44.90)
--    last_trade_ts   1747900200000  (max timestamp)

-- ── Why this matters ───────────────────────────────────────────────────────
-- The "Flink-only" version of this is:
--   trades.keyBy(account+date)
--         .process(aggregator with ValueState<Tuple5<Long,Double,...>>)
--         .toUpsertStream(sink)
-- With the Fluss aggregation merge engine, the aggregator state lives in
-- the storage layer and survives Flink restarts independent of Flink's
-- checkpoint cycle. Multiple Flink jobs (or even non-Flink writers) can
-- contribute to the same aggregate.
