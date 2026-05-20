-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 11 — Step 03: Log tables (append-only, the "Kafka-shaped" half of Fluss)
-- ════════════════════════════════════════════════════════════════════════════
--
-- A Fluss LOG TABLE is append-only. Rows have no primary key. Mentally it's
-- close to a Kafka topic with typed columns and a schema.
--
-- Use it when:
--   - You want an immutable record of every event (trades, audit log,
--     orders-as-issued).
--   - Multiple consumers will replay or join against the stream.
--   - You don't need point-lookups or upserts on the data.
--
-- The workshop's `trade_log_wide` is a log table created by the init job
-- with `bucket.num = 8`. Buckets are Fluss's parallel-write unit — like
-- Kafka partitions but with Fluss-managed placement.
--
-- This step:
--   (a) Insert a handful of sample rows.
--   (b) Read them back in BATCH mode (one-shot snapshot).
--   (c) Read them back in STREAMING mode (continuous tail).
--
-- Both reads work against the same table — Fluss is "queryable streaming",
-- which is one of its core ideas.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

-- ── (a) Insert a few rows ──────────────────────────────────────────────────
-- This is one short batch insert just for illustration; the long-running
-- FlussDatagenJob (scripts/seed-fluss.sh) keeps `trade_log_wide` flowing
-- continuously.
INSERT INTO trade_log_wide VALUES
  ('evt-001', 'ACC-0001', 'CUST-000001', 'TKR0', 'ISIN00000000', 'BUY',  'MARKET',  100,
   45.10, 4510.00, 'USD', 1.0,    4510.00, 'NYSE',   'NYSE-MAIN', 'Technology', 'US',
   'EQ-CASH', 'trader-1', 1747800000000, 1748000000000, 'PENDING', 2.26, 0.45, 'manual'),
  ('evt-002', 'ACC-0002', 'CUST-000002', 'TKR0', 'ISIN00000000', 'SELL', 'LIMIT',    50,
   45.15, 2257.50, 'EUR', 1.09,   2460.68, 'XETR',   'XETR-MAIN', 'Technology', 'DE',
   'EQ-CASH', 'trader-2', 1747800000000, 1748000000000, 'PENDING', 1.13, 0.23, 'manual'),
  ('evt-003', 'ACC-0003', 'CUST-000003', 'TKR1', 'ISIN00000001', 'BUY',  'MARKET',  200,
   12.34, 2468.00, 'GBP', 1.27,   3134.36, 'LSE',    'LSE-MAIN',  'Financials', 'GB',
   'EQ-CASH', 'trader-3', 1747800000000, 1748000000000, 'PENDING', 1.23, 0.25, 'manual');

-- ── (b) Batch read — a snapshot of what's there right now ──────────────────
SET 'execution.runtime-mode' = 'batch';
SELECT event_id, ticker, side, quantity, price, currency
FROM trade_log_wide
LIMIT 10;
-- The batch read terminates: it returns once it has scanned all currently-
-- committed rows. This is exactly how you'd query the table from BI tools.

-- ── (c) Streaming read — keep tailing as new rows arrive ──────────────────
SET 'execution.runtime-mode' = 'streaming';
-- In a SQL Client this query runs until you cancel it. Each row that the
-- FlussDatagenJob writes appears here within a few hundred ms.
SELECT event_id, ticker, side, quantity, price
FROM trade_log_wide
WHERE side = 'BUY';

-- Things to internalise:
--   - Same DDL, two execution modes. You do NOT need two tables.
--   - In a streaming read, `WHERE side = 'BUY'` is a filter on the stream;
--     in batch it's the same filter applied to a finite snapshot.
--   - The result of a streaming SELECT on a log table is itself a stream.
--     You can INSERT INTO another sink with it.
