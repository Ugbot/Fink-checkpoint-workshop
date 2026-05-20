-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 12 — Step 03: Projection pushdown (the centerpiece)
-- ════════════════════════════════════════════════════════════════════════════
--
-- *Projection pushdown* means: when the planner builds the source operator,
-- it tells Fluss which columns the query actually needs, and Fluss only reads
-- those columns off disk. For a 45-column table where the query reads 3
-- columns, that's a ~15× reduction in bytes-from-disk.
--
-- Two queries follow:
--   (A) `SELECT * ... WHERE country = 'US'` — needs every column.
--   (B) `SELECT isin, ticker, last_close ... WHERE country = 'US'`
--       — needs 4 columns (the 3 projected + `country` for the filter).
--
-- The EXPLAIN plans for (A) and (B) differ in one critical place: the
-- TableSourceScan's `project=[...]` field. In (B) the planner lists only
-- the 4 columns; the scan reads only those columns from Fluss; the bytes
-- moved off the tablet server are ~10% of (A).
--
-- After EXPLAIN, we run each query for real. In a properly-seeded table
-- (~10k rows) the wall-clock difference is small but visible. The real win
-- shows up at a million rows or in tight inner-loop pipelines — see
-- WORKSHOP.md for the production-scale numbers.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

SET 'execution.runtime-mode' = 'batch';

-- ── (A) Full-row read — every column travels off disk ─────────────────────
-- The planner has no choice: `SELECT *` means "give me every column", so
-- the TableSourceScan reads all 45 columns. Look in the EXPLAIN output for
-- a line like:
--
--   TableSourceScan(table=[[fluss, workshop, instrument_master, ...,
--                           project=[isin, cusip, sedol, ticker, exchange,
--                                    mic, security_type, sector, sub_sector,
--                                    country, currency, lot_size, tick_size,
--                                    ... all 45 columns ... ]]
--
-- The `project=[...]` list is the literal set of columns Fluss is asked to
-- materialise per row.
EXPLAIN
SELECT * FROM instrument_master WHERE country = 'US';

-- ── (B) Projected read — only 3 result columns + 1 filter column ──────────
-- The EXPLAIN output's TableSourceScan should now read:
--
--   TableSourceScan(table=[[fluss, workshop, instrument_master, ...,
--                           project=[isin, ticker, last_close, country]]])
--
-- Note `country` is in the project list even though it's not in the SELECT —
-- the source has to deliver it because the filter `country = 'US'` runs
-- *after* the scan but the column is still needed. (Step 04 covers the case
-- where the filter itself is pushed down too.)
--
-- The aggregate operator above the source then drops `country` before the
-- output is produced — by that point only 3 columns are flowing through
-- the rest of the plan.
EXPLAIN
SELECT isin, ticker, last_close FROM instrument_master WHERE country = 'US';

-- ── Run both queries for real ─────────────────────────────────────────────
-- The SQL Client prints "Query has been executed in X.YYY sec" at the end
-- of each result. Note both numbers and the row counts.

-- (A) — full row, 45 columns × N rows materialised
SELECT * FROM instrument_master WHERE country = 'US';

-- (B) — projected row, 3 columns × N rows materialised
SELECT isin, ticker, last_close FROM instrument_master WHERE country = 'US';

-- ── What to take away ──────────────────────────────────────────────────────
--
-- 1. The planner discovered the column subset by analysis of the SELECT
--    list and the WHERE clause; you did NOT have to write a different
--    query shape.
-- 2. The Fluss source operator honored the projection — it didn't just
--    read all 45 columns and discard 42 of them in Flink.
-- 3. The effect compounds with table width: at 45 columns you save ~93%
--    of the bytes per row. At 200 columns (real-world security masters
--    can be that wide) you'd save ~98%.
-- 4. Kafka cannot do this. JSON has no column index; Avro/Protobuf in
--    Kafka can be partially-decoded only with custom deserializers, and
--    the connector framework's `SupportsProjectionPushdown` interface
--    doesn't implement that for the Kafka connector by default.
--
-- The follow-on question is "what about filter pushdown?" — that's step 04.
