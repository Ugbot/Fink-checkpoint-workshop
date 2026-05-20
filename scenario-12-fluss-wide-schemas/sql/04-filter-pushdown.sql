-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 12 — Step 04: Filter pushdown — PK predicates vs the rest
-- ════════════════════════════════════════════════════════════════════════════
--
-- Fluss exposes two distinct read paths:
--
--   1. POINT LOOKUP on the primary key — uses the materialised KV index
--      for the PK column(s). O(log N) per lookup.
--
--   2. BUCKET / LOG SCAN for everything else — reads the WAL segments of
--      one or more buckets and applies non-PK filters in Flink.
--
-- The planner picks (1) automatically when the WHERE clause includes the
-- full PK or a PK prefix (for composite keys). Otherwise it falls back to
-- (2) and the filter runs as a regular Flink Calc operator above the source.
--
-- Vocabulary:
--   - **Filter pushdown** = the source applies the filter, fewer rows leave
--     the storage layer.
--   - **No pushdown** = the source returns all rows in the bucket(s) and
--     Flink filters them after.
--
-- For `instrument_master` the PK is `isin`, so:
--   - `WHERE isin = 'ISIN00000042'` → point lookup, pushed down.
--   - `WHERE country = 'US' AND sector = 'Technology'` → no pushdown, the
--     scan reads all buckets and the filter runs in Flink.
--
-- The EXPLAIN output makes both visible. Look for one of:
--   - `lookup=[...]`         — point lookup against the KV index.
--   - `filter=[...]` directly on the TableSourceScan — pushed-down filter.
--   - A separate `Calc(select=..., where=[...])` operator above the scan —
--     filter applied in Flink, not pushed down.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

SET 'execution.runtime-mode' = 'batch';

-- ── (A) PK lookup — pushed down ───────────────────────────────────────────
-- The filter is `isin = 'ISIN00000042'`. `isin` IS the primary key, so the
-- planner emits a point-lookup against the KV index instead of a bucket
-- scan. Look in the EXPLAIN for:
--
--   TableSourceScan(table=[[fluss, workshop, instrument_master, ...,
--                           project=[isin, ticker, last_close],
--                           lookup=[isin]]])
--
-- The `lookup=[isin]` tag tells Fluss "go straight to the KV index".
-- Expected runtime: milliseconds. Returns at most 1 row (PK uniqueness).
EXPLAIN
SELECT isin, ticker, last_close
FROM instrument_master
WHERE isin = 'ISIN00000042';

-- ── (B) Non-PK filter — runs in Flink ─────────────────────────────────────
-- The filter is `country = 'US' AND sector = 'Technology'`. Neither column
-- is the PK. The Fluss source has no per-bucket index on country or sector,
-- so the planner emits a full bucket scan and a Calc above it:
--
--   Calc(select=[isin, ticker], where=[((country = 'US') AND
--                                       (sector = 'Technology'))])
--   +- TableSourceScan(table=[[fluss, workshop, instrument_master, ...,
--                              project=[isin, ticker, country, sector]]])
--
-- Two things to notice:
--   - There is NO `filter=[...]` field on the scan — the filter is not
--     pushed down.
--   - The project list INCLUDES `country` and `sector` even though the
--     SELECT doesn't — they're needed by the filter, so they have to be
--     read from storage.
EXPLAIN
SELECT isin, ticker
FROM instrument_master
WHERE country = 'US' AND sector = 'Technology';

-- ── Run both queries for real ─────────────────────────────────────────────
SELECT isin, ticker, last_close
FROM instrument_master
WHERE isin = 'ISIN00000042';

SELECT isin, ticker
FROM instrument_master
WHERE country = 'US' AND sector = 'Technology';

-- ── Why the storage engine treats them differently ────────────────────────
--
-- Fluss's KV index is built ONLY for the primary-key columns. There is no
-- secondary-index facility today — that's a deliberate scope choice:
--   - Maintaining secondary indexes per column would amplify write cost
--     and create consistency questions on changelog-style data.
--   - The expected pattern is: PK lookups go through the KV index;
--     analytical scans use projection pushdown + bucket parallelism
--     (step 03 + step 05) to stay cheap.
--
-- Practical implication: if you frequently filter by `country` or
-- `sector` in a hot path, design the PK to include them. For example, a
-- "by-country security master" could use `PRIMARY KEY (country, isin)` —
-- then `WHERE country = 'US'` becomes a PK-prefix lookup that scans just
-- the matching bucket range.
--
-- This is the same design tradeoff you'd make for a Cassandra or DynamoDB
-- table — and very different from PostgreSQL where you'd just add an
-- index.
