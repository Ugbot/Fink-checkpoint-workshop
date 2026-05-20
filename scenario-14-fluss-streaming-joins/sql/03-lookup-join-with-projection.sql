-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 14 — Step 03: Projection pushdown into lookup joins
-- ════════════════════════════════════════════════════════════════════════════
--
-- Step 02 selected nine columns across four tables. The two Fluss PK
-- tables we joined against are WIDE — instrument_master is 45 columns,
-- account_profile is 30. If every lookup fetched the whole row, we'd be
-- spending most of our network bandwidth on columns the query throws
-- away. Scenario 12 made this dramatic for batch reads; the same win
-- applies to lookup joins.
--
-- The Fluss connector supports projection pushdown into the lookup path:
-- the planner figures out which columns each side actually contributes
-- to the result, and tells Fluss to send only those columns back. The
-- savings can be substantial — fewer bytes per lookup means lower tail
-- latency and lower Fluss read load.
--
-- This step does two things:
--   (a) Issues a deliberately *narrow* query — pick exactly two columns
--       from each Fluss side. The planner should push those projections
--       into the lookup operators.
--   (b) Calls out what to look for in the EXPLAIN diff vs step 02's
--       wider query.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);

CREATE TEMPORARY TABLE kafka_trades (
  event_id    STRING,
  account_id  STRING,
  ticker      STRING,
  side        STRING,
  quantity    INT,
  price       DOUBLE,
  trade_time  BIGINT,
  proc_time   AS PROCTIME()
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'topic.in',
  'properties.bootstrap.servers' = 'workshop-kafka:9093',
  'properties.group.id'          = 'sql-s14-03',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

-- ── (1) The narrow lookup join ─────────────────────────────────────────────
-- Two columns from instrument_master (sector, last_close) out of 45.
-- Two columns from account_profile (tier, risk_score) out of 30.
-- Two columns from the trade (event_id, ticker).
-- Total: 6 columns of output. That's <8% of the columns these tables
-- expose; without projection pushdown we'd be reading >12x what we need
-- on each lookup.
SELECT
  t.event_id,
  t.ticker,
  im.sector,
  im.last_close,
  ap.tier,
  ap.risk_score
FROM kafka_trades t
LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im
       ON t.ticker     = im.ticker
LEFT JOIN fluss.workshop.account_profile   FOR SYSTEM_TIME AS OF t.proc_time AS ap
       ON t.account_id = ap.account_id;

-- ── (2) EXPLAIN: spot the projection in the lookup operator ────────────────
-- Run this in the SQL Client:
--
-- EXPLAIN
-- SELECT t.event_id, t.ticker, im.sector, im.last_close, ap.tier, ap.risk_score
-- FROM kafka_trades t
-- LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im ON t.ticker = im.ticker
-- LEFT JOIN fluss.workshop.account_profile   FOR SYSTEM_TIME AS OF t.proc_time AS ap ON t.account_id = ap.account_id;
--
-- Look for the `LookupJoin` operator on each Fluss side. Inside it you'll
-- see a `select=[...]` or `lookup=[...]` annotation listing ONLY the
-- columns we actually project — something close to:
--
--   LookupJoin(table=[fluss.workshop.instrument_master],
--              joinType=[LeftOuterJoin],
--              lookup=[ticker=ticker],
--              select=[ticker, sector, last_close])
--
--   LookupJoin(table=[fluss.workshop.account_profile],
--              joinType=[LeftOuterJoin],
--              lookup=[account_id=account_id],
--              select=[account_id, tier, risk_score])
--
-- That `select=[...]` IS the projection pushed into Fluss. The lookup
-- only retrieves those columns from the tablet server; the other 40 (im)
-- / 27 (ap) columns are never read off disk, never serialised, never
-- shipped over the wire.
--
-- ── (3) Compare with step 02's EXPLAIN ─────────────────────────────────────
-- Step 02 selected `im.sector, im.last_close, ap.tier, ap.risk_score, pg.region`
-- plus the Postgres `tier`. The Fluss LookupJoin `select=[...]` lists
-- match what's projected — sector, last_close from instrument_master and
-- tier, risk_score from account_profile.
--
-- If you change this query to `SELECT *` (don't — but as a thought
-- experiment) the planner can no longer prune; the EXPLAIN will show every
-- column from each side in the `select=[...]` list, and the per-lookup
-- payload will balloon.
--
-- The PRACTICAL CONSEQUENCE for production deployments:
--   • Always `SELECT col1, col2, …` not `SELECT *` when joining wide PK
--     tables. The planner can only push down what it can prove unused.
--   • For tables with many wide columns (45+, as here), the bandwidth
--     and CPU difference is measurable end-to-end.
--   • This applies recursively — if you stack a SELECT-projection on
--     top of a view that selects all columns, the planner can sometimes
--     push the projection back down. Sometimes it can't. EXPLAIN is the
--     ground truth.
--
-- ── (4) Compose with caching for extra wins ────────────────────────────────
-- Projection pushdown and `lookup.cache` are independent optimisations.
-- Use them together when:
--   • The probe stream has key skew (caching wins).
--   • The source tables are wide (projection wins).
-- Together you get small, fast, cache-friendly lookups.
--
-- ── Recap ──────────────────────────────────────────────────────────────────
-- The planner pushes column projections into Fluss's lookup path. Narrow
-- SELECTs cost much less than wide ones, even though both look identical
-- in operator shape. This is why scenario 12 spent a whole step on
-- projection pushdown for batch reads — the same machinery serves the
-- streaming lookup-join hot path.
--
-- Next step (04) flips the lookup join into a *regular* join over the
-- Fluss changelog, so we can directly compare the state cost.
