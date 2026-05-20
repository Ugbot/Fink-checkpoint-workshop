-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 14 — Step 01: The basic lookup-join idiom against a Fluss PK table
-- ════════════════════════════════════════════════════════════════════════════
--
-- This is the headline use-case Fluss is built for. We have a high-volume
-- streaming source (Kafka `topic.in` — every executed trade) and we want to
-- enrich each event with reference data (sector, last close, beta, …) that
-- lives in a primary-key table.
--
-- The classical alternatives, all bad in some way:
--   • Materialise the dim table in Flink operator state → state grows with
--     the cardinality of `instrument_master` and has to be kept in sync.
--   • Call out to a Redis / Postgres lookup service per event → another
--     storage system, separate ops, separate consistency story.
--   • Replay a Kafka compacted topic at job startup → cold-start delay,
--     ordering pitfalls, no point lookups by key.
--
-- The Fluss lookup-join idiom collapses all of that into one SQL clause:
--
--     LEFT JOIN <fluss-pk-table>
--       FOR SYSTEM_TIME AS OF <proctime>
--       AS t ON <event>.<fk> = t.<pk>
--
-- This emits a row per probe event. The right side has NO Flink state —
-- every match is a synchronous point lookup against Fluss, which serves it
-- from its tablet servers in O(log N).
--
-- The "FOR SYSTEM_TIME AS OF <proctime>" clause is what tells the planner
-- "this is a lookup join, not a regular join". It's the same syntax used
-- for temporal-table joins in scenario 08 — the difference here is the
-- right side is a Fluss PK table, which natively supports point lookups,
-- so the join compiles into a `LookupJoin` operator rather than the
-- temporal-table version that needs versioned state.
--
-- IMPORTANT teaching point: this query is STATE-FREE on the right side.
-- If `instrument_master` doubles in size tomorrow, Flink uses 0 extra MB
-- of state. The new data is just visible to the next lookup. Compare with
-- step 04 where the same enrichment as a *regular* join holds the entire
-- right-side table in Flink state.

-- ── (0) Register the Fluss catalog ─────────────────────────────────────────
-- Same DDL pattern as scenarios 11–13. The catalog definition lives in this
-- Flink session; the data lives in the Fluss cluster.
CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);

-- We keep the default Flink catalog active and reference Fluss tables by
-- their fully-qualified name `fluss.workshop.<table>`. That keeps the Kafka
-- DDL in the session catalog where temporary DDL belongs.

-- ── (1) Kafka source for the trade stream ──────────────────────────────────
-- The trade datagen (FinancialDatagenJob, see scenarios 01–05) writes JSON
-- records to `topic.in` continuously at ~50 events/sec.
--
-- The crucial column for lookup joins is the COMPUTED column `proc_time AS
-- PROCTIME()`. It's not a real field in the Kafka payload — it's evaluated
-- on the operator. `PROCTIME()` returns the operator's wall-clock millis at
-- the moment the row is processed. We use it as the "as-of" timestamp in
-- the join: "look up the instrument as of NOW".
--
-- For a true event-time temporal join (scenario 08) you'd use the record's
-- own event_time column and require both sides to declare watermarks.
-- For the Fluss lookup-join pattern, proctime is the idiomatic choice —
-- it means "join against whatever the PK table looks like at lookup time"
-- which is exactly what enrichment wants.
CREATE TEMPORARY TABLE kafka_trades (
  event_id    STRING,
  account_id  STRING,
  ticker      STRING,
  side        STRING,
  quantity    INT,
  price       DOUBLE,
  trade_time  BIGINT,            -- epoch millis from the datagen
  proc_time   AS PROCTIME()      -- ← required for the FOR SYSTEM_TIME AS OF clause
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'topic.in',
  'properties.bootstrap.servers' = 'workshop-kafka:9093',
  'properties.group.id'          = 'sql-s14-01',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

-- ── (2) The lookup join itself ─────────────────────────────────────────────
-- Anatomy:
--
--   FROM kafka_trades t                                        ← left/probe side
--   LEFT JOIN fluss.workshop.instrument_master                  ← right/build side
--        FOR SYSTEM_TIME AS OF t.proc_time AS im                ← "lookup as of NOW"
--        ON t.ticker = im.ticker                                ← join predicate
--
-- The planner translates this into a `LookupJoin` operator. For every probe
-- record it issues one point lookup against Fluss using the join key. The
-- result row contains every column from `kafka_trades` plus every column
-- from `instrument_master` — with NULLs on the right when no match exists
-- (that's what LEFT JOIN buys us; an INNER JOIN would drop those rows).
--
-- A caveat about the demo data: the trade datagen picks tickers from a
-- small alphabetic universe (AAPL, GS, …) while `instrument_master` is
-- seeded with ISINs as PKs and tickers as a non-key column. The Fluss
-- lookup-join optimiser is happiest when the join key IS the PK because
-- that turns into an indexed lookup; joining on a non-PK column makes
-- Fluss fall back to a less efficient lookup path (or, in production,
-- you'd add a secondary index — out of scope here).
--
-- For the purposes of this teaching step the join still works; you'll just
-- see many rows where the right side is NULL because the synthetic
-- `ticker` doesn't line up with anything seeded into instrument_master.
-- The point is the *shape* of the query and the absence of state, not
-- the hit rate.
--
-- In a real workshop deployment you'd typically:
--   • Either change the join key to ISIN on both sides (matches the PK),
--   • Or seed `instrument_master` with the same ticker universe the
--     datagen uses (we do a friendly compromise here so the teaching
--     example reads cleanly).
SELECT
  t.event_id,
  t.ticker,
  t.quantity,
  im.sector,
  im.last_close,
  im.beta_1y
FROM kafka_trades t
LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im
  ON t.ticker = im.ticker;

-- ── (3) What to observe in the SQL Client ──────────────────────────────────
-- • The query runs continuously; one output row per incoming trade.
-- • The right-side columns (sector, last_close, beta_1y) will mostly be
--   NULL because of the ticker mismatch noted above. That's expected for
--   this synthetic demo; the *structure* of the join is what we're showing.
-- • Cancel the query, then check the job graph in the Flink UI. You'll see
--   ONE source (Kafka) and ONE lookup-join operator. There is NO source
--   for `instrument_master` — Fluss is being called point-wise, not read
--   as a stream.
--
-- ── (4) `EXPLAIN` the same query (optional, do this in the SQL Client) ────
-- EXPLAIN
-- SELECT t.event_id, t.ticker, im.sector, im.last_close
-- FROM kafka_trades t
-- LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im
--   ON t.ticker = im.ticker;
--
-- Look for `LookupJoin(table=[fluss.workshop.instrument_master], lookup=[ticker=...], joinType=[LeftOuterJoin])`.
-- That's the lookup-join operator; no state on the right side.
--
-- ── Recap ──────────────────────────────────────────────────────────────────
-- One-line summary: lookup joins against Fluss PK tables are STATE-FREE on
-- the right side, served by Fluss's tablet servers as point lookups. This
-- is the idiom Fluss is designed around — and the foundation for the
-- multi-side join in step 02 and the projection-pushdown win in step 03.
