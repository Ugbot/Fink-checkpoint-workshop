-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 14 — Step 05: Delta join — the concept (read-only walkthrough)
-- ════════════════════════════════════════════════════════════════════════════
--
-- This file is a CONCEPT WALKTHROUGH, not a runnable upgrade. The workshop
-- cluster runs Flink 1.20.4 and Fluss 0.9.1; the delta-join optimisation
-- requires Flink 2.2+ with Fluss 0.8+. Submitting this file to the SQL
-- Client will run the regular join (same as step 04) — the OPTIMISATION we
-- discuss here only fires when the planner is a 2.2-era one. The SQL itself
-- is unchanged; the planner does the work.
--
-- Reference (release announcement):
--   https://flink.apache.org/2025/12/04/apache-flink-2.2.0-advancing-real-time-data--ai-and-empowering-stream-processing-for-the-ai-era/
--
-- ── What is a "delta join"? ────────────────────────────────────────────────
--
-- A delta join is a planner optimisation that takes a *regular* streaming
-- JOIN whose right side is a Fluss PK table and compiles it into an
-- operator that:
--
--   1. Reads the LEFT input as a regular stream (same as today).
--   2. Reads the RIGHT input's CHANGELOG as a regular stream (same as today).
--   3. Instead of holding either side in operator state, the join uses the
--      Fluss PK table itself as the state, by issuing PREFIX-KEY LOOKUPS
--      against Fluss when it needs to resolve a match.
--
-- The result has the same semantics as the regular join from step 04 —
-- including the ability to RETROACTIVELY REVISE join outputs when the
-- dim side updates — but the operator state shrinks by ~99% because the
-- right side state (which is the dominant cost in practice) is now
-- externalised to Fluss.
--
-- The "delta" name comes from how the optimisation works internally:
-- the join processes the *delta* (changelog) of the right side, not the
-- materialised state, and uses Fluss's index for the lookups.
--
-- ── Why doesn't a lookup join already give you this? ───────────────────────
--
-- A lookup join (steps 01–03) is "as-of NOW" on the right side. It does
-- NOT retract — once a row is emitted with the join match it had at
-- lookup time, that emission is final. If the right side updates later,
-- the previously-emitted row is NOT revised.
--
-- A regular join (step 04) DOES retract on right-side updates — but pays
-- for that with state on both sides.
--
-- A delta join (this step, Flink 2.2) is the missing third option: it
-- retracts like a regular join (because it processes the right-side
-- changelog) but holds state like a lookup join (because it uses Fluss
-- as the lookup index instead of operator memory).
--
-- ── The SQL is unchanged ───────────────────────────────────────────────────
-- Notice this is byte-for-byte the SAME SQL as step 04. The optimisation
-- is entirely planner-side; users don't write a new clause.
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
  'properties.group.id'          = 'sql-s14-05',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

-- The query: a regular streaming join. SAME as step 04. On a Flink 1.20
-- cluster this compiles to a `Join(state=[BothSides], ...)` operator.
-- On a Flink 2.2 cluster with Fluss 0.8+, the planner is allowed to
-- rewrite it into a `DeltaJoin(state=[None], lookupTable=[...], ...)`.
SELECT
  t.event_id,
  t.ticker,
  t.quantity,
  im.sector,
  im.last_close,
  im.beta_1y
FROM kafka_trades t
JOIN fluss.workshop.instrument_master AS im
  ON t.ticker = im.ticker;

-- ── Expected EXPLAIN diff ──────────────────────────────────────────────────
--
-- TODAY on Flink 1.20.4 (this workshop) — pseudo-EXPLAIN of the Join stage:
--
--   == Optimized Physical Plan ==
--     Sink(table=[<your sink>])
--       Calc(select=[event_id, ticker, quantity, sector, last_close, beta_1y])
--         Join(joinType=[InnerJoin],
--              where=[(ticker = ticker0)],
--              select=[event_id, ticker, quantity, ticker0, sector, last_close, beta_1y],
--              leftInputSpec=[NoUniqueKey],
--              rightInputSpec=[HasUniqueKey],
--              state=[BothSides])               ← state cost lives here
--           Exchange(distribution=[hash[ticker]])
--             TableSourceScan(table=[[default_catalog, default_database, kafka_trades]])
--           Exchange(distribution=[hash[ticker]])
--             TableSourceScan(table=[[fluss, workshop, instrument_master]],
--                             changelog=[changelog])     ← right side scanned
--
--   Operator state at steady state on a 1M-row instrument_master:
--     ~ |instrument_master| × row_bytes  ≈ a few GB of RocksDB state.
--     Checkpoints grow proportionally.
--
-- WITH DELTA JOIN on Flink 2.2 + Fluss 0.8+ — pseudo-EXPLAIN of the Join stage:
--
--   == Optimized Physical Plan ==
--     Sink(table=[<your sink>])
--       Calc(select=[event_id, ticker, quantity, sector, last_close, beta_1y])
--         DeltaJoin(joinType=[InnerJoin],
--                   where=[(ticker = ticker0)],
--                   select=[event_id, ticker, quantity, sector, last_close, beta_1y],
--                   lookupTable=[fluss.workshop.instrument_master],
--                   lookupKey=[ticker],
--                   state=[None])                ← state cost goes to ZERO
--           TableSourceScan(table=[[default_catalog, default_database, kafka_trades]])
--           TableSourceScan(table=[[fluss, workshop, instrument_master]],
--                           changelog=[delta])    ← processes right delta,
--                                                   does not buffer it
--
--   Operator state at steady state: effectively zero (a small per-key
--     book-keeping buffer; no copy of the right-side rows).
--   Checkpoints: orders of magnitude smaller.
--   Right-side lookups: served from Fluss's PK index, same path as the
--     lookup-join operator in steps 01–03.
--
-- The header `Join(...)` ⇒ `DeltaJoin(...)` swap and the `state=[BothSides]`
-- ⇒ `state=[None]` annotation are the two pieces of evidence you'd look
-- for in a real EXPLAIN to confirm the optimisation fired.
--
-- ── When the planner can / can't pick DeltaJoin ────────────────────────────
-- Even on Flink 2.2, the optimiser only chooses DeltaJoin when:
--   • The right-side table is a Fluss PK table (or a Fluss table with a
--     compatible secondary index).
--   • The join predicate is a prefix-key equality on that table's PK.
--   • The right side is used in changelog mode (otherwise lookup join is
--     more efficient outright).
--
-- If your join is on a non-key column, or against a non-Fluss source, the
-- planner falls back to the regular join from step 04. The cluster log
-- and EXPLAIN output will say so.
--
-- ── Why we teach this even though we can't run it ──────────────────────────
-- Two reasons:
--
--   1. Knowing about DeltaJoin changes the way you DESIGN tables today.
--      If you're putting reference data into Fluss with the right PK and
--      a sensible bucket layout, your jobs become eligible for the
--      optimisation the moment you upgrade Flink. If you stuff that data
--      into Kafka compacted topics or Postgres, you'll never get this.
--      The data-modelling decision is what locks in your future
--      performance.
--
--   2. It's the operational answer to "we have hundreds of streaming
--      enrichment jobs with multi-GB join state — how do we get rid of
--      that state?" Today the answer is "use lookup joins where
--      semantics permit". Soon, the answer can also be "leave the SQL
--      alone and let the planner do it for you, by being on Flink 2.2+
--      and putting dims in Fluss."
--
-- ── Recap of scenario 14 ───────────────────────────────────────────────────
--
--   Step 01 — lookup join (one Fluss side)            zero state, fast
--   Step 02 — lookup join (two Fluss + one JDBC)      composes
--   Step 03 — projection pushdown into lookups        narrower lookups
--   Step 04 — regular join over the Fluss changelog   correct, costs state
--   Step 05 — delta join (concept; Flink 2.2+)        correct AND zero state
--
-- The Fluss design choice that makes all of this possible: PK tables are
-- both a queryable current-state view AND a changelog stream, both served
-- by the same storage. Everything else is the planner picking the right
-- access path for your query.
