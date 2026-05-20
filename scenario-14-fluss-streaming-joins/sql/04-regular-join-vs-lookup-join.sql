-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 14 — Step 04: Regular join vs lookup join — the state cost
-- ════════════════════════════════════════════════════════════════════════════
--
-- THIS IS THE HEADLINE TEACHING MOMENT of scenario 14.
--
-- Steps 01–03 used `FOR SYSTEM_TIME AS OF t.proc_time` — that syntax tells
-- the planner "this is a LOOKUP join". The right side is queried point-wise
-- per probe. There is NO in-Flink state for the right side; Fluss serves
-- the lookups.
--
-- A regular SQL `JOIN ... ON ...` over the same right side compiles into
-- a completely different operator. Both sides become continuous streams.
-- Flink holds BOTH SIDES of the join in operator state so that any future
-- record on either side can be matched against everything already seen on
-- the other side.
--
-- For this teaching step we'll express the SAME logical enrichment —
-- trades enriched with instrument reference data — as a regular join, by
-- making `instrument_master` a streaming source (reading its changelog).
-- Compare the EXPLAIN, the state shape, and the failure modes.
--
-- TL;DR table:
--
--   shape                  lookup join (steps 01–03)   regular join (this step)
--   ─────                  ─────────────────────────   ────────────────────────
--   right-side scan        none (point lookups)        full changelog stream
--   right-side state       ZERO                        every row, indefinitely
--   left-side state        none                        every row that hasn't
--                                                       matched yet (and even
--                                                       after — for late updates)
--   semantics              as-of NOW(); right side     full bi-temporal match
--                          is point-in-time
--   throughput             bounded by Fluss lookup     bounded by state I/O
--                          fanout                       and shuffle
--   correctness on update  next lookup sees the new    a row already emitted
--                          row                          does NOT retroactively
--                                                       update (without a
--                                                       retract stream + sink)

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);

-- ── (1) Kafka source for trades, exactly as before ─────────────────────────
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
  'properties.group.id'          = 'sql-s14-04',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

-- ── (2) Regular streaming join over the Fluss changelog ────────────────────
-- The crucial change vs steps 01–03: NO `FOR SYSTEM_TIME AS OF` clause.
--
-- Plain `JOIN` over a Fluss PK table means Flink reads the table AS A
-- STREAM. For PK tables, Fluss generates a changelog stream of +I/-U/+U/-D
-- events. The Flink planner sees this as a regular changelog source — the
-- right side is a continuous stream, not a lookup target.
--
-- Operator shape:
--   LeftSource(kafka_trades)       ──┐
--                                    ├── Join(joinType=[InnerJoin],
--   RightSource(instrument_master) ──┘                       state=[BothSides])
--
-- Both sides feed into a `Join` operator that maintains hash tables on
-- BOTH inputs. Every new trade probes the right hash table for a match;
-- every new instrument update probes the LEFT hash table for any trades
-- already buffered that should now (re-)match.
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

-- ── (3) EXPLAIN the regular join ───────────────────────────────────────────
-- EXPLAIN
-- SELECT t.event_id, t.ticker, im.sector
-- FROM kafka_trades t
-- JOIN fluss.workshop.instrument_master AS im
--   ON t.ticker = im.ticker;
--
-- Look for:
--   • A `TableSourceScan(table=[fluss.workshop.instrument_master], ...)`
--     stage — the right side is being SCANNED as a stream, not looked up
--     per probe. (Compare with step 01's EXPLAIN where there was no scan
--     on `instrument_master`.)
--   • A `Join(joinType=[InnerJoin], ..., leftInputSpec=..., rightInputSpec=...)`
--     operator with metadata that indicates both inputs feed state.
--   • Depending on Flink version, you may also see explicit
--     `state=[(left, right)]` / `state=[BothSides]` annotations on the
--     Join — that's the dead giveaway you're paying for state on both sides.
--
-- The shape change is dramatic vs the lookup-join EXPLAINs in steps 01–03.
--
-- ── (4) Why this matters operationally ─────────────────────────────────────
-- The state size of this regular join is approximately:
--
--   state ≈ |trades held until first match| × bytes_per_trade
--         + |all instrument_master rows seen| × bytes_per_instrument_row
--
-- The right side grows MONOTONICALLY because Fluss continuously emits the
-- changelog as instruments are updated. The left side holds onto trades
-- whose `ticker` hasn't yet matched any instrument seen — and even after
-- a match, if you want a future update to the instrument to retroactively
-- revise the join output, Flink keeps the trade row in state too (so the
-- recompute can happen). That's "BothSides" state.
--
-- Contrast with the lookup-join idiom from step 01–03:
--   • Right side: ZERO state. Fluss is queried point-wise.
--   • Left side: zero state (each probe is processed and emitted).
--   • Total state for the enrichment: ZERO.
--
-- Yes, the regular join is more *expressively* powerful — it can
-- retroactively revise outputs when the dim changes. But for the vast
-- majority of enrichment use-cases ("attach the current sector to each
-- trade and ship it") you do NOT need that power, and you pay for it in
-- state size + checkpoint size + recovery time + RocksDB I/O on every
-- write.
--
-- ── (5) When you DO want a regular join over a Fluss table ─────────────────
-- It's the right tool when:
--   • The semantics genuinely require bitemporal correctness — a late
--     update to the dim should retroactively change outputs that have
--     already been published, and your downstream is retract-aware.
--   • Both sides are roughly the same size and you need full join over
--     their changelogs (e.g. consolidating two PK tables into one
--     materialised view).
--
-- It's the WRONG tool when:
--   • You just want to enrich a high-rate event stream with a slow-moving
--     dim table — use a lookup join, every time.
--   • Your downstream is append-only and can't tolerate retractions.
--   • State budget is constrained.
--
-- ── (6) The "delta join" optimisation — sneak peek ─────────────────────────
-- Flink 2.2 + Fluss 0.8+ introduced an optimisation that lets the planner
-- compile this EXACT query (a regular join over a Fluss changelog) into
-- a DeltaJoin operator that uses Fluss's PK index as the state. The
-- resulting plan has the *expressiveness* of a regular join (it retracts
-- on updates) but the *state cost* of a lookup join (zero on the right).
--
-- We can't run that here — the workshop is on Flink 1.20.4 — but step 05
-- walks through the concept and shows the EXPLAIN diff you'd expect on
-- a Flink 2.2 cluster.
--
-- ── Recap ──────────────────────────────────────────────────────────────────
-- A regular join over a Fluss PK table is correct, but it pays for the
-- right side in Flink operator state. The lookup-join idiom from steps
-- 01–03 gets the same enrichment at zero in-Flink state because Fluss
-- serves the lookups directly. Pick the join shape that matches the
-- semantics you actually need — most of the time that's "lookup".
