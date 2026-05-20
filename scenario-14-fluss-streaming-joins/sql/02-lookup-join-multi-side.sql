-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 14 — Step 02: Lookup join against multiple sides at once
--                        (two Fluss PK tables + one Postgres dim)
-- ════════════════════════════════════════════════════════════════════════════
--
-- Step 01 enriched trades from ONE PK table. Real enrichment pipelines need
-- to add columns from several reference tables in one shot. The lookup-join
-- pattern composes: chain as many `LEFT JOIN ... FOR SYSTEM_TIME AS OF ...`
-- clauses as you have dim sides.
--
-- We'll show three sides at once:
--
--   1. fluss.workshop.instrument_master  — instrument reference (Fluss PK)
--   2. fluss.workshop.account_profile     — account reference   (Fluss PK)
--   3. workshop.accounts (Postgres JDBC)  — a slow-changing account dim
--      held in the operational database (the same one scenarios 08–09 use)
--
-- The point of including Postgres is to make the contrast visible: both
-- connector types support `FOR SYSTEM_TIME AS OF`, but they take very
-- different paths to serve the lookup. Fluss does it natively through its
-- tablet servers (a single network hop, no caching required); the JDBC
-- connector goes over a SQL connection and *has* to be told to cache or
-- it'll hammer the database on every probe.
--
-- Compare side-by-side:
--
--   Fluss lookup                   JDBC (Postgres) lookup
--   ────────────                   ──────────────────────
--   point lookup by PK on the      SELECT … WHERE pk = ?
--   Fluss tablet server            (round-trip over JDBC)
--
--   no client-side cache needed    needs 'lookup.cache' = 'PARTIAL'
--                                  (or 'FULL') or it scales linearly with
--                                  probe rate
--
--   typed columns, projection      typed columns, projection pushdown
--   pushdown (see step 03)         depends on driver/connector version
--
--   one extra connection to the    one extra Postgres connection per
--   Fluss coordinator              subtask (pool tuning matters)
--
-- The teaching takeaway: lookup joins compose cleanly across connectors,
-- but the COST PROFILE on each side differs. Fluss is the path of least
-- resistance for high-fanout enrichment.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);

-- ── (1) Kafka source for trades — identical to step 01 ─────────────────────
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
  'properties.group.id'          = 'sql-s14-02',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

-- ── (2) Postgres `accounts` table as a JDBC lookup source ─────────────────
-- The `accounts` table was created by /scripts/init-postgres.sql and seeded
-- by /scripts/seed-accounts.sh (or AccountSeedJob). Columns:
--   account_id (PK), account_name, tier, region, updated_at.
--
-- The lookup cache options are the *important* part of this DDL:
--   • 'lookup.cache' = 'PARTIAL'                 — enable per-key caching
--   • 'lookup.partial-cache.max-rows' = '1000'   — hold up to 1k rows
--   • 'lookup.partial-cache.expire-after-write' = '5 min'
--                                                — TTL after each load
--
-- Without these, the JDBC connector would issue one SELECT per probe row.
-- With them, common keys are served from operator-local memory and only
-- cache misses (and TTL expirations) hit Postgres.
--
-- Compare with the Fluss sides below: there is no `lookup.cache` option
-- on those — Fluss's own client serves the lookup. You *can* enable
-- client-side caching on Fluss sides for ultra-hot keys; we keep it off
-- here to keep the comparison clean.
CREATE TEMPORARY TABLE pg_accounts (
  account_id   STRING,
  account_name STRING,
  tier         STRING,
  region       STRING,
  PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
  'connector'  = 'jdbc',
  'url'        = 'jdbc:postgresql://workshop-postgres:5432/workshop',
  'table-name' = 'accounts',
  'username'   = 'workshop',
  'password'   = 'workshop',
  'lookup.cache'                              = 'PARTIAL',
  'lookup.partial-cache.max-rows'             = '1000',
  'lookup.partial-cache.expire-after-write'   = '5 min'
);

-- ── (3) The three-sided lookup join ────────────────────────────────────────
-- Read this query top to bottom:
--   • `t` is the probe stream from Kafka.
--   • `im` is the instrument reference (Fluss PK), joined on ticker.
--   • `ap` is the Fluss account reference, joined on account_id.
--   • `pg` is the Postgres account dim, joined on account_id.
--
-- Each FOR SYSTEM_TIME AS OF t.proc_time clause makes that side a lookup
-- (NOT a regular join). The planner emits one LookupJoin operator per
-- side. All three lookups happen in the same subtask, in pipeline order;
-- a single output row carries the columns from all four tables.
--
-- Mental model for the runtime cost per trade:
--   1 Kafka deserialize
-- + 1 Fluss point lookup (instrument)
-- + 1 Fluss point lookup (account)
-- + 1 cached / JDBC point lookup (Postgres)
-- = 1 output row.
--
-- No state on any of the right sides. The only operator state is whatever
-- the lookup caches choose to keep (here: ~1000 rows in the JDBC cache).
SELECT
  t.event_id,
  t.account_id,
  t.ticker,
  im.sector,
  im.last_close,
  ap.tier         AS fluss_tier,
  ap.risk_score   AS fluss_risk_score,
  pg.tier         AS pg_tier,
  pg.region       AS pg_region
FROM kafka_trades t
LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im
       ON t.ticker     = im.ticker
LEFT JOIN fluss.workshop.account_profile   FOR SYSTEM_TIME AS OF t.proc_time AS ap
       ON t.account_id = ap.account_id
LEFT JOIN pg_accounts                       FOR SYSTEM_TIME AS OF t.proc_time AS pg
       ON t.account_id = pg.account_id;

-- ── (4) EXPLAIN the same query ─────────────────────────────────────────────
-- Run this against the SQL Client to inspect the operator tree:
--
-- EXPLAIN
-- SELECT t.event_id, im.sector, ap.tier, ap.risk_score, pg.region
-- FROM kafka_trades t
-- LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im ON t.ticker = im.ticker
-- LEFT JOIN fluss.workshop.account_profile   FOR SYSTEM_TIME AS OF t.proc_time AS ap ON t.account_id = ap.account_id
-- LEFT JOIN pg_accounts                       FOR SYSTEM_TIME AS OF t.proc_time AS pg ON t.account_id = pg.account_id;
--
-- Things to look for in the output:
--   • Three separate `LookupJoin(...)` operators, chained.
--   • Each has `joinType=[LeftOuterJoin]` and a `lookup=[<key>=...]` predicate.
--   • For the two Fluss sides, `table=[fluss.workshop.instrument_master]`
--     and `table=[fluss.workshop.account_profile]` — Flink resolves them
--     through the Fluss catalog and the lookup goes through the Fluss
--     connector (NOT through JDBC).
--   • For the Postgres side, `table=[default_catalog.default_database.pg_accounts]`
--     with the JDBC connector, plus `select=[...]` indicating the columns
--     fetched per probe.
--   • CRUCIALLY: no `Exchange` or `HashAggregate` or `Join(state=...)`
--     stages on the right side of any of these joins. That's the absence
--     of state we keep talking about.
--
-- ── (5) When to add caching to the Fluss sides ─────────────────────────────
-- The Fluss connector exposes `lookup.cache` too. Turn it on if:
--   • Your probe stream is heavily skewed (a few hot keys dominate).
--   • Your Fluss cluster is rate-limited or you want to reduce its load.
-- Leave it off if:
--   • You need every lookup to see the absolute latest value of the PK row
--     (caches obviously trade staleness for throughput).
--
-- The defaults are designed so the lookup-join is correct without caching;
-- caches are an optimisation, not a requirement.
--
-- ── Recap ──────────────────────────────────────────────────────────────────
-- Lookup joins compose across connectors. Two Fluss sides + one Postgres
-- side, one EXPLAIN plan, zero in-Flink state for the right sides. The
-- next step (03) shows how the planner pushes column projections all the
-- way into each lookup so we don't pay the I/O cost of fetching unused
-- columns.
