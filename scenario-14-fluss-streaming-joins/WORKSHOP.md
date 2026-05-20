# Scenario 14 Workshop — Fluss Streaming & Lookup Joins

> **Audience:** anyone who has done scenarios 11–13. You know what a Fluss
> catalog is, what a PK table is, and have run at least one streaming
> SELECT against the cluster.
>
> **Time:** ~50 minutes if you read every comment and reflect at the
> "aha" questions. ~20 minutes for the condensed path (steps 01 → 03 → 04).
>
> **Format:** SQL Client paste-along. Each step is one `.sql` file.

---

## What you'll be able to do after this scenario

1. Write the `LEFT JOIN <fluss-pk-table> FOR SYSTEM_TIME AS OF <proctime>`
   lookup-join idiom from memory and explain why it's state-free on the
   right side.
2. Compose lookup joins across multiple connectors (Fluss + JDBC) in a
   single query and configure `lookup.cache` correctly for each side.
3. Read a Flink `EXPLAIN` plan and identify whether the planner picked
   `LookupJoin`, regular `Join`, or `DeltaJoin` — and what each one
   means for state and correctness.
4. Decide, for a given enrichment requirement, whether you want a lookup
   join, a regular join, or to wait for the cluster to be upgraded to
   the version that gives you delta joins for free.

---

## Mental model — read this first

There are three streaming-join shapes you can build against a Fluss PK
table. They all return the same logical answer for a steady state, but
they have wildly different state costs and update semantics.

| Shape | Syntax | Right-side state in Flink | Retracts on right-side updates? |
|---|---|---|---|
| **Lookup join** | `LEFT JOIN T FOR SYSTEM_TIME AS OF p AS t ON ...` | None — point lookups against Fluss | No (as-of NOW; once emitted, final) |
| **Regular join** | `LEFT JOIN T AS t ON ...` | Holds every row of `T` ever seen | Yes (revises previously emitted rows) |
| **Delta join** (Flink 2.2+) | Same syntax as regular join; planner rewrites | None — uses Fluss's PK index as state | Yes |

This scenario lives inside that 2×2 of "do I pay for state?" × "do I get
retractions?". The lookup join is the easy win — most enrichment doesn't
need retraction, so why pay for it? The regular join is the correct tool
when retraction semantics genuinely matter. The delta join is the future:
you get retraction *without* the state bill, because the right side is
externalised to Fluss's index.

---

## Prerequisites

- Workshop infra up: `podman-compose up -d --build`
- Fluss tables created: `bash scripts/init-fluss.sh`
- Fluss tables seeded: `bash scripts/seed-fluss.sh`
- Postgres `accounts` table seeded: `bash scripts/seed-accounts.sh`
- Trade datagen running: `./quickstart.sh 14`

The trade datagen writes JSON events to Kafka `topic.in` continuously.
Every SQL file here creates a fresh `kafka_trades` temporary table over
that topic, so the runs are independent.

---

## Step 01 — Basic lookup join (one Fluss side)

### Concept
The lookup-join idiom. `LEFT JOIN fluss.workshop.instrument_master
FOR SYSTEM_TIME AS OF t.proc_time AS im ON t.ticker = im.ticker` tells
the planner "for each probe row, look up the right side as it exists
NOW and join". The right side is served by Fluss's tablet servers as
point lookups; Flink holds NO state for it.

### Story
A trading platform receives 50k trades per second. Each trade needs the
sector, last close and beta of its instrument attached before it goes
downstream to risk and P&L. Pre-Fluss, the team materialised the
instrument table into Flink state with a `KeyedCoProcessFunction` —
working but holding ~120MB per task slot at steady state, with a 90s
checkpoint cost. With the lookup-join idiom this is one `JOIN ... FOR
SYSTEM_TIME AS OF` clause and the operator state is empty.

### The SQL
```sql
SELECT t.event_id, t.ticker, im.sector, im.last_close, im.beta_1y
FROM kafka_trades t
LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im
  ON t.ticker = im.ticker;
```

### Observe
- The query runs continuously, one output row per incoming trade.
- The Flink job graph shows ONE source (Kafka) and one lookup-join
  operator. There is NO source for `instrument_master`.
- In the demo dataset most rows return NULL for the right side because
  the synthetic ticker universe is small and doesn't overlap heavily
  with the seeded instrument keys. The *shape* of the join is the
  teaching point, not the hit rate.

### Aha
> *Why use `PROCTIME()` here rather than the trade's own `trade_time` event-time column?*

`FOR SYSTEM_TIME AS OF` against a Fluss PK table is a **point-in-time
lookup**, not a temporal/event-time join. We want "look up the instrument
as it exists when this trade is processed". Using `PROCTIME()` makes the
"as-of" boundary the operator's wall-clock — which for a continuously-
updated PK table means "whatever the latest value is right now". For
event-time temporal joins, you'd use a versioned table (scenario 08); the
Fluss lookup-join path is the simpler version that handles current-state
enrichment.

### Pitfall to remember
Lookup joins do NOT retract. Once a row is emitted with the right-side
match it had at lookup time, that emission is final. If the instrument's
sector changes later, the previously-emitted row keeps the old sector.
For enrichment use-cases that's exactly what you want; for "we ship a
view that must always reflect the latest sector", step 04 (regular join)
is the right tool.

---

## Step 02 — Multi-side lookup join (two Fluss + one Postgres)

### Concept
The lookup-join idiom composes. Chain `LEFT JOIN ... FOR SYSTEM_TIME AS
OF ...` clauses for every side you want to attach. The planner emits one
`LookupJoin` operator per side and pipes them in sequence. Each side can
be a different connector — Fluss tables and Postgres-via-JDBC sit next
to each other comfortably.

### Story
The risk team wants every trade enriched with: instrument reference
(`sector`, `last_close`), account reference (`tier`, `risk_score`) and
the regional desk assignment (`region`) that lives in the operational
Postgres database. Three sources, one query, one job. Without lookup
joins you'd be running three separate enrichment microservices and
joining their outputs with timing assumptions you can't enforce.

### The SQL (shape)
```sql
LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im ON ...
LEFT JOIN fluss.workshop.account_profile   FOR SYSTEM_TIME AS OF t.proc_time AS ap ON ...
LEFT JOIN pg_accounts                       FOR SYSTEM_TIME AS OF t.proc_time AS pg ON ...
```

### Observe
- `EXPLAIN` shows three chained `LookupJoin` operators.
- The Fluss sides have `table=[fluss.workshop.instrument_master]` / `...account_profile]`.
- The Postgres side has `table=[default_catalog.default_database.pg_accounts]` and goes through the JDBC connector.
- The JDBC table has `lookup.cache = 'PARTIAL'` set; the Fluss sides
  don't need a client-side cache to be reasonable.

### Aha
> *Why does the JDBC side need a `lookup.cache` option while the Fluss sides don't?*

The JDBC connector serves lookups by opening a SQL connection and
issuing `SELECT … WHERE pk = ?` per probe. Without caching, that's one
network round-trip + one Postgres planner pass per event — fine for low
rates, catastrophic at production volumes. The Fluss connector serves
lookups through Fluss's own protocol against tablet servers that hold a
PK index; the path is short enough that the default unbounded behaviour
is OK for most workloads. You CAN turn on Fluss-side caching for extra
wins on hot keys; we leave it off here for clarity.

### Pitfall to remember
A multi-side join is only as fast as its slowest side. If the Postgres
JDBC lookup has high tail latency (no cache, slow query), the entire
LookupJoin pipeline stalls per probe waiting on it. Always check
`lookup.cache` and consider whether the dim belongs in Fluss instead.

---

## Step 03 — Projection pushdown into lookups

### Concept
`instrument_master` is 45 columns wide; `account_profile` is 30. A
`SELECT *`-style join would fetch all of them on every lookup even if
you used 2. Projection pushdown means the planner figures out the
columns each side *contributes* to the result and tells the Fluss
connector to only fetch those.

### Story
The same risk pipeline as step 02, but with a narrower output schema —
just `sector`, `last_close`, `tier`, `risk_score` plus the trade fields.
A naive port from a less optimising engine would read all 75 columns
across the two PK tables; with projection pushdown, the lookup payload
shrinks to 4 columns + the join keys. That's a 10× bandwidth reduction
between the Flink operators and the Fluss tablet servers, and a
proportional reduction in deserialisation cost.

### Observe
- `EXPLAIN` shows a `select=[...]` clause inside each `LookupJoin`
  listing only the columns actually used.
- Compare with step 02's `EXPLAIN`: same operator shape, but the
  `select=[...]` lists are necessarily different because the projection
  is different.
- The operator I/O metrics in the Flink UI (bytes-out from the source
  scan path) reflect the difference.

### Aha
> *I added a `WHERE im.sector = 'TECH'` to the query — did that push down too?*

Projection pushdown handles SELECT-list columns. Filter pushdown is a
separate optimisation that pushes WHERE predicates into the source. For
lookup joins, the predicate on the right side typically becomes part of
the lookup itself (it's `=`-shaped on the PK), or a post-filter
immediately after the lookup. Check the `EXPLAIN` — you'll see whether
the planner integrated the predicate or left it as a separate filter
step. Equality filters on indexed columns push down cleanly; range
filters less so.

### Pitfall to remember
The planner can only push down what it can statically prove unused.
`SELECT *` from a view that wraps the join blocks pushdown — the
planner doesn't know which columns the outer query needs. If you find a
job paying for wide reads, look for `SELECT *` somewhere in the pipeline
and narrow it.

---

## Step 04 — Regular join vs lookup join

### Concept
Drop the `FOR SYSTEM_TIME AS OF` clause and the same `JOIN ON` becomes
a *regular* streaming join. The planner reads the Fluss table as its
changelog stream and feeds it into a `Join` operator that holds **both
sides** in operator state. Right-side updates retract and re-emit; the
query is bitemporally correct.

The cost is state. For an `instrument_master` with N rows and an
average row of B bytes, the right-side state alone is ~N × B at steady
state and grows linearly with the table. Checkpoint sizes follow.

### Story
A pricing team builds the regular-join shape because they assumed
"streaming join" meant the same thing in Flink that it means in
Postgres CTE-land. The job runs fine at 50 instruments in dev. In
production with 250k instruments, the checkpoint balloons to 8GB, the
checkpoint duration crosses the configured 90s timeout, and the job
starts taking failures. Switching the query to a lookup join (steps
01–03) drops state to ~zero and checkpoints to seconds.

The headline lesson: a streaming `JOIN` in Flink is not free. The
default plan assumes you need bitemporal correctness; if you don't,
say so explicitly with `FOR SYSTEM_TIME AS OF`.

### The SQL (shape)
```sql
-- Same probe stream, same enrichment, no FOR SYSTEM_TIME AS OF:
SELECT t.event_id, t.ticker, im.sector, im.last_close, im.beta_1y
FROM kafka_trades t
JOIN fluss.workshop.instrument_master AS im
  ON t.ticker = im.ticker;
```

### Observe
- `EXPLAIN` now shows a `TableSourceScan(table=[[fluss, workshop,
  instrument_master]], changelog=[changelog])` stage — the right side
  is being scanned as a stream.
- The `Join(...)` operator has `state=[BothSides]` (or version-specific
  equivalents) — the dead giveaway you're holding state on both inputs.
- In the Flink UI, the join operator's managed-state metric grows
  monotonically until the table is fully seen.

### Aha
> *If the regular join is correct and the lookup join doesn't retract, isn't the regular join "better"?*

Better in *correctness*, but the question is what semantics your
downstream actually needs. Most enrichment outputs are append-only
shipped to systems that don't handle retractions (downstream Kafka
topics, blackhole sinks, analytics tables). For those, the lookup
join's "as-of NOW" semantics ARE what you want — and you save the
state cost. If your downstream is a materialised view that must always
reflect the latest dim, regular join (or delta join, when available)
is the right tool.

### Pitfall to remember
A regular join is "default streaming SQL behaviour" — the syntax that
*looks* simplest is the most expensive. If the operator team isn't
versed in Flink streaming semantics, they will write the regular-join
version and not realise they signed up for unbounded state until
production. Add a code-review rule: any `JOIN` against a Fluss/dim
table needs an explicit `FOR SYSTEM_TIME AS OF` or an explicit
justification comment.

---

## Step 05 — Delta join (concept; Flink 2.2+)

### Concept
A planner optimisation that compiles the step-04 regular-join SQL into
a `DeltaJoin` operator. The operator processes the right-side changelog
as a delta and uses Fluss's PK index as the join state — same lookup
machinery as the LookupJoin from steps 01–03. The result: regular-join
semantics (full retraction on right-side updates) at lookup-join state
cost (~zero).

The SQL is IDENTICAL to step 04. The optimisation is entirely planner-
side; you don't write a new clause. The only thing you do is run a
Flink 2.2+ cluster with Fluss 0.8+.

### Story
The pricing team from step 04's story finishes their lookup-join
rewrite. Six months later the requirement changes: an instrument's
sector reclassification must retroactively revise yesterday's already-
emitted trades. They can't use a lookup join anymore — they need
retraction. They can't go back to the regular join — they remember the
8GB checkpoint pain. Their cluster team upgrades to Flink 2.2 + Fluss
0.8. They redeploy with the regular-join SQL. The `EXPLAIN` now says
`DeltaJoin(state=[None], lookupTable=[fluss.workshop.instrument_master], ...)`
and the checkpoint stays small.

### Expected EXPLAIN diff

Today (Flink 1.20.4 — what's actually in this workshop):
```
Join(joinType=[InnerJoin],
     where=[(ticker = ticker0)],
     leftInputSpec=[NoUniqueKey],
     rightInputSpec=[HasUniqueKey],
     state=[BothSides])
  Exchange(distribution=[hash[ticker]])
    TableSourceScan(table=[[default_catalog, default_database, kafka_trades]])
  Exchange(distribution=[hash[ticker]])
    TableSourceScan(table=[[fluss, workshop, instrument_master]],
                    changelog=[changelog])
```

With delta join (Flink 2.2):
```
DeltaJoin(joinType=[InnerJoin],
          where=[(ticker = ticker0)],
          lookupTable=[fluss.workshop.instrument_master],
          lookupKey=[ticker],
          state=[None])
  TableSourceScan(table=[[default_catalog, default_database, kafka_trades]])
  TableSourceScan(table=[[fluss, workshop, instrument_master]],
                  changelog=[delta])
```

The two diff lines are the only operational change: `Join` → `DeltaJoin`
and `state=[BothSides]` → `state=[None]`. Same query, ~99% less state.

### Observe (on a future Flink 2.2 cluster)
- Submit the step-04 SQL on a 2.2 cluster and check `EXPLAIN` — you
  should see `DeltaJoin` for joins where the right side is a Fluss PK
  table and the join key is the PK (or a compatible prefix).
- For joins where the right side is anything else (Postgres, Paimon,
  Kafka), the planner falls back to the regular join.
- Operator state on the join shrinks to a small bookkeeping buffer.

### Aha
> *Does my SQL change at all to get this?*

No. That's the point. The optimisation is enabled by your *data layout*
(reference data in Fluss PK tables, the right keys) and your *cluster
version* (Flink 2.2 + Fluss 0.8+). Today's SQL works tomorrow
unchanged; you get the optimisation for free.

This is why teaching this scenario matters NOW even though we can't run
the optimisation — the decisions you make today about WHERE to put your
reference data determine whether you're eligible for the optimisation
the moment it lands.

### Pitfall to remember
The delta join only fires when (a) the right side is a Fluss table with
the right PK/index shape, (b) the join predicate matches that PK prefix,
and (c) the right side is used as a changelog stream. Joins on
non-key columns, against non-Fluss sources, or with non-equality
predicates fall back to the regular-join plan. Always confirm the
optimisation actually fired by reading `EXPLAIN` — don't assume.

Reference:
<https://flink.apache.org/2025/12/04/apache-flink-2.2.0-advancing-real-time-data--ai-and-empowering-stream-processing-for-the-ai-era/>

---

## Quick-reference cheat sheet

| You want… | Use… | Right-side state |
|---|---|---|
| Enrich a stream with current dim values (most enrichment) | Lookup join with `FOR SYSTEM_TIME AS OF` | None |
| Retraction-correct join over a Fluss changelog, Flink 1.20 | Regular `JOIN` | Full table |
| Retraction-correct join over a Fluss changelog, Flink 2.2+ | Regular `JOIN` (planner picks `DeltaJoin`) | None |
| Enrich from a Postgres dim | Lookup join + JDBC `lookup.cache` | None |
| Multi-side enrichment | Chain `LEFT JOIN ... FOR SYSTEM_TIME AS OF ...` per side | None per side |
| Narrow the lookup payload | Project only the columns you need (no `SELECT *`) | None |

---

## Production checklist

When you ship a Fluss-backed enrichment job to production:

- [ ] Every `JOIN` against a Fluss/dim table has an explicit `FOR
      SYSTEM_TIME AS OF` clause OR an in-code comment justifying why
      a regular join is required.
- [ ] `SELECT` lists are narrow; no `SELECT *` over wide PK tables.
- [ ] JDBC dim sides have `lookup.cache = 'PARTIAL'` with sensible
      `max-rows` and `expire-after-write` values.
- [ ] `EXPLAIN` has been read at deploy time; the operator types match
      the intended plan (`LookupJoin` vs `Join` vs `DeltaJoin`).
- [ ] Checkpoint size and operator state are monitored. A growing join
      state on a "lookup" job means the planner picked a regular join
      against your wishes — investigate immediately.
- [ ] Hot keys identified; consider Fluss-side `lookup.cache` if the
      key skew justifies it.

---

## Where these ideas re-appear

- **Scenario 08** (`scenario-08-stream-table-joins/`) — the original
  `FOR SYSTEM_TIME AS OF` against Postgres / Paimon. This scenario is
  the Fluss-native version of that one.
- **Scenario 09** (`scenario-09-multiway-join-recovery/`) — multi-way
  enrichment + crash recovery. Useful contrast for the "state matters"
  argument in step 04.
- **Scenario 12** (`scenario-12-fluss-wide-schemas/`) — projection
  pushdown for batch reads; step 03 here applies the same idea to the
  streaming lookup path.
- **Scenario 13** (`scenario-13-fluss-partial-updates/`) — multi-source
  partial updates into a single PK table. The PK tables used here as
  lookup sides are the same shape.
- **Scenario 15** (`scenario-15-fluss-java-clients/`) — point lookups
  from Java without Flink. The exact same Fluss read path that the
  lookup-join operator uses, exposed to application code.

---

## Glossary

- **Lookup join** — A streaming join shape where one side is queried
  point-wise per probe row. Triggered by `FOR SYSTEM_TIME AS OF`.
- **Regular join** — A streaming join shape where both sides are
  continuous streams and both sides are held in operator state.
- **Delta join** — A Flink 2.2+ planner optimisation that compiles a
  regular join over a Fluss-PK right side into an operator that uses
  Fluss's index as state. Same syntax as a regular join.
- **Projection pushdown** — The planner figuring out which columns are
  used and pushing the SELECT list into the source connector so unused
  columns are never read.
- **`FOR SYSTEM_TIME AS OF <proctime>`** — The SQL clause that tells
  the planner to compile a lookup join against the right side. In
  scenario 08 (versioned tables) the "as-of" is event-time; in this
  scenario against a Fluss PK table it's processing-time.
- **`lookup.cache`** — A JDBC connector option that caches lookup
  results in operator-local memory. Essential at production rates.
- **PK index** — The internal index Fluss tablet servers use to serve
  point lookups in O(log N). The same index the delta-join optimisation
  would use as state in Flink 2.2+.

---

## Next steps

- **Scenario 15** — implement the same point lookup directly from a
  Java client. Useful if you have non-Flink services that need the
  same enrichment data.
- **Scenario 16** — tiering Fluss tables to Paimon. The PK tables
  joined here are eligible for tiering; the lookup path stays the same
  for hot data while cold data lives in Parquet on object storage.
- Re-read step 04's `EXPLAIN` after you've internalised the lookup-join
  plan from step 03 — the difference is much more striking the second
  time.
