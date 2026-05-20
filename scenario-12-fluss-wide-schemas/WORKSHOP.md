# Scenario 12 Workshop — Fluss Wide Schemas & Pushdown

> **Audience:** anyone who finished scenario 11 and wants to know why the
> "schema" part of Fluss matters in practice.
> **Time:** ~50 minutes if you read every comment and run every EXPLAIN.
> **Format:** SQL Client paste-along. Six steps, one `.sql` file each.

---

## What you'll be able to do after this scenario

1. Read an EXPLAIN plan for a Fluss query and identify the
   `project=[...]` field on the TableSourceScan.
2. Predict which predicates a Fluss source will push down (PK / PK prefix)
   and which will run as a Flink Calc operator above the scan.
3. Quantify the bytes saved by projection pushdown on a wide table.
4. Write a batch query that joins Fluss + an external store (Postgres /
   MySQL / S3 / ...) in one statement.

---

## Prerequisites

- Scenario 11 completed end-to-end.
- Workshop infra up: `podman-compose up -d --build`
- Fluss seed running: `bash scripts/seed-fluss.sh`
- (For step 06) Postgres `accounts` table populated: this happens
  automatically once `AccountSeedJob` runs as part of compose-up, but you
  can re-run `bash scripts/seed-accounts.sh` if the table is empty.

---

## Step 01 — The 45-column instrument master

### Concept

`instrument_master` is the workshop's analogue of a real-world *security
master* table — the canonical reference dataset every trading-system
component reads to learn "what is this instrument and how does it behave?".
At 45 columns it spans seven domain groups: identity, classification,
trading rules, pricing snapshot, risk, fundamentals, and ESG. The table
exists in Fluss as a single PK table keyed on `isin`, bucketed 4 ways.

### Story

In production a security master is hit by every order-routing service,
every pricing screen, every risk engine. The Fluss replacement of the
typical "compacted-Kafka-topic-plus-Redis-cache" lets all of those
consumers point at the same lookupable streaming store.

### The SQL

```sql
DESCRIBE instrument_master;
SHOW CREATE TABLE instrument_master;
```

### Observe

- 45 rows in the DESCRIBE output. The last row is the PK marker.
- The WITH clause from SHOW CREATE TABLE includes `bucket.num = '4'`.
- Every column has a strong type — none are JSON / VARIANT / opaque blobs.

### Aha

> Why is `isin` the PK rather than `ticker`?

Because tickers collide across exchanges (`MSFT` on NASDAQ vs `MSFT` on
LSE wouldn't share a row). ISIN is globally unique by definition. The PK
choice is the *first* design call you make for a Fluss table — see step 04
for what it controls.

### Pitfall

DESCRIBE doesn't show table options. To see `bucket.num` and the merge
engine you need SHOW CREATE TABLE or look at the Fluss admin UI.

---

## Step 02 — Confirm the seed ran

### Concept

The teaching scenarios all assume `instrument_master` already has rows.
A simple COUNT verifies that and a 3-row LIMIT verifies that the data
looks plausible. The optional on-disk inspection sidebar lets curious
learners poke at where Fluss actually stores the data.

### Story

Most of "is my data infrastructure healthy?" comes down to "does this
table have rows and do they look right?". Knowing two quick SQL queries
answers that question for Fluss without the operator having to learn the
admin SDK.

### The SQL

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT COUNT(*) FROM instrument_master;
SELECT isin, ticker, exchange, sector, last_close FROM instrument_master LIMIT 3;
```

### Observe

- The count is in the low thousands (depends on how long the datagen ran).
- The sample rows have realistic mixes of currencies, sectors, exchanges.
- The on-disk sidebar lists buckets 0..3 each with `log/` and `kv/`
  directories — the WAL and the materialised PK index respectively.

### Aha

> Why is the COUNT not exact?

`seed-fluss.sh` is a *continuous* datagen — it keeps writing as long as
the workshop is running. The COUNT is a snapshot at one instant. For a
deterministic count you'd need to pause the seed or read from a fixed
offset.

### Pitfall

Running `SELECT COUNT(*)` without `SET 'execution.runtime-mode' = 'batch'`
returns a *streaming* count that updates forever and never terminates.

---

## Step 03 — Projection pushdown (the centerpiece)

### Concept

When the planner builds the source operator for a Fluss table, it inspects
the SELECT list + WHERE + GROUP BY and computes the column set the query
actually needs. That set goes to the Fluss source as a projection list;
the source reads only those bytes off disk.

### Story

A typical screen on a trading desk shows ~6 columns of instrument data.
A typical analytics query touches ~4. The full 45-column row is rarely
needed all at once. Projection pushdown is what makes "store the whole
thing in one wide table" cheaper than "split it into 7 narrow tables and
join on demand".

### The SQL

```sql
EXPLAIN SELECT * FROM instrument_master WHERE country = 'US';
EXPLAIN SELECT isin, ticker, last_close FROM instrument_master WHERE country = 'US';
```

### Observe

- The first EXPLAIN's `project=[...]` is the full 45-column list.
- The second EXPLAIN's `project=[...]` is `[isin, ticker, last_close, country]`
  — three SELECT columns plus the filter column.
- Running both queries: the projected version returns the same rows in
  noticeably less data volume (visible in the result paging size).

### Aha

> Why is `country` in the project list even though I didn't select it?

Because the WHERE filter runs *above* the source scan, the source has to
deliver the column the filter needs. If the filter itself were pushed
down (as the PK lookup in step 04 is), the source could drop it before
returning.

### Pitfall

Don't conflate projection pushdown with column-store compression. Fluss
log segments are row-oriented; projection pushdown means "decode only
these fields per row", not "read a separate column file". The savings
are real but smaller than what Parquet gives you. The next step in the
track (scenario 16, Paimon tiering) is where column-store storage shows
up.

---

## Step 04 — Filter pushdown

### Concept

Fluss has a PK index. Filter predicates that touch only the PK column(s)
are translated by the planner into KV lookups against that index.
Non-PK filters fall through to a Flink Calc operator that runs above the
bucket scan.

### Story

"What's the last close of ISIN XYZ?" is the trading-desk question: it
should be a single point lookup, millisecond latency. "What's the average
beta of US Technology stocks?" is the analytics question: it's a scan, not
a lookup. Fluss handles both cleanly but you need to know which one your
query is.

### The SQL

```sql
EXPLAIN SELECT isin, ticker, last_close
FROM instrument_master WHERE isin = 'ISIN00000042';

EXPLAIN SELECT isin, ticker
FROM instrument_master WHERE country = 'US' AND sector = 'Technology';
```

### Observe

- The PK-lookup EXPLAIN includes a `lookup=[isin]` (or equivalent) field
  on the TableSourceScan.
- The non-PK-filter EXPLAIN has a separate `Calc(... where=[...])` above
  the scan — the filter is *not* pushed down.
- The non-PK-filter plan's project list includes the filter columns
  even though they're not in the SELECT.

### Aha

> What if I want country to be fast too?

Two options: (a) design the PK as `(country, isin)` so country becomes a
PK prefix (Cassandra-style — works well if every query starts with
country); (b) tier the table to Paimon (scenario 16) so the column store's
own pruning takes over. Fluss does NOT support secondary indexes today.

### Pitfall

The lookup is "fast" only if `bucket.num` is large enough to spread KV
state across tablet servers. A single-bucket PK table has one KV index
per server and that index becomes a hot key for high-traffic lookups.
Tune `bucket.num` for your read pattern.

---

## Step 05 — Batch aggregation over the wide table

### Concept

`SET 'execution.runtime-mode' = 'batch'` + a GROUP BY = the standard
analytical query. Combined with projection pushdown (step 03) you get
a "Trino-like" experience against streaming storage, no separate
analytical cluster needed.

### Story

The morning sector-risk report wants "instrument count, average 1y beta,
average ESG score per sector". On a Kafka topic you'd need to spin up a
Spark / Trino cluster and tier the topic to Parquet first. On Fluss you
write the SQL and get the answer.

### The SQL

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT sector, COUNT(*), AVG(beta_1y), AVG(esg_score)
FROM instrument_master GROUP BY sector ORDER BY 2 DESC;
```

### Observe

- ~11 rows of output (one per GICS sector).
- Sub-second runtime on the workshop dataset.
- EXPLAIN shows `project=[sector, beta_1y, esg_score]` — 3 of 45 columns.

### Aha

> Could I do this continuously instead of batched?

Yes — drop the `SET runtime-mode = batch` line and the same query runs as
a streaming aggregation that emits retract+update for every new row.
That's the right shape for an "always-up-to-date sector-risk dashboard"
sink. Batch is the right shape for "give me the answer once, terminate".

### Pitfall

Don't run the batch aggregation inside a session that still has streaming
queries running — they'll share the same TaskManager slot pool and you
may starve one. Open a fresh SQL Client session per mode.

---

## Step 06 — Cross-store batch join

### Concept

Flink SQL doesn't care whether the two sides of a JOIN live in the same
storage system. The catalog model lets you mix Fluss tables, JDBC tables,
filesystem tables, etc. in a single query. Step 06 demonstrates this
with Fluss `account_profile` LEFT JOIN Postgres `accounts`.

### Story

The trading firm is migrating accounts from a legacy Postgres system to a
new Fluss-backed account-state service. During the migration you want a
nightly reconciliation: how many accounts are in Fluss but not yet in
Postgres? One Flink SQL batch job answers that without copying data into
a third system.

### The SQL

```sql
CREATE TEMPORARY TABLE pg_accounts (...) WITH ('connector'='jdbc', ...);

SET 'execution.runtime-mode' = 'batch';
SELECT ap.tier, COUNT(*) AS fluss_count, COUNT(pg.account_id) AS pg_count
FROM account_profile ap LEFT JOIN pg_accounts pg
  ON ap.account_id = pg.account_id
GROUP BY ap.tier;
```

### Observe

- One row per tier (GOLD / PLATINUM / SILVER / ...).
- `fluss_count >= pg_count` — Fluss continues to seed while the Postgres
  table is one-shot.
- The EXPLAIN plan shows projection pushdown on BOTH sources — only
  `account_id` from Postgres, only `account_id, tier` from Fluss.

### Aha

> Why a LEFT JOIN rather than INNER?

LEFT JOIN preserves Fluss rows that have no Postgres counterpart and
makes them visible as `pg.account_id IS NULL`. That's the actionable
information for the migration job: "these accounts haven't been
replicated yet". INNER JOIN would silently drop them.

### Pitfall

JDBC sources aren't streaming. If you forget the `SET runtime-mode = batch`
the Fluss side will run forever waiting for the JDBC side to emit more
rows, which it never will. The streaming counterpart is a *lookup join*
(`FOR SYSTEM_TIME AS OF`) — that's scenario 14.

---

## Quick-reference cheat sheet

| You want to… | Use… |
|---|---|
| See which columns Fluss reads off disk | `EXPLAIN SELECT ...` and look at `project=[...]` |
| Fast point lookup by PK | `WHERE pk = '...'` → translated to `lookup=[pk]` |
| Filter by a non-PK column | falls through to a Flink Calc — design PK to include the column if it's hot |
| Aggregate the whole table | `SET runtime-mode = batch` + GROUP BY |
| Join Fluss with an external store | `CREATE TEMPORARY TABLE ... WITH ('connector'='jdbc', ...)` + batch mode |
| See the on-disk layout | `podman exec workshop-fluss-tablet-1 ls /tmp/fluss-data/` |

---

## Where these ideas re-appear

- **Scenario 13** — partial updates write only the columns you specify; the
  inverse of projection pushdown applied to writes.
- **Scenario 14** — streaming lookup joins are the always-on version of
  step 04's point lookup.
- **Scenario 15** — Java SDK lookups bypass Flink entirely; same KV index,
  same projection pushdown, no SQL layer.
- **Scenario 16** — Paimon tiering brings true column-store storage to
  the cold tier. Projection pushdown is even more dramatic on Parquet.
