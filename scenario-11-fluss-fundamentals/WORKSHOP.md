# Scenario 11 Workshop — Apache Fluss Fundamentals

> **Audience:** anyone who can read Flink SQL but hasn't used Fluss.
> **Time:** ~45 minutes if you read every comment and reflect at the "aha"
> questions.
> **Format:** SQL Client paste-along. Each step is one `.sql` file.

---

## What you'll be able to do after this scenario

1. Explain in one sentence what Apache Fluss is and which Kafka pain points
   it addresses.
2. Write the `CREATE CATALOG fluss WITH (...)` DDL from memory.
3. Describe the difference between a log table and a primary-key table.
4. Recognise three concrete cases where a Kafka compacted topic gives a
   surprising or wrong answer that a Fluss PK table handles cleanly.

---

## Prerequisites

- Workshop infra up: `podman-compose up -d --build`
- Fluss tables created: `bash scripts/init-fluss.sh`
- Seed data flowing: `bash scripts/seed-fluss.sh`

---

## Step 01 — What is Fluss?

### Concept
Apache Fluss (incubating) is a streaming storage system that adds **schemas**,
**primary-key tables**, **point-in-time lookups**, **projection pushdown**
and **tiered storage to a lakehouse** to the Kafka-shaped data model. It's
designed for Flink as the primary execution engine.

### Story
A trading firm runs a hot data path on Kafka: trades published, services
consume. To answer "what's the current state of account X?" they built a
sidecar that materialises a compacted topic into Redis. To answer
"aggregate trades by sector" they tier to S3 with a separate stream job and
let analysts hit it with Trino. Three storage systems for one logical
"account stream". Fluss collapses these into one queryable streaming store.

### The SQL
```sql
CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
SHOW CATALOGS;
USE CATALOG fluss;
SHOW DATABASES;
```

### Observe
- `SHOW CATALOGS` includes `fluss`. The catalog definition lives in your
  Flink session only — the data lives in the Fluss cluster.
- `SHOW DATABASES` returns at least `fluss` (the built-in default) and
  `workshop` (created by `init-fluss.sh`).

### Aha
> Why does the catalog DDL not contain any credentials?

The workshop's Fluss cluster runs without auth (PLAINTEXT). Production
deployments configure SASL/TLS via additional `client.security.*` options
on the `CREATE CATALOG` block.

### Pitfall
Re-running `CREATE CATALOG fluss WITH (...)` with different options does
**not** update the registration silently — without `IF NOT EXISTS` the
second attempt errors; with `IF NOT EXISTS` the first registration wins.
Drop and recreate to change options.

---

## Step 02 — Database and listing

### Concept
Catalogs are a Flink concept; databases and tables inside them are real
Fluss objects. The workshop's `init-fluss.sh` job created the `workshop`
database and the canonical set of tables this track uses.

### The SQL
```sql
USE CATALOG fluss;
USE workshop;
SHOW TABLES;
DESCRIBE instrument_master;
```

### Observe
- `SHOW TABLES` lists the seven workshop tables (instrument_master,
  account_profile, customer_360, trade_log_wide, …).
- `DESCRIBE instrument_master` shows all 45 columns + `PRIMARY KEY (isin)
  NOT ENFORCED`.

### Aha
> What does "NOT ENFORCED" mean?

Flink's SQL spec marks every primary-key constraint as NOT ENFORCED
because Flink itself does not enforce uniqueness — it trusts the source.
For a Fluss PK table, the storage engine *does* enforce it: every insert
upserts by PK. The NOT ENFORCED is a Flink-side label, not a Fluss
weakness.

---

## Step 03 — Log table (append-only)

### Concept
Log tables are Fluss's append-only flavour. Bucketed (default `bucket.num`
= 1; the workshop uses 8 for `trade_log_wide`). No primary key. Reads can
be either batch (one-shot snapshot) or streaming (tail).

### Story
The desk wants to publish every executed trade onto a stream that risk,
P&L, and compliance can each consume independently. Append-only is the
right shape — every system needs the same events, ordered. Compaction
would erase history.

### Observe
- `INSERT INTO trade_log_wide VALUES (...)` succeeds without a primary
  key.
- `SELECT * FROM trade_log_wide` in batch mode terminates; in streaming
  mode it tails new rows forever.

### Aha
> Where does `bucket.num` show up?

Buckets are visible in the Fluss UI / metrics as concurrent producer/
consumer paths. They behave like Kafka partitions for throughput
purposes; unlike Kafka they're rebalanceable across tablet servers.

### Pitfall
Don't read a log table in batch mode from a long-running session and
expect it to "tail" — batch reads always terminate at the time of issue.
Use streaming runtime mode for continuous reads.

---

## Step 04 — Primary-key table

### Concept
The headline Fluss feature. Reads as a regular table; insert upserts by
key; the storage engine also exposes a changelog stream of `+I/-U/+U/-D`
events for downstream change-data-capture jobs.

### Story
Your trading platform has 50k accounts. A user clicks a button — your
microservice needs the account's current tier, risk score and credit line.
In Kafka land this means consuming the compacted topic at startup and
keeping a sidecar materialisation hot. In Fluss this is a literal SQL
`SELECT * FROM account_profile WHERE account_id = ?` — or, from Java, a
single `table.lookup(key)` call (see scenario 15).

### Observe
- Inserting the same `account_id` twice updates the row, doesn't duplicate
  it.
- `SELECT * WHERE account_id = '...'` is a point lookup, not a scan.
- Streaming SELECT shows row-kind annotations (`+I`, `-U`, `+U`).

### Aha
> Where is the "latest write wins" rule documented?

In the table's `'table.merge-engine'` option (default = `last_write_wins`).
Scenario 13 changes this to `aggregation`, `first_row`, or `versioned` for
different merge semantics.

### Pitfall
The changelog stream is generated by Fluss at read time, not at write
time — there's no separate "changelog topic". Downstream consumers see the
view that matches the time they start reading.

---

## Step 05 — Batch aggregation

### Concept
Switching `execution.runtime-mode` to `batch` makes the same `SELECT GROUP
BY` query terminate at the current Fluss snapshot rather than tailing. The
EXPLAIN plan shows the planner reads only the projected columns.

### Story
The morning P&L report wants per-account totals across the trading day.
You don't want this report to be a streaming job that produces an output
record per update — you want a one-shot answer. Batch mode is the right
fit.

### Observe
- The batch query terminates with a finite result set.
- `EXPLAIN` shows `TableSourceScan(table=[[fluss, workshop,
  trade_log_wide]])` with the projection list pruned.

### Aha
> Could I use this for "wake up every 5 minutes and produce a report"?

Yes, but consider whether a streaming query with a 5-minute tumbling
window would express the same thing more cheaply. Batch is for jobs you
trigger; streaming is for jobs that run continuously.

---

## Step 06 — Kafka compacted vs Fluss

This is the centerpiece. Read the file end-to-end — it's a guided
comparison of three concrete queries.

### The three pain points

1. **Point lookup by key** — Kafka compacted topic needs a full scan;
   Fluss serves it as a primary-key lookup.
2. **Batch aggregation by non-PK column** — compacted-topic scan is
   unbounded without manual offset bounding; Fluss's batch mode is
   first-class.
3. **Partial updates** — Kafka compaction upserts the entire value;
   Fluss writes only the columns you list and leaves the rest alone
   (scenario 13 covers this in depth).

### Aha
> When SHOULD I still use a Kafka compacted topic?

Use Kafka compacted topics when:
- Your downstream is a Kafka-native consumer (no SQL, no Flink).
- You only need "latest config per key" replayed at app startup.
- Whole-value upserts are exactly what you want.

Use Fluss PK tables for everything else where you've found yourself
"building a lookup service on top of a compacted topic".

---

## Step 07 — Cleanup

DROP TABLE / DROP DATABASE / DROP CATALOG. Symmetric and idempotent with
`IF EXISTS`. **Do not run this during the workshop** — scenarios 12–16
reuse the same tables.

---

## Quick-reference cheat sheet

| You want to… | Use… |
|---|---|
| Append-only event stream | Fluss log table |
| Latest state per key, lookupable | Fluss PK table |
| Both at once on the same data | A PK table + changelog read |
| Aggregate the whole history once | `SET 'execution.runtime-mode' = 'batch'` |
| Tail new events as they arrive | Default streaming mode |
| Query the data from outside Flink | The Fluss Java SDK (scenario 15) |

---

## Where these ideas re-appear

- **Scenario 12** — projection pushdown made dramatic with the 45-column
  `instrument_master`.
- **Scenario 13** — `customer_360` partial updates from three sources;
  merge-engine variants.
- **Scenario 14** — lookup joins from streaming queries into Fluss PK
  tables (the operational version of step 04's point lookup).
- **Scenario 15** — Java apps doing the same point lookups without Flink.
- **Scenario 16** — tiering Fluss tables to Paimon and querying both
  with Spark.
