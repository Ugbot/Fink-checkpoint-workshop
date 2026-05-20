# Scenario 13 Workshop — Partial Updates & Merge Engines

> **Audience:** completed scenario 11 (knows what a Fluss PK table is) and
> ideally scenario 12 (saw the wide-schema teaching). Knows Java DataStream
> basics if planning to read `WideProfileUpdaterJob`.
> **Time:** 45–60 minutes including reading the Java job.

---

## What you'll be able to do after this scenario

1. Write a partial-update INSERT that touches 3 of 30 columns and
   articulate why the other 27 are unaffected.
2. Pick the right merge engine for a use case: last-write-wins (default),
   aggregation, first_row, or versioned.
3. Read the `WideProfileUpdaterJob` and understand why it has one Kafka
   source, three side outputs, three Table-API INSERTs.

---

## Mental model

A Fluss PK table is a *queryable current-state view* of its primary key
space. Whenever you `INSERT INTO table_with_pk VALUES (...)`, Fluss looks
up the existing row by PK and *merges* the new row into it. The exact
merge rule is set by the table's `'table.merge-engine'` option:

| Engine | Behaviour | Use case |
|---|---|---|
| (default) | Each listed column is overwritten; unlisted columns keep their previous value | Customer 360 from many sources |
| `aggregation` | Per-column aggregate function (`sum`/`max`/`min`/`first_value`/`count`) | Running aggregates per key |
| `first_row` | Keep the FIRST write; ignore later ones | Event idempotency / dedup |
| `versioned` | A column carries a version number; the highest version wins | Out-of-order CDC, anti-stale |

Everything in scenario 13 is one of those four cases.

---

## Step 01 — Customer 360 DDL

### Concept
The workshop's `customer_360` Fluss PK table holds one row per customer
keyed on `customer_id`. The `init-fluss.sh` job created it; this step
just confirms via `DESCRIBE` and shows the options.

### Story
A bank has 12 source systems writing customer state: KYC, AML, marketing,
trading, support, mobile-app analytics, fraud, ad-attribution, …
Production teams want one canonical "this is the current state of
customer X" record without operating a 12-input keyed-state Flink job.
Fluss PK tables with partial-update semantics are the storage answer.

### Observe
- `SHOW CREATE TABLE customer_360` returns the DDL with `PRIMARY KEY
  (customer_id) NOT ENFORCED` and `bucket.num = 4`.
- No `table.merge-engine` set → default last-write-wins per column.

### Aha
> Where's the merge engine spec for "last-write-wins per column"?

There isn't one — that *is* the default behaviour. The merge engine
options activate alternative semantics like aggregation or first_row.

---

## Step 02 — KYC partial update

### Concept
INSERT lists only KYC columns. Other columns are not touched.

### Story
The KYC system runs once per customer onboarding. It writes legal name,
DOB, residency, document numbers — never AML or marketing data.

### Observe
After the INSERT, `SELECT *` shows KYC columns populated; AML and
marketing columns still NULL. Run a second insert with `address_line`
changed — only that column updates; the others stay put.

### Aha
> What stops the AML system overwriting KYC fields by accident?

Only the column list in the INSERT statement. If an engineer writes
`INSERT INTO customer_360 VALUES (...)` listing all 25 columns with
NULLs for the ones AML doesn't know about, it WILL stomp KYC data. The
contract is at the SQL-shape level: list only what you own.

### Pitfall
In Java (e.g. `WideProfileUpdaterJob`), the column subsets per source
must be hard-coded — there's no schema-registry-driven "this source owns
columns X..Y" enforcement. Document the ownership in a wiki + a unit
test.

---

## Step 03 — AML + Marketing slices

### Concept
Same shape as step 02, two more sources contribute. The row fills up.

### Story
20 minutes after KYC writes, the AML batch finishes and writes its
columns. An hour later marketing's CDC job updates consent preferences.
All three flow through to the same row.

### Observe
After each INSERT, `SELECT *` shows progressively more columns populated.
A re-write of AML columns updates only those — KYC and marketing slices
survive.

### Aha
> Why is this a big deal? Couldn't I just use a Flink stateful join?

You could. The Flink stateful version is:
```
trades.keyBy(customerId).process(KeyedCoProcessFunction with N inputs)
```
You'd hand-build the merge logic, manage TTL on state, choose backend,
checkpoint it, scale it. With Fluss, it's free — the state lives in the
storage layer.

### Pitfall
Streaming reads of `customer_360` see *every* partial update as a
changelog event. If the downstream consumer wants "the row is now in its
final shape" semantics, they need to define that themselves (e.g. wait
N seconds after the last update). Fluss emits changes as they happen.

---

## Step 04 — Aggregation merge engine

### Concept
Recreate the table with `'table.merge-engine' = 'aggregation'` plus per-
column aggregate functions. Each INSERT contributes to the running
aggregate.

### Story
Daily volume per account. Each trade is an insert of
`(account_id, trade_date, qty, notional, price, ts)`. Without aggregation,
the table would hold only the LAST trade per (account, date). With
aggregation:
- `total_qty.aggregate-function = sum` → cumulative quantity
- `total_notional.aggregate-function = sum` → cumulative notional
- `max_price.aggregate-function = max` → running max
- `min_price.aggregate-function = min` → running min
- `last_trade_ts.aggregate-function = max` → most recent timestamp

The workshop's `daily_volumes_agg` is shipped pre-configured this way.

### Observe
Three INSERTs for the same (account_id, trade_date) produce ONE row with
summed totals.

### Aha
> What was the alternative before this engine existed?

A Flink keyed-aggregate operator over the trade stream, with state for
each (account, date) bucket. Memory cost: 1 entry per active key per day.
The Fluss aggregation merge engine moves that state into the storage
layer where it survives Flink restarts, can be shared across jobs, and
costs you nothing during Flink rescaling.

### Pitfall
Aggregation merge is **not retract-aware**. If a trade is later corrected
to a different quantity, you can't INSERT (-300, +250) — there's no
"subtract this amount" semantics for `sum`. For correctness-critical
workloads you still want the Flink retract stream.

---

## Step 05 — first_row and versioned

### Concept
Two more merge engines for specific patterns.

`first_row`: the first INSERT for a PK wins; later inserts are silently
ignored. Use case: deduplication of events emitted at-least-once by
upstream.

`versioned`: a column carries a version number; the engine keeps only
the row with the highest version. Out-of-order writes can't go backwards.

### Story
- `first_row` example: a payment-event router pushes each event through
  several geo-regions for resilience. Each region writes its own copy to
  Fluss. Without `first_row` you'd see N duplicate rows per payment;
  with it, the first regional arrival wins and the rest are no-ops.
- `versioned` example: a CDC pipeline reading from a Postgres replica
  might apply LSN-7 *after* LSN-9 arrives due to topic re-ordering.
  Marking `version` as the LSN-equivalent column and using the versioned
  engine guarantees stale writes are ignored.

### Observe
For `first_row`: insert "router-a" then "router-b" with same event_id;
the SELECT returns "router-a".
For `versioned`: insert version 5, then version 3 (stale), then version
7; the SELECT returns version 7.

### Aha
> What if I genuinely WANT to overwrite the first-row case?

Then `first_row` is the wrong engine for that column. Use last-write-wins
(default) or aggregation. The choice is per-table, not per-write.

---

## The Java job — `WideProfileUpdaterJob`

This is the production-shaped pattern: one Flink job consuming a Kafka
topic of mixed customer events, fanning them out into per-source partial
updates.

### Shape

```
Kafka topic: customer-events
        │
        │   payload = { "source": "kyc"/"aml"/"marketing",
        │                "customerId": "...", "fields": { ... } }
        ▼
   KafkaSource<CustomerEvent>
        │
        ▼
   RouterFn (ProcessFunction with 3 side outputs)
        ├── KYC_TAG → DataStream<Row>     (12 cols)
        ├── AML_TAG → DataStream<Row>     ( 8 cols)
        └── MKT_TAG → DataStream<Row>     ( 9 cols)
                │           │            │
                ▼           ▼            ▼
        Table API view  view  view
                │           │            │
                ▼           ▼            ▼
      INSERT INTO customer_360 (kyc cols)
      INSERT INTO customer_360 (aml cols)
      INSERT INTO customer_360 (mkt cols)
                │           │            │
                └───────────┴────────────┘
                            │
                            ▼
                  fluss.workshop.customer_360
                  (merges per PK at the storage layer)
```

### Why three INSERTs?

Each INSERT has a **different column list**. The Fluss connector handles
each as a partial update of its column subset.

### Why side outputs and not three separate Kafka topics?

Two reasons:
1. Many real teams unify their CDC into one topic with envelope-typed
   payloads. The router-side is the natural place to split.
2. With one Kafka source you get one consumer group, one offset commit,
   one set of metrics. Three sources triple the operational surface.

If your real-world setup has three separate topics, replace the single
KafkaSource with three sources and drop the router. The downstream INSERT
logic is unchanged.

### Try this

Run `WideProfileUpdaterJob`, feed it test events, and watch
`customer_360` rows fill up. The verify script does this end-to-end.

---

## Quick reference

| Goal | Merge engine | Table options |
|---|---|---|
| Customer 360 from many sources | default | (none) |
| Per-PK running totals | `aggregation` | `fields.<col>.aggregate-function = sum / max / min / …` |
| Event idempotency / dedup | `first_row` | `'table.merge-engine' = 'first_row'` |
| Anti-stale CDC | `versioned` | `'table.merge-engine' = 'versioned'`, `'table.merge-engine.versioned.ver-field' = '<col>'` |

---

## Where these ideas re-appear

- **Scenario 14** — lookup joins against `customer_360` from a trade stream;
  the result of step 03 becomes the lookup target for step 14.
- **Scenario 15** — the Java SDK app does point lookups into the same table.
- **Scenario 16** — tiering `customer_360` to Paimon, queryable by Spark.
