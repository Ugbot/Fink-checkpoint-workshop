# Flink SQL Equivalents

For each DataStream scenario, these are the key SQL DDL and configuration changes.

---

## Scenario 01 — Baseline AT_LEAST_ONCE

```sql
SET 'execution.checkpointing.mode' = 'AT_LEAST_ONCE';
SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE kafka_in (
  event_id    STRING,
  account_id  STRING,
  ticker      STRING,
  side        STRING,
  quantity    INT,
  price       DOUBLE,
  trade_time  BIGINT,
  source_app  STRING
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'topic.in',
  'properties.bootstrap.servers'  = 'localhost:9092',
  'properties.group.id'           = 'sql-s01-app1',
  'scan.startup.mode'             = 'earliest-offset',
  'format'                        = 'json'
);

CREATE TABLE kafka_mid (
  event_id    STRING,
  account_id  STRING,
  ticker      STRING,
  side        STRING,
  quantity    INT,
  price       DOUBLE,
  trade_time  BIGINT,
  source_app  STRING
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'topic.mid',
  'properties.bootstrap.servers'  = 'localhost:9092',
  'format'                        = 'json'
  -- No 'sink.delivery-guarantee' or 'sink.transactional-id-prefix' → AT_LEAST_ONCE
);

INSERT INTO kafka_mid SELECT * FROM kafka_in;
```

**Key difference from exactly-once:** no `sink.delivery-guarantee` property and no `sink.transactional-id-prefix`. The default in Flink SQL's Kafka connector is `at-least-once`.

---

## Scenario 02 — EXACTLY_ONCE Kafka

```sql
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE kafka_mid (
  event_id    STRING,
  ...
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'topic.mid',
  'properties.bootstrap.servers'  = 'localhost:9092',
  'format'                        = 'json',
  'sink.delivery-guarantee'       = 'exactly-once',
  'sink.transactional-id-prefix'  = 'sql-s02-mid'
);

CREATE TABLE kafka_out (
  ...
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'topic.out',
  'properties.bootstrap.servers'  = 'localhost:9092',
  'format'                        = 'json',
  'sink.delivery-guarantee'       = 'exactly-once',
  'sink.transactional-id-prefix'  = 'sql-s02-out'
);

-- Source must set isolation.level=read_committed to only see committed transactions:
CREATE TABLE kafka_mid_source (
  ...
) WITH (
  'connector'                         = 'kafka',
  'topic'                             = 'topic.mid',
  'properties.bootstrap.servers'      = 'localhost:9092',
  'properties.group.id'               = 'sql-s02-app2',
  'scan.startup.mode'                 = 'earliest-offset',
  'properties.isolation.level'        = 'read_committed',
  'format'                            = 'json'
);
```

**Key difference from scenario-01:** add `sink.delivery-guarantee = 'exactly-once'`, `sink.transactional-id-prefix`, and `properties.isolation.level = 'read_committed'` on the source.

---

## Scenario 03 — External Sink (JDBC)

```sql
-- Sink table definition WITH primary key makes Flink SQL use upsert mode automatically.
-- This is the "fixed" behavior — the SQL connector handles idempotency for you.
CREATE TABLE processed_trades (
  event_id     STRING,
  account_id   STRING,
  ticker       STRING,
  net_qty      INT,
  PRIMARY KEY (event_id) NOT ENFORCED  -- tells Flink SQL to use upsert/ON CONFLICT
) WITH (
  'connector'   = 'jdbc',
  'url'         = 'jdbc:postgresql://localhost:5432/workshop',
  'table-name'  = 'processed_trades',
  'username'    = 'workshop',
  'password'    = 'workshop',
  'driver'      = 'org.postgresql.Driver'
);

INSERT INTO processed_trades
SELECT event_id, account_id, ticker, quantity FROM kafka_mid_source;
```

**Important teaching point:** Flink SQL's JDBC connector automatically uses upsert semantics when the sink table has `PRIMARY KEY NOT ENFORCED` in its DDL. This is safer than the DataStream `RichSinkFunction` approach because idempotency is declarative, not hand-coded. The "buggy" plain-INSERT behavior from the DataStream scenario cannot easily be reproduced in Flink SQL — which is an advantage.

---

## Scenario 04 — Semantic Dedup

Flink SQL does not have a built-in dedup operator, but the optimizer recognizes the `ROW_NUMBER()` pattern and compiles it to the same stateful dedup operator as `DeduplicationFunction`:

```sql
-- Control state TTL for the dedup operator
SET 'table.exec.state.ttl' = '3600000';  -- 1 hour in milliseconds

-- Dedup using ROW_NUMBER: keep only the first occurrence of each event_id
SELECT event_id, account_id, ticker, side, quantity, price, trade_time, source_app
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY event_id
      ORDER BY PROCTIME() ASC
    ) AS row_num
  FROM kafka_mid_source
)
WHERE row_num = 1;
```

**Key difference from DataStream:** the TTL is controlled by the session property `table.exec.state.ttl` (in ms) rather than `StateTtlConfig` in the operator. The semantics are equivalent.

**Important:** `PROCTIME()` produces processing-time ordering, which is what you want for dedup (first-seen wins). Using an event-time column would require watermarks and would not guarantee first-seen ordering.

---

## Scenario 05 — Rescaling

Rescaling (savepoint + parallelism change) works identically in Flink SQL and DataStream — the runtime does not distinguish between the two APIs at the checkpoint level.

```sql
-- Set max parallelism at job submission time to control key group range.
-- This determines how fine-grained future rescaling can be.
-- Default is 128. Changing this after the first savepoint requires full state migration.
SET 'pipeline.max-parallelism' = '128';

-- Event-time window equivalent of App2Pipeline:
SET 'table.exec.state.ttl' = '60000';  -- 1 minute window state TTL

SELECT
  account_id,
  TUMBLE_START(trade_ts, INTERVAL '30' SECOND) AS window_start,
  COUNT(*) AS trade_count
FROM (
  SELECT *, TO_TIMESTAMP_LTZ(trade_time, 3) AS trade_ts
  FROM kafka_mid_source
)
GROUP BY account_id, TUMBLE(trade_ts, INTERVAL '30' SECOND);
```

**Savepoint workflow (identical for SQL jobs):**
```bash
# Get the job ID from Flink Web UI or:
flink list

# Take savepoint
flink savepoint <jobId> file:///tmp/flink-savepoints/s05-before-rescale

# Stop and restart with higher parallelism
flink stop --savepointPath file:///tmp/flink-savepoints/s05-auto <jobId>

# Or restart from manual savepoint at new parallelism
flink run -p 4 --fromSavepoint file:///tmp/flink-savepoints/s05-before-rescale \
    my-sql-job.jar
```

**`pipeline.max-parallelism` note:** Flink distributes state across `max-parallelism` key groups. When you rescale from p=2 to p=4, each task now handles a smaller range of key groups — but the key group → partition mapping is fixed. Setting `max-parallelism` to a power of 2 (64, 128, 256) allows rescaling to any divisor of that value without state migration.

---

## Scenario 06 — Paimon lakehouse + SQL Gateway

This scenario is **SQL-first by design**: the Java DataStream job exists only to wrap the Kafka source, while the actual lakehouse logic is pure Flink SQL. The same DDL is reused by `PaimonCatalogBootstrap`, `PaimonIngestJob`, the SQL Client, and the Web UI.

```sql
-- 1. Register the Paimon catalog on MinIO. Same DDL is executed at
--    bootstrap time and on every ingest-job submission (idempotent).
CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type'                  = 'paimon',
  'warehouse'             = 's3a://paimon/warehouse',
  's3.endpoint'           = 'http://workshop-minio:9000',
  's3.access-key'         = 'workshop',
  's3.secret-key'         = 'workshopsecret',
  's3.path.style.access'  = 'true'
);

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS workshop;
USE workshop;

-- 2. Paimon primary-key table. `changelog-producer = 'input'` records the
--    upsert stream so downstream readers can do streaming reads with
--    proper change semantics. `bucket = 4` matches the Kafka topic.
CREATE TABLE IF NOT EXISTS trades (
  event_id   STRING,
  account_id STRING,
  ticker     STRING,
  side       STRING,
  quantity   INT,
  price      DOUBLE,
  trade_time TIMESTAMP_LTZ(3),
  source_app STRING,
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'bucket'                 = '4',
  'changelog-producer'     = 'input',
  'snapshot.time-retained' = '1h'
);

-- 3. Streaming insert from Kafka. The Java job constructs `kafka_trades`
--    via fromDataStream(); a fully-SQL equivalent would use the kafka
--    connector DDL from earlier scenarios.
INSERT INTO paimon.workshop.trades
SELECT
  eventId,
  accountId,
  ticker,
  side,
  quantity,
  price,
  TO_TIMESTAMP_LTZ(tradeTime, 3),
  sourceApp
FROM default_catalog.default_database.kafka_trades;
```

**Ad-hoc queries via the SQL Gateway:**

```sql
-- Lifetime row count
SELECT COUNT(*) FROM paimon.workshop.trades;

-- Per-ticker notional
SELECT ticker, SUM(quantity * price) AS notional
FROM paimon.workshop.trades
GROUP BY ticker
ORDER BY notional DESC;

-- Lakehouse metadata: Paimon system tables
SELECT snapshot_id, commit_time, total_record_count
FROM paimon.workshop.`trades$snapshots`
ORDER BY snapshot_id DESC;
```

**Snapshot visibility:** Paimon only publishes a new snapshot at Flink checkpoint barriers. With the workshop's default 10-second checkpoint interval, query results lag the Kafka source by at most one checkpoint — this is why `verify-06.sh` tolerates a small `kafka - paimon` delta.

---

## Scenario 07 — Stream-Stream Joins (regular, interval, window)

Shared event-time DDL used by the join queries below:

```sql
CREATE TABLE kafka_trades (
  event_id   STRING,
  account_id STRING,
  ticker     STRING,
  side       STRING,
  quantity   INT,
  price      DOUBLE,
  trade_time BIGINT,
  trade_ts   AS TO_TIMESTAMP_LTZ(trade_time, 3),
  WATERMARK FOR trade_ts AS trade_ts - INTERVAL '5' SECOND
) WITH ('connector'='kafka', 'topic'='topic.in', /* ... */);

CREATE TABLE kafka_quotes (
  quote_id   STRING,
  ticker     STRING,
  bid        DOUBLE,
  ask        DOUBLE,
  quote_time BIGINT,
  quote_ts   AS TO_TIMESTAMP_LTZ(quote_time, 3),
  WATERMARK FOR quote_ts AS quote_ts - INTERVAL '5' SECOND
) WITH ('connector'='kafka', 'topic'='topic.quotes', /* ... */);

CREATE TABLE kafka_orders (...);  -- same shape as topic.orders
CREATE TABLE kafka_fills  (...);  -- same shape as topic.fills
```

### Regular join — all four kinds in SQL

```sql
SET 'table.exec.state.ttl' = '3600000';  -- 1 hour, bounds unbounded join state

-- INNER
SELECT o.event_id, o.ticker, o.price AS order_price, f.price AS fill_price
FROM kafka_orders o
JOIN kafka_fills f ON o.event_id = f.event_id;

-- LEFT OUTER (orders without fills)
SELECT o.event_id, o.ticker, f.event_id IS NULL AS unfilled
FROM kafka_orders o
LEFT JOIN kafka_fills f ON o.event_id = f.event_id;

-- RIGHT OUTER
SELECT f.event_id, o.event_id IS NULL AS orphan_fill
FROM kafka_orders o
RIGHT JOIN kafka_fills f ON o.event_id = f.event_id;

-- FULL OUTER
SELECT COALESCE(o.event_id, f.event_id) AS event_id,
       CASE WHEN o.event_id IS NULL THEN 'RIGHT_ORPHAN'
            WHEN f.event_id IS NULL THEN 'LEFT_ORPHAN'
            ELSE 'INNER' END AS join_kind
FROM kafka_orders o
FULL OUTER JOIN kafka_fills f ON o.event_id = f.event_id;
```

**Key point:** `table.exec.state.ttl` is the *only* lever for bounding regular-join state in pure SQL. The DataStream equivalent (`OrderFillRegularJoin`) uses `StateTtlConfig` *plus* per-key processing-time timers — strictly more control than SQL offers.

### Interval join — time-bounded SQL

```sql
SELECT t.event_id, t.ticker, t.price, q.bid, q.ask
FROM kafka_trades t, kafka_quotes q
WHERE t.ticker = q.ticker
  AND q.quote_ts BETWEEN t.trade_ts - INTERVAL '5' SECOND
                     AND t.trade_ts + INTERVAL '5' SECOND;
```

The planner recognizes the `BETWEEN` predicate as an interval join and generates a bounded-state operator — same one Flink uses for `intervalJoin(...).between(...)` in DataStream. No TTL knob needed; the bound itself is the eviction policy.

### Window join — co-windowed counts

```sql
SELECT t.window_start, t.window_end, t.ticker,
       t.trade_count, q.quote_count
FROM (
  SELECT window_start, window_end, ticker, COUNT(*) AS trade_count
  FROM TABLE(TUMBLE(TABLE kafka_trades, DESCRIPTOR(trade_ts), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, ticker
) t
JOIN (
  SELECT window_start, window_end, ticker, COUNT(*) AS quote_count
  FROM TABLE(TUMBLE(TABLE kafka_quotes, DESCRIPTOR(quote_ts), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, ticker
) q
ON t.window_start = q.window_start AND t.ticker = q.ticker;
```

Uses the `TUMBLE` TVF (table-valued function) added in Flink 1.13, then a regular equi-join on the window boundaries.

---

## Scenario 08 — Stream-Table Joins (lookup vs temporal)

### Lookup join (JDBC accounts dim)

```sql
CREATE TABLE accounts (
  account_id   STRING,
  account_name STRING,
  tier         STRING,
  region       STRING,
  PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
  'connector'              = 'jdbc',
  'url'                    = 'jdbc:postgresql://workshop-postgres:5432/workshop',
  'table-name'             = 'accounts',
  'username'               = 'workshop',
  'password'               = 'workshop',
  'driver'                 = 'org.postgresql.Driver',
  'lookup.cache'           = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '1000',
  'lookup.partial-cache.expire-after-write' = '5 min'
);

-- Lookup join uses FOR SYSTEM_TIME AS OF PROCTIME()
SELECT t.event_id, t.ticker, a.account_name, a.tier, a.region
FROM kafka_trades t
LEFT JOIN accounts FOR SYSTEM_TIME AS OF t.trade_ts AS a
  ON t.account_id = a.account_id;
```

**Note:** the JDBC connector's built-in `lookup.partial-cache.*` config is the SQL equivalent of the Caffeine cache used in `App08LookupJoin.java`. Same semantics: processing-time lookup, cache TTL, async pool managed by the connector.

### Temporal (versioned) join — FX rate AS OF trade_ts

```sql
CREATE TABLE kafka_fxrates (
  currency   STRING,
  rate       DOUBLE,
  rate_time  BIGINT,
  rate_ts    AS TO_TIMESTAMP_LTZ(rate_time, 3),
  WATERMARK FOR rate_ts AS rate_ts - INTERVAL '2' SECOND,
  PRIMARY KEY (currency) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic'     = 'topic.fxrates',
  'properties.bootstrap.servers' = 'workshop-kafka:9093',
  'key.format'   = 'raw',
  'value.format' = 'json'
);

-- Notice the AS OF column is the trade's event-time, not PROCTIME().
-- The planner generates a TemporalRowTimeJoin operator backed by per-key state.
SELECT t.event_id, t.ticker, fx.currency, fx.rate, fx.rate_ts
FROM kafka_trades t
LEFT JOIN kafka_fxrates FOR SYSTEM_TIME AS OF t.trade_ts AS fx
  ON CurrencyOf(t.account_id) = fx.currency;   -- CurrencyOf is a user-defined function
```

**The fundamental difference:**
- Lookup join → `FOR SYSTEM_TIME AS OF PROCTIME()` → answers "current row" at runtime
- Temporal join → `FOR SYSTEM_TIME AS OF <event_time_col>` → answers "row in effect at event time"

The lookup join's output is **non-deterministic** under replay; the temporal join's output **is** deterministic. Pick based on whether your downstream system tolerates drift.

---

## Scenario 09 — Multi-Way Join + Recovery

```sql
INSERT INTO kafka_enriched_s09
SELECT
  t.event_id, t.ticker, t.quantity, t.price,
  q.bid, q.ask,
  fx.currency, fx.rate, fx.rate_ts AS fx_as_of,
  a.account_name, a.tier, a.region,
  t.quantity * t.price * fx.rate AS notional_usd
FROM kafka_trades t
JOIN kafka_quotes q
  ON t.ticker = q.ticker
 AND q.quote_ts BETWEEN t.trade_ts - INTERVAL '5' SECOND
                    AND t.trade_ts + INTERVAL '5' SECOND
LEFT JOIN kafka_fxrates FOR SYSTEM_TIME AS OF t.trade_ts AS fx
  ON CurrencyOf(t.account_id) = fx.currency
LEFT JOIN accounts FOR SYSTEM_TIME AS OF t.trade_ts AS a
  ON t.account_id = a.account_id;
```

Pedagogically equivalent to the DataStream pipeline in `App09MultiWayJoin.java`. **Recovery semantics are identical** — both compile to checkpointed keyed-state operators. The DataStream version is exposed because (a) it's easier to drop a `CrashTrigger` into custom operators, and (b) the per-operator state behaviour is observable in the Flink UI's operator panel.

Watermark alignment in SQL — the SQL equivalent of `withWatermarkAlignment` is per-source DDL:

```sql
CREATE TABLE kafka_trades_aligned ( ... ) WITH (
  ...
  'scan.watermark.alignment.group'           = 's09-aligned',
  'scan.watermark.alignment.max-drift'       = '30 s',
  'scan.watermark.alignment.update-interval' = '1 s'
);
```

All sources sharing the same `alignment.group` name stall together when one runs more than `max-drift` ahead of another.

---

## Scenario 10 — Event Time, Watermarks, Late Data (SQL-only)

This scenario lives entirely in `scenario-10-event-time-sql/sql/*.sql`. It's the SQL-only complement to scenarios 07–09:

- `01-source-proctime.sql` — bare DDL using `PROCTIME()`
- `02-source-event-time.sql` — `WATERMARK FOR ts AS ts - INTERVAL '5' SECOND`
- `03-tumbling-proctime.sql` vs `04-tumbling-event-time.sql` — same window, different semantics
- `05-idle-source-timeout.sql` — `scan.watermark.idle-timeout`
- `06-allowed-lateness.sql` — `table.exec.window-allowed-lateness` (retract/revise)
- `07-late-data-side-channel.sql` — `CURRENT_WATERMARK()` predicate to route late records to a dedicated topic
- `08-watermark-alignment.sql` — `scan.watermark.alignment.*` per source

Run via `bash scripts/run-scenario-10-sql.sh`.
