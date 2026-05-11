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
