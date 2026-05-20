-- Step 1 — Bare DDL with processing-time only.
--
-- Lessons:
--   • PROCTIME() is a computed column. It is NOT a stored value — every read returns
--     the current wall-clock millis on the operator's machine.
--   • Processing-time windows are simple but non-deterministic across reruns and
--     across operator restarts. Use them for monitoring, never for correctness.

DROP TABLE IF EXISTS kafka_trades_proctime;

CREATE TABLE kafka_trades_proctime (
  event_id    STRING,
  account_id  STRING,
  ticker      STRING,
  side        STRING,
  quantity    INT,
  price       DOUBLE,
  trade_time  BIGINT,                   -- epoch millis, written by the datagen job
  source_app  STRING,
  proc_ts     AS PROCTIME()             -- ← computed column: processing time
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'topic.in',
  'properties.bootstrap.servers'  = 'workshop-kafka:9093',
  'properties.group.id'           = 'sql-s10-proctime',
  'scan.startup.mode'             = 'earliest-offset',
  'format'                        = 'json',
  'json.ignore-parse-errors'      = 'true'
);

-- Validate it shows up in the catalog:
SHOW CREATE TABLE kafka_trades_proctime;
