-- Step 5 — Keep watermarks moving when a partition is idle.
--
-- Lessons:
--   • Flink advances the operator's watermark to MIN(per-partition watermarks). If
--     one of the topic's partitions stops emitting, that partition's watermark
--     freezes and the operator's watermark freezes with it.
--   • `'scan.watermark.idle-timeout'` declares: "treat a partition with no records
--     for N as idle — i.e., ignore it for watermark computation."
--   • Pick this generously: too short and a real lull misclassifies as idle and
--     advances the watermark prematurely, missing legitimate records as late.

DROP TABLE IF EXISTS kafka_trades_eventtime_idle;

CREATE TABLE kafka_trades_eventtime_idle (
  event_id    STRING,
  account_id  STRING,
  ticker      STRING,
  quantity    INT,
  price       DOUBLE,
  trade_time  BIGINT,
  trade_ts    AS TO_TIMESTAMP_LTZ(trade_time, 3),
  WATERMARK FOR trade_ts AS trade_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                       = 'kafka',
  'topic'                           = 'topic.in',
  'properties.bootstrap.servers'    = 'workshop-kafka:9093',
  'properties.group.id'             = 'sql-s10-idle',
  'scan.startup.mode'               = 'earliest-offset',
  'format'                          = 'json',
  'json.ignore-parse-errors'        = 'true',
  'scan.watermark.idle-timeout'     = '30 s'   -- ← key line
);

-- Equivalent in DataStream:
--   WatermarkStrategy.forBoundedOutOfOrderness(...).withIdleness(Duration.ofSeconds(30))
