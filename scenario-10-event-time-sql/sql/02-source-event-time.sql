-- Step 2 — Declare `trade_time` as the event-time attribute with a watermark.
--
-- Lessons:
--   • The WATERMARK clause is what makes a column event-time. It tells the
--     planner: "emit a watermark equal to (max trade_ts so far) - 5 seconds".
--   • The bounded-out-of-orderness pattern is by far the most common watermark
--     strategy in SQL. Pick the delay generously enough that 99.x% of records
--     fall inside it; truly late records become the "late data" problem we
--     handle in step 6 and 7.
--   • TO_TIMESTAMP_LTZ is required when the source carries epoch millis as a
--     BIGINT — Flink SQL needs a TIMESTAMP_LTZ(3) for event-time semantics.

DROP TABLE IF EXISTS kafka_trades_eventtime;

CREATE TABLE kafka_trades_eventtime (
  event_id    STRING,
  account_id  STRING,
  ticker      STRING,
  side        STRING,
  quantity    INT,
  price       DOUBLE,
  trade_time  BIGINT,
  source_app  STRING,
  trade_ts    AS TO_TIMESTAMP_LTZ(trade_time, 3),
  WATERMARK FOR trade_ts AS trade_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'topic.in',
  'properties.bootstrap.servers'  = 'workshop-kafka:9093',
  'properties.group.id'           = 'sql-s10-eventtime',
  'scan.startup.mode'             = 'earliest-offset',
  'format'                        = 'json',
  'json.ignore-parse-errors'      = 'true'
);

DESCRIBE EXTENDED kafka_trades_eventtime;
-- The output annotates `trade_ts` as `ROWTIME` and shows the watermark expression.
