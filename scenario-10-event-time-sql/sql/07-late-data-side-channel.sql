-- Step 7 — Route late-arriving records to a dedicated sink.
--
-- Lessons:
--   • Flink SQL does not (yet) expose a generic "late events" side output the way
--     DataStream does. The pragmatic substitute: keep the source's CURRENT_WATERMARK
--     and compare event timestamp against it via a stateful query.
--   • This trick uses the system function `CURRENT_WATERMARK(trade_ts)`, available
--     since Flink 1.17. The comparison is evaluated per record.
--   • Records older than the current watermark would normally be silently dropped by
--     window operators — here we route them to a separate Kafka topic for inspection.

DROP TABLE IF EXISTS kafka_late_trades;

CREATE TABLE kafka_late_trades (
  event_id    STRING,
  account_id  STRING,
  ticker      STRING,
  trade_time  BIGINT,
  watermark_at_arrival_ms BIGINT,
  lateness_ms BIGINT
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'topic.late.s10',
  'properties.bootstrap.servers'  = 'workshop-kafka:9093',
  'format'                        = 'json',
  'sink.delivery-guarantee'       = 'at-least-once'
);

-- This INSERT runs continuously: any record whose `trade_ts` is older than the
-- watermark at arrival time is funneled into `topic.late.s10` for audit/replay.
INSERT INTO kafka_late_trades
SELECT
  event_id,
  account_id,
  ticker,
  trade_time,
  UNIX_TIMESTAMP_MILLIS(CURRENT_WATERMARK(trade_ts))                                AS watermark_at_arrival_ms,
  UNIX_TIMESTAMP_MILLIS(CURRENT_WATERMARK(trade_ts)) - trade_time                   AS lateness_ms
FROM kafka_trades_eventtime
WHERE CURRENT_WATERMARK(trade_ts) IS NOT NULL
  AND trade_ts < CURRENT_WATERMARK(trade_ts);
