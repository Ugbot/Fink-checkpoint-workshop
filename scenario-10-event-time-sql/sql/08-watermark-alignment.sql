-- Step 8 — Bound the gap between fast and slow partitions.
--
-- Lessons:
--   • The operator's watermark = MIN(per-partition watermarks). If one partition is
--     hours ahead of another, all window state for the leading partition piles up
--     waiting for the laggard. Joins of two streams compound this risk.
--   • Watermark alignment groups partitions together and forces fast ones to stall
--     when they get too far ahead of slow ones. This bounds in-flight state size at
--     the cost of throughput on the leading partition.
--   • In SQL: declare a group name and a maximum allowed drift. All sources sharing
--     the same group name align together.

DROP TABLE IF EXISTS kafka_trades_aligned;

CREATE TABLE kafka_trades_aligned (
  event_id   STRING,
  ticker     STRING,
  trade_time BIGINT,
  trade_ts   AS TO_TIMESTAMP_LTZ(trade_time, 3),
  WATERMARK FOR trade_ts AS trade_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                                = 'kafka',
  'topic'                                    = 'topic.in',
  'properties.bootstrap.servers'             = 'workshop-kafka:9093',
  'properties.group.id'                      = 'sql-s10-aligned',
  'scan.startup.mode'                        = 'earliest-offset',
  'format'                                   = 'json',
  'json.ignore-parse-errors'                 = 'true',
  'scan.watermark.alignment.group'           = 'trades-aligned',
  'scan.watermark.alignment.max-drift'       = '30 s',
  'scan.watermark.alignment.update-interval' = '1 s'
);

-- A second source in the same alignment group co-stalls when one is ahead by >30s.
-- Useful when joining (e.g., scenario 09's interval+temporal joins).
DROP TABLE IF EXISTS kafka_quotes_aligned;

CREATE TABLE kafka_quotes_aligned (
  quote_id   STRING,
  ticker     STRING,
  bid        DOUBLE,
  ask        DOUBLE,
  quote_time BIGINT,
  quote_ts   AS TO_TIMESTAMP_LTZ(quote_time, 3),
  WATERMARK FOR quote_ts AS quote_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                                = 'kafka',
  'topic'                                    = 'topic.quotes',
  'properties.bootstrap.servers'             = 'workshop-kafka:9093',
  'properties.group.id'                      = 'sql-s10-quotes-aligned',
  'scan.startup.mode'                        = 'earliest-offset',
  'format'                                   = 'json',
  'json.ignore-parse-errors'                 = 'true',
  'scan.watermark.alignment.group'           = 'trades-aligned',
  'scan.watermark.alignment.max-drift'       = '30 s',
  'scan.watermark.alignment.update-interval' = '1 s'
);
