-- Scenario 10 — Step 07 (deployable variant)
-- Route records arriving after the current watermark to a dedicated topic.
-- This uses CURRENT_WATERMARK() (Flink 1.17+) as a per-record predicate.

CREATE TEMPORARY TABLE kafka_trades_eventtime (
  event_id    STRING,
  account_id  STRING,
  ticker      STRING,
  trade_time  BIGINT,
  trade_ts    AS TO_TIMESTAMP_LTZ(trade_time, 3),
  WATERMARK FOR trade_ts AS trade_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'topic.in',
  'properties.bootstrap.servers'  = 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
  'properties.group.id'           = 'sql-s10-07-late',
  'scan.startup.mode'             = 'latest-offset',
  'format'                        = 'json',
  'json.ignore-parse-errors'      = 'true'
);

CREATE TEMPORARY TABLE blackhole_sink (
  event_id                STRING,
  account_id              STRING,
  ticker                  STRING,
  trade_time              BIGINT,
  watermark_at_arrival_ms BIGINT,
  lateness_ms             BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO blackhole_sink
SELECT
  event_id,
  account_id,
  ticker,
  trade_time,
  UNIX_TIMESTAMP_MILLIS(CURRENT_WATERMARK(trade_ts))                AS watermark_at_arrival_ms,
  UNIX_TIMESTAMP_MILLIS(CURRENT_WATERMARK(trade_ts)) - trade_time   AS lateness_ms
FROM kafka_trades_eventtime
WHERE CURRENT_WATERMARK(trade_ts) IS NOT NULL
  AND trade_ts < CURRENT_WATERMARK(trade_ts);
