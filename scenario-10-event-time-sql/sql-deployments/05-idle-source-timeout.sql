-- Scenario 10 — Step 05 (deployable variant)
-- Event-time source with `scan.watermark.idle-timeout` so the operator's
-- watermark keeps moving when one Kafka partition stops emitting. Sink the
-- 30-second event-time count to blackhole.

CREATE TEMPORARY TABLE kafka_trades_eventtime_idle (
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
  'properties.bootstrap.servers'    = 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
  'properties.group.id'             = 'sql-s10-05-idle',
  'scan.startup.mode'               = 'latest-offset',
  'format'                          = 'json',
  'json.ignore-parse-errors'        = 'true',
  'scan.watermark.idle-timeout'     = '30 s'
);

CREATE TEMPORARY TABLE blackhole_sink (
  ticker        STRING,
  window_start  TIMESTAMP_LTZ(3),
  window_end    TIMESTAMP_LTZ(3),
  trade_count   BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO blackhole_sink
SELECT
  ticker,
  TUMBLE_START(trade_ts, INTERVAL '30' SECOND) AS window_start,
  TUMBLE_END  (trade_ts, INTERVAL '30' SECOND) AS window_end,
  COUNT(*) AS trade_count
FROM kafka_trades_eventtime_idle
GROUP BY ticker, TUMBLE(trade_ts, INTERVAL '30' SECOND);
