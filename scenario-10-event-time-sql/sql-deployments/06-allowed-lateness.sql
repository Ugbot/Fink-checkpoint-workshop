-- Scenario 10 — Step 06 (deployable variant)
-- Allowed-lateness produces a retract+revise stream. Blackhole accepts retracts.
-- Note: the SET statement applies to the SQL Gateway session — VVC sets it
-- via flinkConfiguration in the deployment spec. For inline-script deployments
-- prefer the deployment's flinkConfiguration; we keep the SET here so the file
-- is also runnable from a SQL Client.

SET 'table.exec.window-allowed-lateness' = '1 min';

CREATE TEMPORARY TABLE kafka_trades_eventtime (
  event_id    STRING,
  account_id  STRING,
  ticker      STRING,
  quantity    INT,
  price       DOUBLE,
  trade_time  BIGINT,
  trade_ts    AS TO_TIMESTAMP_LTZ(trade_time, 3),
  WATERMARK FOR trade_ts AS trade_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'topic.in',
  'properties.bootstrap.servers'  = 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
  'properties.group.id'           = 'sql-s10-06-lateness',
  'scan.startup.mode'             = 'latest-offset',
  'format'                        = 'json',
  'json.ignore-parse-errors'      = 'true'
);

CREATE TEMPORARY TABLE blackhole_sink (
  ticker        STRING,
  window_start  TIMESTAMP_LTZ(3),
  trade_count   BIGINT,
  PRIMARY KEY (ticker, window_start) NOT ENFORCED
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO blackhole_sink
SELECT
  ticker,
  TUMBLE_START(trade_ts, INTERVAL '30' SECOND) AS window_start,
  COUNT(*) AS trade_count
FROM kafka_trades_eventtime
GROUP BY ticker, TUMBLE(trade_ts, INTERVAL '30' SECOND);
