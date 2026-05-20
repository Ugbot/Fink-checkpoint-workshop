-- Scenario 10 — Step 03 (deployable variant)
-- 30-second processing-time tumble. Identical SELECT shape to step 04 so the
-- contrast with event-time semantics is visible side-by-side at deploy time.

CREATE TEMPORARY TABLE kafka_trades_proctime (
  event_id    STRING,
  account_id  STRING,
  ticker      STRING,
  side        STRING,
  quantity    INT,
  price       DOUBLE,
  trade_time  BIGINT,
  source_app  STRING,
  proc_ts     AS PROCTIME()
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'topic.in',
  'properties.bootstrap.servers'  = 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
  'properties.group.id'           = 'sql-s10-03-proctime-30s',
  'scan.startup.mode'             = 'latest-offset',
  'format'                        = 'json',
  'json.ignore-parse-errors'      = 'true'
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
  TUMBLE_START(proc_ts, INTERVAL '30' SECOND) AS window_start,
  TUMBLE_END  (proc_ts, INTERVAL '30' SECOND) AS window_end,
  COUNT(*) AS trade_count
FROM kafka_trades_proctime
GROUP BY ticker, TUMBLE(proc_ts, INTERVAL '30' SECOND);
