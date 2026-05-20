-- Scenario 10 — Step 08 (deployable variant)
-- Two Kafka sources in the same alignment group; a trade ⋈ quote interval
-- join. Demonstrates how alignment bounds in-flight join state when one
-- partition runs ahead of another.

CREATE TEMPORARY TABLE kafka_trades_aligned (
  event_id   STRING,
  ticker     STRING,
  quantity   INT,
  price      DOUBLE,
  trade_time BIGINT,
  trade_ts   AS TO_TIMESTAMP_LTZ(trade_time, 3),
  WATERMARK FOR trade_ts AS trade_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                                = 'kafka',
  'topic'                                    = 'topic.in',
  'properties.bootstrap.servers'             = 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
  'properties.group.id'                      = 'sql-s10-08-trades',
  'scan.startup.mode'                        = 'latest-offset',
  'format'                                   = 'json',
  'json.ignore-parse-errors'                 = 'true',
  'scan.watermark.alignment.group'           = 's10-aligned',
  'scan.watermark.alignment.max-drift'       = '30 s',
  'scan.watermark.alignment.update-interval' = '1 s'
);

CREATE TEMPORARY TABLE kafka_quotes_aligned (
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
  'properties.bootstrap.servers'             = 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
  'properties.group.id'                      = 'sql-s10-08-quotes',
  'scan.startup.mode'                        = 'latest-offset',
  'format'                                   = 'json',
  'json.ignore-parse-errors'                 = 'true',
  'scan.watermark.alignment.group'           = 's10-aligned',
  'scan.watermark.alignment.max-drift'       = '30 s',
  'scan.watermark.alignment.update-interval' = '1 s'
);

CREATE TEMPORARY TABLE blackhole_sink (
  ticker      STRING,
  trade_time  TIMESTAMP_LTZ(3),
  quote_time  TIMESTAMP_LTZ(3),
  price       DOUBLE,
  bid         DOUBLE,
  ask         DOUBLE
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO blackhole_sink
SELECT
  t.ticker,
  t.trade_ts AS trade_time,
  q.quote_ts AS quote_time,
  t.price,
  q.bid,
  q.ask
FROM kafka_trades_aligned t, kafka_quotes_aligned q
WHERE t.ticker = q.ticker
  AND q.quote_ts BETWEEN t.trade_ts - INTERVAL '5' SECOND
                     AND t.trade_ts + INTERVAL '5' SECOND;
