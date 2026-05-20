-- Deployable variant of scenario 14 / step 01.
-- Lookup join from a Kafka trade stream into the Fluss instrument_master
-- PK table. The teaching SQL (../sql/01-lookup-join-instrument-master.sql)
-- ends with a bare SELECT — for VVC deployment we sink to blackhole so
-- the job is a complete streaming pipeline.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);

CREATE TEMPORARY TABLE kafka_trades (
  event_id    STRING,
  account_id  STRING,
  ticker      STRING,
  side        STRING,
  quantity    INT,
  price       DOUBLE,
  trade_time  BIGINT,
  proc_time   AS PROCTIME()
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'topic.in',
  'properties.bootstrap.servers' = 'workshop-kafka:9093',
  'properties.group.id'          = 'sql-s14-01-deploy',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

CREATE TEMPORARY TABLE blackhole_sink (
  event_id   STRING,
  ticker     STRING,
  quantity   INT,
  sector     STRING,
  last_close DOUBLE,
  beta_1y    DOUBLE
) WITH ('connector' = 'blackhole');

INSERT INTO blackhole_sink
SELECT
  t.event_id,
  t.ticker,
  t.quantity,
  im.sector,
  im.last_close,
  im.beta_1y
FROM kafka_trades t
LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im
  ON t.ticker = im.ticker;
