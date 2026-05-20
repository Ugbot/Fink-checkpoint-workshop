-- Deployable variant of scenario 14 / step 04.
-- The same enrichment expressed as a REGULAR streaming join over the
-- Fluss changelog instead of a lookup join. Provided as a deployable so
-- operators can compare checkpoint size and operator state against the
-- step-01 / step-03 deployments side by side in the Flink UI. See
-- ../sql/04-regular-join-vs-lookup-join.sql for the teaching narrative.

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
  'properties.group.id'          = 'sql-s14-04-deploy',
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

-- Note: no FOR SYSTEM_TIME AS OF clause. This is a regular streaming
-- join; the Fluss table is read as a changelog stream and the join
-- operator holds state on both sides.
INSERT INTO blackhole_sink
SELECT
  t.event_id,
  t.ticker,
  t.quantity,
  im.sector,
  im.last_close,
  im.beta_1y
FROM kafka_trades t
JOIN fluss.workshop.instrument_master AS im
  ON t.ticker = im.ticker;
