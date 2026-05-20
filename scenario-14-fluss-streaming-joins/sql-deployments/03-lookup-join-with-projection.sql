-- Deployable variant of scenario 14 / step 03.
-- Narrow lookup join: two columns from each wide PK table demonstrates
-- projection pushdown into the Fluss lookup path. See
-- ../sql/03-lookup-join-with-projection.sql for the teaching version.

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
  'properties.group.id'          = 'sql-s14-03-deploy',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

CREATE TEMPORARY TABLE blackhole_sink (
  event_id   STRING,
  ticker     STRING,
  sector     STRING,
  last_close DOUBLE,
  tier       STRING,
  risk_score INT
) WITH ('connector' = 'blackhole');

INSERT INTO blackhole_sink
SELECT
  t.event_id,
  t.ticker,
  im.sector,
  im.last_close,
  ap.tier,
  ap.risk_score
FROM kafka_trades t
LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im
       ON t.ticker     = im.ticker
LEFT JOIN fluss.workshop.account_profile   FOR SYSTEM_TIME AS OF t.proc_time AS ap
       ON t.account_id = ap.account_id;
