-- Deployable variant of scenario 14 / step 02.
-- Three-sided lookup join: Kafka trades enriched with two Fluss PK tables
-- and one Postgres JDBC dim table. See ../sql/02-lookup-join-multi-side.sql
-- for the heavily-commented teaching version.

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
  'properties.group.id'          = 'sql-s14-02-deploy',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

CREATE TEMPORARY TABLE pg_accounts (
  account_id   STRING,
  account_name STRING,
  tier         STRING,
  region       STRING,
  PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
  'connector'  = 'jdbc',
  'url'        = 'jdbc:postgresql://workshop-postgres:5432/workshop',
  'table-name' = 'accounts',
  'username'   = 'workshop',
  'password'   = 'workshop',
  'lookup.cache'                              = 'PARTIAL',
  'lookup.partial-cache.max-rows'             = '1000',
  'lookup.partial-cache.expire-after-write'   = '5 min'
);

CREATE TEMPORARY TABLE blackhole_sink (
  event_id          STRING,
  account_id        STRING,
  ticker            STRING,
  sector            STRING,
  last_close        DOUBLE,
  fluss_tier        STRING,
  fluss_risk_score  INT,
  pg_tier           STRING,
  pg_region         STRING
) WITH ('connector' = 'blackhole');

INSERT INTO blackhole_sink
SELECT
  t.event_id,
  t.account_id,
  t.ticker,
  im.sector,
  im.last_close,
  ap.tier         AS fluss_tier,
  ap.risk_score   AS fluss_risk_score,
  pg.tier         AS pg_tier,
  pg.region       AS pg_region
FROM kafka_trades t
LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time AS im
       ON t.ticker     = im.ticker
LEFT JOIN fluss.workshop.account_profile   FOR SYSTEM_TIME AS OF t.proc_time AS ap
       ON t.account_id = ap.account_id
LEFT JOIN pg_accounts                       FOR SYSTEM_TIME AS OF t.proc_time AS pg
       ON t.account_id = pg.account_id;
