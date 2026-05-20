-- Deployable variant of step 05: batch aggregation over Fluss, terminated
-- by an INSERT INTO a blackhole sink so the job is a valid VVC deployment.
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

CREATE TEMPORARY TABLE blackhole_sink (
    account_id      STRING,
    trade_count     BIGINT,
    total_qty       BIGINT,
    total_notional  DOUBLE,
    max_price       DOUBLE,
    min_price       DOUBLE
) WITH ('connector' = 'blackhole');

INSERT INTO blackhole_sink
SELECT
  account_id,
  COUNT(*)        AS trade_count,
  SUM(quantity)   AS total_qty,
  SUM(notional)   AS total_notional,
  MAX(price)      AS max_price,
  MIN(price)      AS min_price
FROM trade_log_wide
GROUP BY account_id;
