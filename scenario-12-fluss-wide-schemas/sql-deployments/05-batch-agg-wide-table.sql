-- Deployable variant of step 05: batch aggregation over the 45-column
-- instrument_master, written into a blackhole sink so VVC accepts the
-- pipeline. Projection pushdown reads only the 3 columns the query touches.

SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

CREATE TEMPORARY TABLE blackhole_sink (
    sector           STRING,
    instrument_count BIGINT,
    avg_beta_1y      DOUBLE,
    avg_esg_score    DOUBLE
) WITH ('connector' = 'blackhole');

INSERT INTO blackhole_sink
SELECT
  sector,
  COUNT(*)        AS instrument_count,
  AVG(beta_1y)    AS avg_beta_1y,
  AVG(esg_score)  AS avg_esg_score
FROM instrument_master
GROUP BY sector;
