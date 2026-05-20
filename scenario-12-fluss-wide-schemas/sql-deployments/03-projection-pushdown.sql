-- Deployable variant of step 03: projection pushdown.
-- The teaching file runs two EXPLAINs and two SELECTs in the SQL Client.
-- This deployable variant turns the projected query (the interesting one)
-- into a streaming job that writes into a blackhole sink so VVC accepts it.
--
-- We deliberately keep the runtime mode at the default (streaming) so the
-- job stays running and the SQL Gateway / VVC harness can observe its
-- "RUNNING" state. The same projection pushdown applies in streaming mode.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

CREATE TEMPORARY TABLE blackhole_sink (
    isin       STRING,
    ticker     STRING,
    last_close DOUBLE
) WITH ('connector' = 'blackhole');

INSERT INTO blackhole_sink
SELECT isin, ticker, last_close
FROM instrument_master
WHERE country = 'US';
