-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 16 — Step 01: Enable tiered storage from Fluss to Paimon
-- ════════════════════════════════════════════════════════════════════════════
--
-- Fluss can automatically tier data from its native segments to a Paimon
-- table on S3-compatible storage. This is configured at TWO levels:
--
--   Cluster level (in podman-compose.yml's fluss-coordinator config):
--     datalake.format: paimon
--     datalake.paimon.metastore: filesystem
--     datalake.paimon.warehouse: s3a://paimon/warehouse
--     datalake.paimon.s3.endpoint / access-key / secret-key / path-style-access
--
--   Table level (per Fluss table):
--     'table.datalake.enabled' = 'true'        -- opt the table in
--     'table.datalake.freshness' = '1 min'     -- max staleness in Paimon
--
-- Tiering happens in the background. When the Fluss tiering service runs,
-- it writes hot data to the Paimon mirror table. Reads of the Fluss table
-- transparently union hot (Fluss) + cold (Paimon) for queries.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

-- ── Drop and recreate trade_log_wide WITH tiering enabled ──────────────────
-- (The init job created it without tiering so scenarios 12 and 14 don't
--  incur tiering overhead. Scenario 16 alters that.)
DROP TABLE IF EXISTS trade_log_wide;

CREATE TABLE trade_log_wide (
    event_id            STRING,
    account_id          STRING,
    customer_id         STRING,
    ticker              STRING,
    isin                STRING,
    side                STRING,
    order_type          STRING,
    quantity            INT,
    price               DOUBLE,
    notional            DOUBLE,
    currency            STRING,
    fx_rate_to_usd      DOUBLE,
    notional_usd        DOUBLE,
    exchange            STRING,
    venue               STRING,
    sector              STRING,
    country             STRING,
    desk                STRING,
    trader              STRING,
    trade_time          BIGINT,
    settled_at          BIGINT,
    settlement_status   STRING,
    commission          DOUBLE,
    tax                 DOUBLE,
    source_app          STRING
) WITH (
    'bucket.num'              = '4',
    'table.datalake.enabled'  = 'true',
    'table.datalake.freshness' = '1 min'
);

-- After this DDL completes, the Fluss coordinator automatically creates
-- a Paimon mirror table at:
--    s3a://paimon/warehouse/workshop.db/trade_log_wide
-- with the same schema PLUS three system columns added by Fluss:
--    __bucket    INT
--    __offset    BIGINT
--    __timestamp TIMESTAMP_LTZ(3)
-- Those columns let the lakehouse-side know which Fluss bucket+offset
-- each tiered row came from.

SHOW CREATE TABLE trade_log_wide;
