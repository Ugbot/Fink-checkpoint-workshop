-- Used by scenario-03: external sink duplicate demonstration
-- Mounted into the Postgres container as /docker-entrypoint-initdb.d/init.sql

CREATE TABLE IF NOT EXISTS processed_trades (
    event_id     VARCHAR(36)  PRIMARY KEY,
    account_id   VARCHAR(20)  NOT NULL,
    ticker       VARCHAR(10)  NOT NULL,
    net_qty      INTEGER      NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Convenience view: shows any eventId that appears more than once.
-- In buggy mode (plain INSERT) this will return rows after a crash+restart.
-- In fixed mode (upsert) this will always return 0 rows.
CREATE VIEW duplicate_trades AS
SELECT event_id, COUNT(*) AS cnt
FROM processed_trades
GROUP BY event_id
HAVING COUNT(*) > 1;

-- ── Scenarios 07–09 (joins) ──────────────────────────────────────────────────

-- Slow-changing dimension table for the lookup-join demos (scenarios 08a, 09).
-- Populated by com.workshop.flink.common.setup.AccountSeedJob.
CREATE TABLE IF NOT EXISTS accounts (
    account_id    VARCHAR(20)  PRIMARY KEY,
    account_name  VARCHAR(64)  NOT NULL,
    tier          VARCHAR(16)  NOT NULL,
    region        VARCHAR(16)  NOT NULL,
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Sink for the FULL OUTER join in scenario 07a — records fills that arrive
-- without a corresponding order. The job upserts on event_id so replays are idempotent.
CREATE TABLE IF NOT EXISTS fills_orphan_log (
    event_id    VARCHAR(36)  PRIMARY KEY,
    ticker      VARCHAR(10)  NOT NULL,
    fill_time   TIMESTAMPTZ  NOT NULL,
    reason      VARCHAR(64)  NOT NULL,
    logged_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
