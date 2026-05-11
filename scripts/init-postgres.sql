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
