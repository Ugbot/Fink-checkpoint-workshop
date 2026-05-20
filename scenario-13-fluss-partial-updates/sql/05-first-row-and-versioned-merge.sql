-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 13 — Step 05: first_row and versioned merge engines
-- ════════════════════════════════════════════════════════════════════════════
--
-- Two more merge engines that solve specific problems:
--
--   first_row  — keep the FIRST observed row per PK; subsequent writes
--                are ignored. Use case: deduplication / event idempotency
--                where you want the original event preserved.
--
--   versioned  — the row with the highest value of a designated `version`
--                column wins. Use case: handling out-of-order updates
--                where a stale write must not overwrite a newer one.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;
SET 'execution.runtime-mode' = 'batch';

-- ── first_row demo ─────────────────────────────────────────────────────────
SHOW CREATE TABLE first_observed;

INSERT INTO first_observed (event_id, first_seen_at, source_system, payload)
VALUES ('EVT-001', 1747900000000, 'router-a', 'original payload');

INSERT INTO first_observed (event_id, first_seen_at, source_system, payload)
VALUES ('EVT-001', 1747910000000, 'router-b', 'duplicate from another router');

SELECT * FROM first_observed WHERE event_id = 'EVT-001';
-- ↑ Expect: source_system=router-a, payload='original payload'.
-- The second INSERT was silently dropped because the first_row engine
-- keeps only the first observed write per PK.

-- ── versioned demo ─────────────────────────────────────────────────────────
SHOW CREATE TABLE versioned_config;

-- Initial write at version 5
INSERT INTO versioned_config (config_key, version, config_value, updated_at)
VALUES ('rate_limit', 5, '1000 rps', 1747900000000);

-- A STALE write at version 3 arrives later — should be ignored
INSERT INTO versioned_config (config_key, version, config_value, updated_at)
VALUES ('rate_limit', 3, '500 rps (stale)', 1747910000000);

-- A NEWER write at version 7 arrives — should win
INSERT INTO versioned_config (config_key, version, config_value, updated_at)
VALUES ('rate_limit', 7, '2000 rps', 1747920000000);

SELECT * FROM versioned_config WHERE config_key = 'rate_limit';
-- ↑ Expect:
--    version       7
--    config_value  '2000 rps'
-- The version-3 write was correctly ignored because version 5 already
-- existed; the version-7 write overrode version 5.

-- ── Why these matter ──────────────────────────────────────────────────────
-- Both engines solve problems that, in Kafka land, require Flink jobs
-- with stateful operators:
--   first_row  ≈ deduplication function (see scenario 04's TTL dedup)
--   versioned  ≈ "highest-version wins" CDC pattern
-- With Fluss, the storage layer does it.
