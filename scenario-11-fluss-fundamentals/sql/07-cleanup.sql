-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 11 — Step 07: Cleanup (DROP TABLE / DROP DATABASE)
-- ════════════════════════════════════════════════════════════════════════════
--
-- Fluss supports the usual DDL lifecycle commands. They're idempotent with
-- the IF EXISTS qualifier. Drops cascade to physical segments on the next
-- tablet-server housekeeping pass.
--
-- For the workshop, you usually do NOT want to run these — the tables are
-- reused by scenarios 12–16. This file exists so learners see the symmetric
-- DDL.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

-- ── Drop a single table ────────────────────────────────────────────────────
-- DROP TABLE IF EXISTS account_profile;

-- ── Drop the entire workshop database ──────────────────────────────────────
-- USE fluss;     -- step out of the database you want to drop
-- DROP DATABASE IF EXISTS workshop CASCADE;

-- ── Drop the catalog from the current Flink session ───────────────────────
-- DROP CATALOG IF EXISTS fluss;
-- Note: this only forgets the catalog *registration*. The Fluss cluster
-- and its data are unaffected.

-- ── Recreate everything ────────────────────────────────────────────────────
-- bash scripts/init-fluss.sh
-- bash scripts/seed-fluss.sh
