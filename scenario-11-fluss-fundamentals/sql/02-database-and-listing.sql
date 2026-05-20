-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 11 — Step 02: Databases, tables, schemas
-- ════════════════════════════════════════════════════════════════════════════
--
-- Fluss is namespaced like most databases:
--
--     catalog  →  database  →  table  →  rows
--      "fluss"     "workshop"  "instrument_master"
--
-- Catalogs are a Flink concept — they're what tell Flink "here's a system
-- that has tables in it". A single Flink session can have many catalogs
-- (e.g. one Fluss catalog + one Paimon catalog + the built-in default).
-- Inside a catalog, databases are real first-class Fluss objects.
--
-- This step:
--   (a) Drops into the `workshop` database.
--   (b) Lists the tables that init-fluss.sh created for the workshop.
--   (c) Inspects a table's schema with DESCRIBE.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);

-- ── (a) Use the workshop database ──────────────────────────────────────────
USE CATALOG fluss;
USE workshop;

-- ── (b) List tables ────────────────────────────────────────────────────────
SHOW TABLES;
-- Expected (the workshop's standard set):
--   account_profile           — primary-key table (~30 cols)
--   customer_360              — primary-key table (~25 cols, partial-update target)
--   daily_volumes_agg         — primary-key table with aggregation merge engine
--   first_observed            — primary-key table with first_row merge engine
--   instrument_master         — primary-key table (~45 cols, security master)
--   trade_log_wide            — log table (~25 cols, append-only trades)
--   versioned_config          — primary-key table with versioned merge engine

-- ── (c) Inspect a schema ───────────────────────────────────────────────────
DESCRIBE instrument_master;
-- DESCRIBE shows the column list, types, primary key, and nullability.
-- The PRIMARY KEY (isin) NOT ENFORCED at the bottom is the key to PK-table
-- semantics: inserts upsert by isin, and `SELECT * WHERE isin = '...'` is
-- a cheap point lookup (we'll do that in step 04).
