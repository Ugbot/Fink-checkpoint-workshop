-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 13 — Step 01: customer_360 PK table DDL
-- ════════════════════════════════════════════════════════════════════════════
--
-- The "customer 360" pattern: one row per customer, with columns sourced
-- from multiple upstream systems. Three sources contribute in our demo:
--
--   - KYC system   →  legal_name, dob, nationality, kyc_status, …
--   - AML system   →  aml_status, risk_score, pep_flag, sanctions_flag, …
--   - Marketing    →  marketing_opt_in, preferred_channel, segment, …
--
-- No single source has the whole row, and crucially: source A's writes
-- must NEVER stomp on source B's columns.
--
-- This is exactly what Fluss PK tables with partial updates solve. The
-- DDL below was created by `FlussInitJob` — this file just verifies it
-- and explains what each option means.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

-- ── Show the existing definition ────────────────────────────────────────────
SHOW CREATE TABLE customer_360;
DESCRIBE customer_360;

-- ── Notes on the DDL ────────────────────────────────────────────────────────
--
--   PRIMARY KEY (customer_id) NOT ENFORCED
--      Identifies the merge key. Two INSERTs with the same customer_id
--      will upsert; the default merge engine is `last_write_wins`, but
--      partial updates still preserve unspecified columns (see step 02).
--
--   'bucket.num' = '4'
--      Concurrency unit for parallel writes / reads. Workshop default.
--
-- The DDL did NOT set 'table.merge-engine'. Without that option, Fluss
-- uses its default merge strategy: each upsert overwrites the columns
-- you list, leaves the rest of the row unchanged.
--
-- Step 04 recreates this table with `'table.merge-engine' = 'aggregation'`
-- and per-column aggregate-function options. Step 05 shows `first_row`
-- and `versioned`. The default merge engine in this step is exactly the
-- partial-update behaviour customers want for 360-degree views.
