-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 11 — Step 04: Primary-key tables (the killer Fluss feature)
-- ════════════════════════════════════════════════════════════════════════════
--
-- A Fluss PRIMARY-KEY TABLE looks like a regular SQL table from outside:
-- INSERTs upsert by key, SELECT-WHERE-pk is a point lookup, the table holds
-- exactly one row per key at any given time.
--
-- INTERNALLY, a PK table is two things at once:
--   1. The CURRENT state: row-per-key, lookupable in O(log N) (used by the
--      lookup-join optimiser in scenario 14).
--   2. The CHANGELOG: every insert/update/delete produces a record on a
--      stream that downstream Flink jobs can consume (cdc-style).
--
-- This is the difference vs Kafka compacted topics (covered in detail in
-- step 06). Compacted topics give you the changelog but not a queryable
-- current-state view; Fluss gives you both.
--
-- The init job created `account_profile` as a 30-col PK table keyed on
-- `account_id`. This step:
--   (a) Inserts one row.
--   (b) Re-inserts with the same key — upsert wins, count remains 1.
--   (c) Point-lookup by primary key.
--   (d) Streaming changelog read — observe the +I / -U / +U markers.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

SET 'execution.runtime-mode' = 'batch';

-- ── (a) First insert ───────────────────────────────────────────────────────
INSERT INTO account_profile (
    account_id, customer_id, account_name, account_type, currency,
    tier, region, country, mifid_classification, fatca_status, tax_residency,
    kyc_status, kyc_verified_at, aml_status, risk_score, pep_flag, sanctions_flag,
    daily_trade_limit, monthly_trade_limit, credit_line, available_balance, margin_used,
    iban, swift_bic, custodian_bank, opened_at, last_login_at, updated_at,
    active_flag, source_system
) VALUES (
    'ACC-9001', 'CUST-900001', 'Demo Account', 'INDIVIDUAL', 'USD',
    'GOLD', 'NA', 'US', 'RETAIL', 'COMPLIANT', 'US',
    'VERIFIED', 1747800000000, 'CLEAR', 35, false, false,
    100000.0, 2000000.0, 50000.0, 25000.0, 0.0,
    'GB12-WSHP-0000009001', 'WSHPGB22', 'Workshop Custody', 1700000000000, 1747800000000, 1747800000000,
    true, 'manual-step-04');

SELECT account_id, tier, risk_score, available_balance FROM account_profile WHERE account_id = 'ACC-9001';
--  ACC-9001  GOLD  35  25000.00

-- ── (b) Upsert — same key, different values ────────────────────────────────
INSERT INTO account_profile (
    account_id, customer_id, account_name, account_type, currency,
    tier, region, country, mifid_classification, fatca_status, tax_residency,
    kyc_status, kyc_verified_at, aml_status, risk_score, pep_flag, sanctions_flag,
    daily_trade_limit, monthly_trade_limit, credit_line, available_balance, margin_used,
    iban, swift_bic, custodian_bank, opened_at, last_login_at, updated_at,
    active_flag, source_system
) VALUES (
    'ACC-9001', 'CUST-900001', 'Demo Account (upgraded)', 'INDIVIDUAL', 'USD',
    'PLATINUM', 'NA', 'US', 'PROFESSIONAL', 'COMPLIANT', 'US',
    'VERIFIED', 1747800000000, 'CLEAR', 10, false, false,
    500000.0, 10000000.0, 250000.0, 25000.0, 0.0,
    'GB12-WSHP-0000009001', 'WSHPGB22', 'Workshop Custody', 1700000000000, 1747900000000, 1747900000000,
    true, 'manual-step-04');

-- ── (c) Point lookup — the headline PK-table operation ─────────────────────
SELECT account_id, tier, risk_score FROM account_profile WHERE account_id = 'ACC-9001';
--  ACC-9001  PLATINUM  10
-- Note: one row, not two. Tier was upgraded GOLD → PLATINUM by the upsert.
-- The previous version is gone from the current-state view.

-- ── (d) Streaming changelog read ───────────────────────────────────────────
-- Switch to streaming and watch the same table — Fluss emits change events.
SET 'execution.runtime-mode' = 'streaming';
-- This SELECT keeps running. As background datagen writes additional rows,
-- they appear here as +I (insert), -U (retract old), +U (new state).
-- The SQL Client shows the row-kind in the leftmost column when you scroll.
SELECT account_id, tier, risk_score
FROM account_profile
WHERE account_id = 'ACC-9001';
