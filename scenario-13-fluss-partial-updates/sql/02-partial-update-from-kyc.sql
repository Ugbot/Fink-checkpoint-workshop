-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 13 — Step 02: Partial update from the KYC source
-- ════════════════════════════════════════════════════════════════════════════
--
-- The KYC subsystem only knows about the identity / residency columns of
-- a customer. It writes ONLY those columns; everything else is left to
-- the other source systems.
--
-- The key SQL trick: list ONLY the columns you want to set in INSERT.
-- Fluss treats the unlisted columns as "don't touch" — it merges the new
-- partial row into the existing one, preserving whatever other sources
-- wrote previously.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

SET 'execution.runtime-mode' = 'batch';

-- ── Setup: start from a known clean state for CUST-DEMO13 ──────────────────
-- This row doesn't exist yet (or if it does from a prior run, the upsert
-- below will refresh it).
INSERT INTO customer_360
    (customer_id, legal_name, kyc_status, kyc_verified_at, nationality, updated_at)
VALUES
    ('CUST-DEMO13', 'Initial Name', 'PENDING', 0, 'XX', 1747900000000);

SELECT * FROM customer_360 WHERE customer_id = 'CUST-DEMO13';
-- ↑ Expect: legal_name=Initial Name, kyc_status=PENDING, all other cols NULL.

-- ── The KYC source writes its slice ─────────────────────────────────────────
-- Notice: only kyc-related columns + the PK.
INSERT INTO customer_360
    (customer_id, legal_name, date_of_birth, nationality, tax_residency,
     kyc_status, kyc_verified_at,
     id_document_type, id_document_number,
     address_line, address_country,
     updated_at)
VALUES
    ('CUST-DEMO13', 'Ada Lovelace', '1980-12-10', 'GB', 'GB',
     'VERIFIED', 1747910000000,
     'PASSPORT', 'GB-1234-5678',
     '12 Acacia Avenue', 'GB',
     1747910000000);

SELECT * FROM customer_360 WHERE customer_id = 'CUST-DEMO13';
-- ↑ Expect: KYC cols populated; AML cols + marketing cols still NULL.
--   This is the partial-update guarantee: KYC didn't overwrite AML or
--   marketing with NULLs, because it didn't list those columns.
