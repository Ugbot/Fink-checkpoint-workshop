-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 13 — Step 03: AML and Marketing sources contribute their slices
-- ════════════════════════════════════════════════════════════════════════════
--
-- Continuing the customer_360 narrative. KYC wrote in step 02. Now AML
-- and Marketing each contribute their columns. The same row fills up
-- without any source overwriting the others' contributions.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

SET 'execution.runtime-mode' = 'batch';

-- ── AML system writes its slice ─────────────────────────────────────────────
INSERT INTO customer_360
    (customer_id, aml_status, risk_score, pep_flag, sanctions_flag,
     aml_reviewed_by, aml_reviewed_at, updated_at)
VALUES
    ('CUST-DEMO13', 'CLEAR', 18, false, false,
     'aml-bot-1', 1747920000000, 1747920000000);

SELECT customer_id, legal_name, kyc_status, aml_status, risk_score, segment
FROM customer_360 WHERE customer_id = 'CUST-DEMO13';
-- ↑ Expect: legal_name=Ada Lovelace (from KYC), kyc_status=VERIFIED (from
--   KYC), aml_status=CLEAR (from AML), risk_score=18 (from AML),
--   segment=NULL (Marketing hasn't written yet).

-- ── Marketing system writes its slice ───────────────────────────────────────
INSERT INTO customer_360
    (customer_id, marketing_opt_in, email_opt_in, sms_opt_in,
     preferred_channel, segment, acquisition_source,
     last_engagement_at, updated_at)
VALUES
    ('CUST-DEMO13', true, true, false,
     'EMAIL', 'VIP', 'REFERRAL',
     1747930000000, 1747930000000);

SELECT *
FROM customer_360 WHERE customer_id = 'CUST-DEMO13';
-- ↑ Expect: ALL columns populated. The single row has contributions from
--   three independent sources, each preserved.

-- ── What if the same source writes the same column twice? ──────────────────
-- The default merge engine is "last_write_wins" — the most-recent write
-- of a given column wins. So if AML re-classifies CUST-DEMO13:
INSERT INTO customer_360
    (customer_id, aml_status, risk_score, aml_reviewed_at, updated_at)
VALUES
    ('CUST-DEMO13', 'FLAGGED', 85, 1747940000000, 1747940000000);

SELECT customer_id, aml_status, risk_score, segment, legal_name
FROM customer_360 WHERE customer_id = 'CUST-DEMO13';
-- ↑ Expect: aml_status=FLAGGED, risk_score=85 (newest AML), segment=VIP
--   (untouched), legal_name=Ada Lovelace (untouched).

-- This is the partial-update guarantee at work:
--   - Each source can write its own slice without coordination.
--   - The most-recent write of any specific column wins.
--   - Columns nobody has written yet stay NULL.
--
-- In Kafka-land this requires a Flink job that reads each source's
-- changelog, materialises the merged row, and writes back to a topic.
-- Fluss does it natively with no operator state on your side.
