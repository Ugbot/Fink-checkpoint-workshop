-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 11 — Step 06: Where Kafka compacted topics struggle, Fluss shines
-- ════════════════════════════════════════════════════════════════════════════
--
-- Kafka compacted topics are the closest Kafka-native analogue to a Fluss
-- primary-key table. Both promise "one record per key, last write wins".
-- In practice, compacted topics work less well than people expect once you
-- need to actually USE them as a queryable store. This file walks through
-- three queries that work cleanly on a Fluss PK table and trip over
-- well-known Kafka compaction footguns.
--
-- You do NOT need to actually create the Kafka compacted topic to follow
-- this file — the comparison is conceptual. The CREATE TABLE block below
-- defines both shapes so the contrast is concrete.

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

-- ── A hypothetical Kafka compacted topic shape ────────────────────────────
-- This is what a Kafka-native version of `account_profile` would look like.
-- Note: there is no PRIMARY KEY declaration — Kafka compaction operates
-- on the message KEY (a bytes blob) and the planner has no clue what's
-- actually inside a record's VALUE.
CREATE TEMPORARY TABLE kafka_account_profile_compacted (
    account_id     STRING,
    tier           STRING,
    region         STRING,
    risk_score     INT
    -- 26 more columns not shown — every one of them is just bytes inside
    -- the same opaque message value.
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'compacted.account_profile',
    'properties.bootstrap.servers' = 'workshop-kafka:9093',
    'properties.group.id'          = 'compaction-demo',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json'
);

-- ── PAIN POINT 1: Point lookup by key ─────────────────────────────────────
--
-- On Fluss (works, fast, single-record fetch):
SELECT * FROM account_profile WHERE account_id = 'ACC-0042';
--
-- On Kafka compacted topic (NEEDS A FULL SCAN to find one record):
SELECT * FROM kafka_account_profile_compacted WHERE account_id = 'ACC-0042';
--
-- The Kafka connector has no way to look up a record by message KEY through
-- SQL — it consumes from the offset you tell it (earliest, latest, or a
-- specific offset). So the query becomes a full topic scan that terminates
-- only when watermark + bounded scan finishes (it won't, on an unbounded
-- compacted topic without explicit bounding). In a real query workload
-- you'd build a materialised view in Flink first — i.e. you'd reinvent
-- "Fluss" by hand.

-- ── PAIN POINT 2: Batch aggregation by a non-PK column ────────────────────
--
-- "Average risk score per tier" — a standard analytics query.
--
-- On Fluss (works in batch mode, projection-pushdown only reads `tier` and
-- `risk_score`, ignores the other 28 columns):
SET 'execution.runtime-mode' = 'batch';
SELECT tier, AVG(risk_score) AS avg_risk
FROM account_profile
GROUP BY tier;
--
-- On Kafka compacted topic:
--   - The compaction may not have caught up: you'll see both the OLD and
--     NEW value of an account that was updated, depending on how long ago
--     compaction last ran.
--   - The Kafka connector has no projection pushdown for JSON values — the
--     whole value blob is parsed for every record.
--   - There's no bound on the scan: the query keeps running until you give
--     up. Same query, same data, but you've left "queryable storage" and
--     entered "manually consumed pile of records".

-- ── PAIN POINT 3: Partial updates ─────────────────────────────────────────
--
-- A common request: "update only the AML fields for an account, leave KYC
-- alone".
--
-- On Fluss (covered in detail in scenario 13 — partial updates write only
-- the columns you specify, other columns keep their previous value):
INSERT INTO customer_360 (customer_id, aml_status, risk_score, updated_at)
VALUES ('CUST-000123', 'FLAGGED', 87, 1747900000000);
-- ↑ This INSERT touches 4 columns. The remaining ~21 columns of
-- customer_360 are NOT overwritten. The KYC team's contribution survives.
--
-- On Kafka compacted topic:
--   - Every produced record is a WHOLE-VALUE upsert. You can't write "just
--     the AML columns" — you'd be writing a record with the AML columns
--     filled in and the other columns NULL, then compaction would overwrite
--     the entire previous record. The KYC data is gone.
--   - Workaround: a streaming Flink job that reads the compacted topic,
--     merges with the incoming partial event, and writes the merged value
--     back. You've just built what scenario 13 builds in 20 lines of SQL.

-- ── Summary ───────────────────────────────────────────────────────────────
--
-- Compacted Kafka topics are great for:
--   - "Latest config per key" replayed at app startup.
--   - Whole-value upserts as a stream.
--   - Cases where the consumer always reads the whole topic.
--
-- Fluss PK tables add to that:
--   - Point lookups by key without re-reading the world.
--   - Projection / filter pushdown (only fetch the columns you need).
--   - Partial-column updates with predictable merge semantics.
--   - Streaming + batch reads of the same table.
--   - Lookup joins from streaming queries (scenarios 14, 15).
--
-- When you find yourself "building a lookup service on top of a compacted
-- topic with a sidecar to materialise it" — that's the workload Fluss was
-- built for.
