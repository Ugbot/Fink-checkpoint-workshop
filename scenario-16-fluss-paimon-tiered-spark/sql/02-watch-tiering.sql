-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 16 — Step 02: Watch tiering happen
-- ════════════════════════════════════════════════════════════════════════════
--
-- After step 01 enabled tiering, the Fluss coordinator's background
-- tiering service writes hot data to Paimon at most every
-- `table.datalake.freshness` (1 min in our config).
--
-- We can observe this from the Paimon side directly via scenario 06's
-- existing Paimon catalog — proof that the lake-side mirror is just plain
-- Paimon.

-- ── Register the Paimon catalog (from scenario 06) ─────────────────────────
CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type'                 = 'paimon',
  'warehouse'            = 's3a://paimon/warehouse',
  's3.endpoint'          = 'http://workshop-minio:9000',
  's3.access-key'        = 'workshop',
  's3.secret-key'        = 'workshopsecret',
  's3.path.style.access' = 'true'
);

-- ── List Paimon tables — trade_log_wide should appear ──────────────────────
USE CATALOG paimon;
SHOW DATABASES;
USE workshop;
SHOW TABLES;
-- Expected to include `trade_log_wide` plus the existing `trades` from
-- scenario 06.

-- ── Inspect the mirror's schema — note the three system columns ────────────
DESCRIBE trade_log_wide;
-- Expected columns: all 25 user columns + __bucket + __offset + __timestamp

-- ── Count rows in the Paimon mirror ────────────────────────────────────────
SET 'execution.runtime-mode' = 'batch';
SELECT COUNT(*) FROM trade_log_wide;
-- This is the count of *tiered* rows only. If the tiering hasn't fired
-- yet (give it ~1 minute after step 01 was issued), the count is 0.

-- ── Per-bucket distribution ───────────────────────────────────────────────
SELECT __bucket, COUNT(*) AS row_count, MIN(__offset) AS min_offset, MAX(__offset) AS max_offset
FROM trade_log_wide
GROUP BY __bucket
ORDER BY __bucket;
-- The four buckets (matching `'bucket.num' = '4'` from step 01) appear,
-- each with its own offset range.

-- ── Time-travel via Paimon snapshots ──────────────────────────────────────
SELECT snapshot_id, commit_time, total_record_count
FROM `trade_log_wide$snapshots`
ORDER BY snapshot_id DESC
LIMIT 5;
-- Paimon snapshots are the unit of visibility — each tiering commit
-- produces a snapshot.

-- ── Inspect actual files on disk via MinIO ────────────────────────────────
-- Run in another terminal:
--    podman exec workshop-minio mc ls -r local/paimon/warehouse/workshop.db/trade_log_wide/
-- You'll see:
--    bucket-0/data-xxxxx.parquet
--    bucket-1/data-xxxxx.parquet
--    ...
--    snapshot/snapshot-1
--    snapshot/snapshot-2
--    manifest/...
-- That's a vanilla Paimon table layout. Spark, Trino, anything that reads
-- Paimon can read this.
