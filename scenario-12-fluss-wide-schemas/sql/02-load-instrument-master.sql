-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 12 — Step 02: Verify the seed populated `instrument_master`
-- ════════════════════════════════════════════════════════════════════════════
--
-- The workshop's `scripts/seed-fluss.sh` runs the FlussDatagenJob which
-- writes ~10,000 synthetic instruments into `instrument_master`. If you've
-- already done scenario 11 step 03, this seed is already running — there's
-- nothing to (re)create here.
--
-- This step does three things:
--   (a) Counts rows so you know the seed worked.
--   (b) Inspects a few sample rows so the column values look reasonable.
--   (c) Drops a sidebar on how to inspect the on-disk Fluss layout from
--       the host (useful for debugging tiering / bucket placement).
--
-- If COUNT(*) returns 0:
--   - Did you run `bash scripts/seed-fluss.sh` (or equivalent)?
--   - Is the FlussDatagenJob still healthy?  `podman ps | grep fluss`

CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);
USE CATALOG fluss;
USE workshop;

-- Use batch mode so COUNT terminates with a finite answer.
SET 'execution.runtime-mode' = 'batch';

-- ── (a) Row count ──────────────────────────────────────────────────────────
SELECT COUNT(*) AS instrument_count FROM instrument_master;
-- Expected: a number in the low thousands — the seed populates roughly 10k
-- instruments by default but the exact count depends on how long the
-- datagen has been running.

-- ── (b) Eyeball three rows across a small subset of columns ────────────────
SELECT
  isin, ticker, exchange, sector, country, currency, last_close,
  beta_1y, esg_score, active_flag
FROM instrument_master
LIMIT 3;
-- Note: we projected 10 of the 45 columns. In a Kafka topic this query
-- would still parse all 45 columns out of the JSON value. In Fluss only
-- those 10 columns are read off disk — that's the projection-pushdown
-- punchline that step 03 measures.

-- ── (c) Sidebar: peek at the on-disk layout ────────────────────────────────
--
-- Fluss tablet servers persist tablet data under their data directory
-- (configured via `data.dir` — `/tmp/fluss-data` in the workshop compose
-- file). The directory layout is:
--
--   /tmp/fluss-data/
--   └── <table-id>/                 # numeric id, see SHOW CREATE TABLE
--       └── <bucket-num>/           # 0..3 for instrument_master
--           ├── log/                # WAL segments
--           │   ├── 00000000000000000000.log
--           │   ├── 00000000000000000000.index
--           │   └── 00000000000000000000.timeindex
--           └── kv/                 # PK table state (RocksDB-style)
--               ├── 000001.sst
--               ├── MANIFEST-000001
--               └── ...
--
-- To poke around from the host:
--
--   # Find the tablet container name and the table id.
--   podman ps --format '{{.Names}}' | grep tablet
--   # → workshop-fluss-tablet-1   (or whatever your compose names it)
--
--   # Top-level: list all tables this tablet hosts.
--   podman exec workshop-fluss-tablet-1 ls /tmp/fluss-data/
--
--   # Drill into instrument_master (table ids are visible in the
--   # coordinator's logs or via the Fluss admin SDK):
--   podman exec workshop-fluss-tablet-1 ls /tmp/fluss-data/<table-id>/
--
--   # See log segment sizes — useful for confirming bucket distribution:
--   podman exec workshop-fluss-tablet-1 \
--       du -sh /tmp/fluss-data/<table-id>/*/log
--
-- Why this matters for the rest of the scenario:
--
--   - Each bucket's `log/` directory is a Kafka-style WAL: appends are fast,
--     scans are linear.
--   - Each bucket's `kv/` directory is the materialised PK index: point
--     lookups by isin go straight here without scanning the log.
--   - Projection pushdown (step 03) means only the BYTES for the projected
--     columns are loaded from the log files — even though the full row is
--     stored.
--
-- You don't NEED to inspect the on-disk files to work with Fluss — but
-- seeing them once helps lock in the mental model.
