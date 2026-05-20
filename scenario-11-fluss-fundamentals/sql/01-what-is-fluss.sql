-- ════════════════════════════════════════════════════════════════════════════
-- Scenario 11 — Step 01: What IS Apache Fluss?
-- ════════════════════════════════════════════════════════════════════════════
--
-- Apache Fluss (incubating) is a **streaming storage system designed for
-- Apache Flink**. If you've used Kafka before, the easiest mental model is:
--
--    Kafka                              Fluss
--    ─────                              ─────
--    Topics                             Tables (log + primary-key flavours)
--    Partitions                         Buckets
--    Records (opaque key + value)       Rows with typed columns + schema
--    Brokers                            Coordinator + Tablet Servers
--    Consumers                          SQL queries / SDK lookups
--    ZooKeeper or KRaft for metadata    ZooKeeper for metadata
--
-- The big differences that motivate scenarios 11-16:
--
--   1. Fluss has SCHEMAS. Columns are typed. The query engine can pushdown
--      projections (read only the columns you need) and filters.
--
--   2. Fluss has PRIMARY KEY TABLES with point-in-time lookups. Where Kafka
--      gives you a stream of records, a Fluss PK table is the **materialised
--      current state per key** — readable as either a stream of changes or a
--      regular table you can SELECT WHERE pk = '...' against.
--
--   3. Fluss tables can be TIERED to a lakehouse (Apache Paimon today). Hot
--      data stays in Fluss; cold data lives in Parquet on S3. Same table,
--      both layers. Scenario 16 covers this.
--
--   4. Fluss is built FOR Flink. The Flink SQL connector handles lookup
--      joins, partial updates, merge engines, and (on Flink 2.1+) delta
--      joins — all without the user writing operator code.
--
-- This step does two things only:
--   (a) Register the Fluss catalog so all subsequent SQL can refer to
--       `fluss.workshop.<table>`.
--   (b) Verify the catalog is reachable by listing what's there.
--
-- If step (b) fails:
--   - Is the Fluss coordinator container running?   podman ps | grep fluss
--   - Are the tablet servers registered?            see Fluss UI / logs
--   - Did you reuse a stale connection? Drop catalog + recreate.

-- ── (a) Register the Fluss catalog ──────────────────────────────────────────
-- `type = 'fluss'` is the connector. `bootstrap.servers` points at one or
-- more coordinator endpoints (comma-separated). Inside the podman network
-- this is workshop-fluss-coordinator:9123; from your host it'd be
-- localhost:19123.
CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'workshop-fluss-coordinator:9123'
);

-- ── (b) Verify connectivity ────────────────────────────────────────────────
SHOW CATALOGS;
-- Expected output (alphabetical): default_catalog, fluss
-- The `default_catalog` is Flink's in-memory catalog. `fluss` is what we
-- just created — its definition lives in Flink session memory but the data
-- it references lives in the Fluss cluster.

USE CATALOG fluss;
SHOW DATABASES;
-- Fluss ships with `fluss` as a built-in default database. The init job
-- (scripts/init-fluss.sh) also creates `workshop` — if you don't see it,
-- run that script first.
