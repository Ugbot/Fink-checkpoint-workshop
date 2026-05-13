# Scenario 06 — Paimon Lakehouse + SQL Gateway

> Stream Kafka trades into an Apache Paimon table on MinIO, then query that table from a **separate** Flink session cluster via REST, the interactive SQL Client, or a built-in web UI.

This is the only scenario in the workshop that demonstrates the **streaming-to-lakehouse** pattern and the separation between an *ingest cluster* (writers) and a *query cluster* (readers).

---

## What it shows

| Concept | Where you see it |
|---|---|
| Kafka → lakehouse streaming | `PaimonIngestJob.java` |
| Idempotent catalog/schema bootstrap | `PaimonCatalogBootstrap.java` |
| Paimon primary-key table on S3-compatible storage | `PaimonConfig.createTableSql()` |
| Snapshot visibility = checkpoint interval | EXACTLY_ONCE checkpointing in `PaimonIngestJob` |
| Compute isolation between writers and ad-hoc readers | Two Flink clusters in `podman-compose.yml` |
| Three ways to query the same Paimon table | `scripts/query-rest.sh`, `scripts/sql-client.sh`, web UI on `:3000` |

---

## Architecture

```
                  ┌────────────────────────────────────────────────┐
                  │             Existing infra (reused)            │
  Trades datagen  │  workshop-kafka  ← topic: trades.scenario06    │
  (FinancialDatagenJob from common)                                │
                  └──────────────────┬─────────────────────────────┘
                                     │
                                     ▼
   ┌─────────────────────────────────────────────────────────────┐
   │  INGEST CLUSTER (workshop-jobmanager / -taskmanager)        │
   │  Submits: scenario-06-ingest-jar-with-dependencies.jar      │
   │  Kafka source → StreamTableEnv → INSERT INTO paimon.trades  │
   └────────────────────────┬────────────────────────────────────┘
                            │  writes Parquet + manifests + snapshot files
                            ▼
                ┌───────────────────────────┐
                │  MinIO  bucket: paimon    │
                │  (s3a://paimon/warehouse) │
                └─────────────▲─────────────┘
                              │  reads
   ┌──────────────────────────┴──────────────────────────────────┐
   │  QUERY CLUSTER (workshop-query-jobmanager / -taskmanager)   │
   │  + workshop-sql-gateway  (REST on :18083)                   │
   └────────────┬───────────────────────┬────────────────────────┘
                │                       │                  │
       curl (REST)            sql-client.sh           Node web UI
       scripts/query-rest.sh                          (:3000)
```

The two clusters are deliberately independent: a 10-minute analyst scan on the query cluster cannot back-pressure or destabilise the streaming ingest.

---

## Ports

| Surface | URL |
|---|---|
| Ingest Flink UI | http://localhost:18081 |
| Query Flink UI | http://localhost:18181 |
| Flink SQL Gateway (REST) | http://localhost:18083 |
| Web Query UI | http://localhost:3000 |
| MinIO console | http://localhost:19001  (`workshop` / `workshopsecret`) |
| MinIO S3 API | http://localhost:19000 |

---

## Run it end-to-end

```bash
# 1. Bring up everything (Kafka, Postgres, MinIO, both Flink clusters, SQL Gateway, web UI)
podman-compose up -d --build

# 2. Create the scenario-06 Kafka topic
./scripts/create-topics.sh

# 3. Build the scenario-06 jars
mvn -pl common,scenario-06-paimon-lakehouse -am package

# 4. Bootstrap the Paimon catalog/table on MinIO (idempotent)
./scripts/init-paimon.sh

# 5. Start the Kafka -> Paimon ingest job
./scripts/submit-scenario-06.sh

# 6. Produce trades via the existing workshop datagen
#    (any of the existing scenario quickstarts will do — they all publish into Kafka)
./quickstart.sh 2     # or run FinancialDatagenJob directly

# 7. Query the data — pick any of these:
./scripts/query-rest.sh "SELECT COUNT(*) FROM paimon.workshop.trades"
./scripts/sql-client.sh                  # interactive SQL Client
open http://localhost:3000               # web UI with prebaked queries

# 8. End-to-end check: Kafka count ≈ Paimon count
./scripts/verify-06.sh
```

---

## The Paimon table

```sql
CREATE TABLE paimon.workshop.trades (
  event_id   STRING,
  account_id STRING,
  ticker     STRING,
  side       STRING,
  quantity   INT,
  price      DOUBLE,
  trade_time TIMESTAMP_LTZ(3),
  source_app STRING,
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'bucket'                 = '4',
  'changelog-producer'     = 'input',
  'snapshot.time-retained' = '1h'
);
```

- **Primary key on `event_id`** — upserts deduplicate by trade UUID (so the same event delivered twice from Kafka collapses to one row).
- **`bucket = 4`** matches the Kafka topic partition count for parallel writes.
- **`changelog-producer = 'input'`** records the upsert stream so downstream Flink jobs can do CDC-style streaming reads of this table.
- **`s3a://` + `s3.path.style.access = true`** is required for MinIO (not real AWS S3).

The full DDL — including the `CREATE CATALOG` block — lives in [`PaimonConfig.java`](src/main/java/com/workshop/flink/scenario06/PaimonConfig.java) and is the single source of truth used by the bootstrap main, the ingest main, and (manually) by SQL Client sessions.

---

## The three query surfaces

### 1. REST API (curl + jq)

[`scripts/query-rest.sh`](../scripts/query-rest.sh) walks the gateway's four-step lifecycle:

```text
POST   /v1/sessions                                  → sessionHandle
POST   /v1/sessions/{sid}/statements                 → operationHandle
GET    /v1/sessions/{sid}/operations/{op}/result/0   → paginated rows
DELETE /v1/sessions/{sid}                            → cleanup
```

This is the lowest-dependency way to query — no Flink client jar, no JDBC driver, just `curl`.

### 2. Interactive SQL Client

[`scripts/sql-client.sh`](../scripts/sql-client.sh) execs `sql-client.sh gateway` inside the gateway container, giving you a REPL connected to the query cluster. Good for `EXPLAIN`, `DESCRIBE`, `SHOW PARTITIONS`, and exploratory work.

### 3. Web UI

[`docker/query-ui/`](../docker/query-ui) — a ~150-line Node + Express service that wraps the same REST lifecycle as the curl script and renders results as an HTML table. The dropdown is populated from [`presets.json`](../docker/query-ui/presets.json), which includes a `paimon.workshop.trades$snapshots` query so attendees can see lakehouse metadata directly.

---

## How it works under the hood

### The Docker image

Both Flink clusters (and the SQL Gateway) run the **same** custom image built from [`docker/flink-paimon/Dockerfile`](../docker/flink-paimon/Dockerfile):

```
flink:1.20
+ /opt/flink/lib/paimon-flink-1.20-1.0.1.jar      ← Paimon connector
+ /opt/flink/lib/paimon-s3-1.0.1.jar              ← Paimon S3 filesystem
+ /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-1.20.4.jar
```

Versions are pinned via the parent POM's `paimon.version` and `flink.version` — bump both in [`pom.xml`](../pom.xml) when upgrading.

### Snapshot visibility latency

Paimon writes Parquet files continuously but only **commits a snapshot** at every Flink checkpoint barrier. The workshop's default is a 10-second checkpoint interval (see `Constants.CHECKPOINT_INTERVAL_MS`), so:

- A query issued via the gateway sees data up to the most recent committed snapshot.
- That snapshot lags Kafka by **at most one checkpoint interval** (≈ 10 seconds).
- `verify-06.sh` accounts for this by tolerating a small `kafka - paimon` delta.

This is the lakehouse trade-off: read latency in exchange for cheap, scalable columnar storage and time-travel.

### Why two Flink clusters?

A single cluster could host both the ingest job and ad-hoc queries — and at workshop scale that would work fine. The split exists to demonstrate the **production pattern**:

- Ingest jobs have strict SLAs and need predictable resources.
- Ad-hoc queries are unpredictable (a `SELECT *` from an analyst can scan terabytes).
- Sharing slot pools couples those two failure domains.

In the compose file, kill the query cluster (`podman stop workshop-query-jobmanager`) and the ingest job keeps running unaffected. That's the property we want.

---

## See also

- [`sql-notes/scenario-sql-equivalents.md`](../sql-notes/scenario-sql-equivalents.md) — pure-SQL version of the whole pipeline.
- Apache Paimon docs: https://paimon.apache.org/
- Flink SQL Gateway REST reference: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql-gateway/rest/
