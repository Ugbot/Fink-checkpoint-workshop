# Scenario 16 Workshop — Tiered Storage & Cross-Engine Analytics

> **Audience:** completed scenarios 06 (Paimon basics) and 11–15 (Fluss
> track up to Java clients).
> **Time:** ~60 minutes.
> **Format:** SQL Client paste-along for steps 1–4, then `spark-submit`
> for steps 5–6.

---

## What you'll be able to do

1. Configure Fluss tiering at the cluster + table level.
2. Recognise the system columns Fluss adds to the Paimon mirror.
3. Read the same Fluss table in three ways: unified (Flink), Paimon-direct
   (any Paimon-aware engine), live (Spark via Fluss-Spark connector).
4. Choose between hot-Fluss, tiered-Paimon, and Spark-Paimon for a given
   query.

---

## Mental model

```
                     writes                                  reads
                       │                                       │
                       ▼                                       │
                  ┌─────────┐  background tiering   ┌──────────┴────────┐
   datagen ─────► │  FLUSS  │ ────► s3a://paimon ────►  PAIMON MIRROR   │
                  │  hot    │     /warehouse/...   │  cold (Parquet)    │
                  │ buckets │                      │  + __bucket/       │
                  │         │                      │    __offset/       │
                  │         │                      │    __timestamp     │
                  └────┬────┘                      └────────┬──────────┘
                       │                                    │
                       ▼                                    ▼
            Flink SQL: SELECT * FROM           Flink/Spark/Trino:
            fluss.workshop.trade_log_wide      SELECT * FROM
            (hot + tiered, unified)            paimon.workshop.trade_log_wide
                                               (cold only, but cheap)
```

The Fluss table and the Paimon mirror are **the same logical data**,
queryable through either path. The path you pick is a latency/cost choice.

---

## Step 01 — Enable tiering

### Concept
Two-level configuration:
1. **Cluster level**: `datalake.format`, `datalake.paimon.*` already set
   in the workshop's `podman-compose.yml`.
2. **Table level**: `'table.datalake.enabled' = 'true'` plus
   `'table.datalake.freshness'` (the max age data can have in Fluss-only
   before being tiered).

We drop and recreate `trade_log_wide` with tiering on. The init job's
default had tiering disabled to keep scenarios 12/14 lighter.

### Story
The lifecycle team wants the last 7 days of trades available for hot
queries (point lookups, current-day reports). Years of history must be
retained for regulators. Solving this with separate hot/cold pipelines
means keeping two copies in sync. Fluss tiering eliminates that — same
table, two layers managed automatically.

### Aha
> What controls how often tiering fires?

`'table.datalake.freshness'`. Setting it to `'1 min'` means at most 1
minute of trades are Fluss-only at any time. Smaller value = fresher
Paimon but more frequent commits (more S3 traffic, more snapshots).
Default in many Fluss deployments is `5 min` or higher.

### Pitfall
Changing `'table.datalake.freshness'` on an existing table is an `ALTER
TABLE` operation. Don't expect it to migrate existing data — only future
tiering windows respect the new value.

---

## Step 02 — Watch tiering happen

### Concept
Fluss creates a **mirror** Paimon table at
`s3://paimon/warehouse/workshop.db/trade_log_wide/` automatically. The
mirror has the user-defined schema PLUS three system columns:
- `__bucket INT` — Fluss bucket the row came from
- `__offset BIGINT` — offset within the bucket
- `__timestamp TIMESTAMP_LTZ(3)` — Fluss-side commit time

### Observe
- `SHOW TABLES` under the Paimon catalog now lists `trade_log_wide`.
- `DESCRIBE trade_log_wide` shows the 28 columns (25 user + 3 system).
- `SELECT __bucket, COUNT(*)` returns the per-bucket distribution.

### Aha
> Why are `__bucket` / `__offset` exposed?

They make the tiered side **rewindable**. A downstream Spark job that
resumes after a failure can use `__offset` to pick up exactly where it
left off. Without these columns Paimon would just have an unordered
collection of rows.

### Pitfall
Don't write to the Paimon mirror table directly. Fluss owns the mirror —
external writes will conflict with the tiering service and may corrupt
metadata.

---

## Step 03 — Unified Fluss read

### Concept
A `SELECT * FROM fluss.workshop.trade_log_wide` from Flink SQL pulls
data from BOTH the hot Fluss segments AND the Paimon mirror, unioned
behind the scenes by the Fluss connector.

### Story
The on-call dashboard wants "trades in the last 4 hours". Some are still
hot (in Fluss); some are tiered (in Paimon). The dashboard query doesn't
care which — it just wants all of them.

### Observe
- Count on the Fluss side ≥ count on the Paimon side. The difference is
  hot data not yet tiered.
- EXPLAIN shows the table source labelled with both layers' coordinates.

### Aha
> Is the union deduplicated?

For log tables, no — each row exists in exactly one layer at any given
time (Fluss segments are pruned once they've been tiered). For PK tables
with tiering, the connector merges by PK so you see one row per key.

---

## Step 04 — Direct Paimon read

### Concept
The Paimon mirror is just plain Paimon. Any engine that can read a Paimon
catalog can query it without going through Fluss.

### Story
The analyst team has been using Trino on Paimon for a year. They don't
need to learn Fluss SQL — they query the mirror table the same way they'd
query any Paimon table. The streaming team owns the Fluss cluster; the
analyst team owns their Trino. Decoupled.

### Observe
The count on the Paimon side is slightly less than on the Fluss side
(the missing rows are hot-but-not-yet-tiered). The difference shrinks if
you wait until the next tiering tick fires.

### Aha
> Could I query the Paimon mirror with Trino?

Yes. Or DuckDB-Paimon. Or Hive on Paimon. Or any of the engines that
have a Paimon reader. This is the value of standardised lakehouse table
formats.

---

## Step 05 — `SparkPaimonBatchJob`

### Concept
Spark Java application that reads the Paimon mirror directly. Aggregates
volumes by sector + country. Writes a summary table back into Paimon.

### Story
The "monthly volumes by sector" report runs at 02:00 every morning. It
scans millions of trades. Spark + Paimon's Parquet columnar storage +
projection pushdown make this a 30-second job. Doing the same against
Fluss-only would cost more per query (Fluss is optimised for low-latency
random reads, not bulk scans).

### Run
```bash
mvn -pl scenario-16-fluss-paimon-tiered-spark -am package -DskipTests
bash scripts/spark-submit-scenario-16.sh batch
```

### Observe
- Spark prints the catalog list (paimon, default).
- The aggregation runs cleanly and produces sector + country breakdowns.
- A new Paimon table `paimon.workshop.spark_sector_summary` is created
  (you can query it from Flink afterwards — same lakehouse, multiple
  writers).

### Aha
> Why is Spark cheap here?

Three things compound: (1) Parquet columnar storage means only the few
columns the query references are read from S3, (2) Spark's catalyst
optimiser uses Paimon's manifest files to prune data files entirely,
(3) the workload is read-mostly so the AWS-S3 connector amortises
connection costs across the cluster.

---

## Step 06 — `SparkFlussLiveJob`

### Concept
Same shape as step 05, but Spark uses the **Fluss-Spark connector** to
read the live Fluss table directly (hot + cold).

### Story
Some analyses cannot tolerate even a 1-minute lag. Risk team wants
"position-by-account as of NOW" for end-of-trading-hour cutoffs. Spark
on the live Fluss table gives them current snapshots; Spark on the
Paimon mirror would lag.

### Run
```bash
bash scripts/spark-submit-scenario-16.sh live
```

### Aha
> When is this slower than step 05?

When the workload is "scan everything from the past year." The Fluss
connector serves Spark via the Fluss tablet servers' read path, which is
tuned for keyed lookups, not for petabyte scans. For huge bulk scans
read the Paimon mirror.

---

## Decision matrix

| Need | Surface |
|---|---|
| Point lookup by key, sub-ms | Fluss SDK or SQL `WHERE pk = ?` |
| Streaming enrichment | Fluss SQL lookup join (scenario 14) |
| Aggregation, last few hours, fresh | Fluss batch read (step 03) |
| Heavy historical batch | Spark on Paimon (step 05) |
| Live snapshot from Spark | Spark on Fluss (step 06) |
| Cross-engine (Spark + Trino + DuckDB) | Paimon mirror, ignore Fluss |

---

## Production checklist

- [ ] `'table.datalake.freshness'` tuned to your latency tolerance.
      Too aggressive = lots of S3 small files; too lax = stale Paimon.
- [ ] Paimon snapshot retention policy set (`snapshot.time-retained`).
- [ ] MinIO / S3 connection credentials secured (workshop uses
      plaintext for simplicity).
- [ ] Compaction job scheduled on the Paimon side (Paimon `compact`).
- [ ] Decide which engine owns which use case — write it down.
