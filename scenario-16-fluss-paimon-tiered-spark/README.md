# Scenario 16 — Fluss Tiered Storage to Paimon + Spark Queries

> The streaming-lakehouse capstone. Hot data in Fluss, cold data in Paimon
> on MinIO, queryable by Flink and **Apache Spark**. Demonstrates the
> "single logical table, two storage tiers, multiple compute engines"
> architecture.

---

## What you'll learn

| Concept | Where you see it |
|---|---|
| Enable tiering on a Fluss table: `'table.datalake.enabled' = 'true'` | `sql/01-enable-tiering.sql` |
| The auto-created Paimon mirror + system columns | `sql/02-watch-tiering.sql` |
| Read Fluss in unified (hot+cold) mode | `sql/03-query-fluss-unified.sql` |
| Query the Paimon mirror directly with the existing scenario-06 catalog | `sql/04-query-paimon-directly.sql` |
| Spark reading Paimon for cheap large scans | `SparkPaimonBatchJob.java` |
| Spark reading live Fluss for current-snapshot analytics | `SparkFlussLiveJob.java` |
| When to use which surface | `sql/07-compare-and-discuss.md` |

---

## Prerequisites

- Workshop infra up: `podman-compose up -d --build` (includes Fluss
  cluster + Spark service + MinIO already used by Paimon).
- Tables created: `bash scripts/init-fluss.sh`
- Data flowing: `bash scripts/seed-fluss.sh`

After step 01, leave the workshop running for ~2 minutes before step 02
so the Fluss tiering service has a chance to fire.

---

## The 7 steps

| Step | Surface | What it does |
|---|---|---|
| `sql/01-enable-tiering.sql`            | Flink SQL  | Recreate `trade_log_wide` with `'table.datalake.enabled' = 'true'` |
| `sql/02-watch-tiering.sql`             | Flink SQL  | Query the auto-created Paimon mirror; see system cols + per-bucket data |
| `sql/03-query-fluss-unified.sql`       | Flink SQL  | Aggregate the Fluss table — returns hot + cold unified |
| `sql/04-query-paimon-directly.sql`     | Flink SQL  | Same aggregate via the existing scenario-06 Paimon catalog |
| `src/.../SparkPaimonBatchJob.java`      | Spark      | Batch analytics on the Paimon mirror — sink summary back to Paimon |
| `src/.../SparkFlussLiveJob.java`        | Spark      | Live (hot+cold) read via Fluss-Spark connector |
| `sql/07-compare-and-discuss.md`        | Doc        | When to use which surface |

---

## Run it

### SQL teaching path (steps 01–04)

```bash
bash scripts/sql-client.sh
# paste sql/01..04 in order
```

### Spark apps (steps 05–06)

```bash
mvn -pl scenario-16-fluss-paimon-tiered-spark -am package -DskipTests
bash scripts/spark-submit-scenario-16.sh both
# or 'batch' or 'live' to run one at a time
```

The script copies the JARs into `workshop-spark` and submits via
`spark-submit`. Output streams to stdout. The Spark UI is at
<http://localhost:14040> while a job is running.

---

## Verification

```bash
bash scripts/verify-16.sh
```

Checks:
- `paimon.workshop.trade_log_wide` exists (auto-created by tiering).
- Row counts on the Paimon side and Fluss side are consistent
  (Fluss ≥ Paimon by at most one tier-freshness window).
- Both Spark apps complete successfully and produce non-empty output.

---

## See also

- [`WORKSHOP.md`](./WORKSHOP.md) — deep walkthrough with the "what to use
  when" decision tree.
- [`sql/07-compare-and-discuss.md`](./sql/07-compare-and-discuss.md) — the
  decision matrix.
- **Scenario 06** — Paimon basics. The catalog scenario 16 piggybacks on.
- **Scenario 14** — Flink lookup joins into Fluss; what the live side
  looks like from a streaming pipeline.
- [Fluss lakehouse storage docs](https://fluss.apache.org/docs/maintenance/tiered-storage/lakehouse-storage/)
