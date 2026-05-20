# Scenario 13 — Fluss Partial Updates & Merge Engines

> The headline Fluss feature for "customer 360 / instrument master from N
> sources." Each source writes only the columns it owns; Fluss merges
> them. Plus a working Java DataStream job (`WideProfileUpdaterJob`) that
> demonstrates the production-shaped pattern.

---

## What you'll learn

| Concept | Where you see it |
|---|---|
| Partial-update semantics — write some columns, leave the rest alone | `sql/02`, `sql/03` |
| Default merge engine — last-write-wins per column | `sql/02`, `sql/03` |
| Aggregation merge engine — sum / max / min per column | `sql/04` |
| First-row merge engine — keep the first-observed write | `sql/05` |
| Versioned merge engine — out-of-order updates can't go backwards | `sql/05` |
| Production-shaped pattern: one Flink job fans many sources into one table | `WideProfileUpdaterJob.java` |

---

## Prerequisites

```bash
bash scripts/init-fluss.sh           # creates customer_360, daily_volumes_agg, first_observed, versioned_config
bash scripts/seed-fluss.sh           # streams the three partial-update sources via FlussDatagenJob
```

---

## How to run it

### SQL teaching path

Paste each `sql/0N-*.sql` into the SQL Client in order:

```bash
bash scripts/sql-client.sh
```

### Java DataStream job (`WideProfileUpdaterJob`)

Build the shaded JAR:

```bash
mvn -pl scenario-13-fluss-partial-updates -am package -DskipTests
```

Submit to the workshop Flink:

```bash
JAR=scenario-13-fluss-partial-updates/target/scenario-13-wide-profile-updater-jar-with-dependencies.jar
podman cp "$JAR" workshop-jobmanager:/tmp/scenario-13.jar
podman exec workshop-jobmanager flink run -d \
    -c com.workshop.flink.scenario13.WideProfileUpdaterJob /tmp/scenario-13.jar
```

The job reads a Kafka topic (`customer-events`, configurable) of mixed
KYC/AML/Marketing events and partial-updates the Fluss `customer_360`
table. See `WideProfileUpdaterJob` JavaDoc for the envelope shape.

---

## The 6 steps

1. **`sql/01`** — Inspect the `customer_360` table.
2. **`sql/02`** — KYC partial update; only KYC columns are written.
3. **`sql/03`** — AML and Marketing each contribute their slices; the row
   fills up without any source overwriting the others.
4. **`sql/04`** — Aggregation merge engine; multiple inserts accumulate
   into per-column sums/maxes/mins.
5. **`sql/05`** — `first_row` (idempotency) and `versioned` (anti-stale)
   merge engines.
6. **`WideProfileUpdaterJob.java`** — production-shaped Flink DataStream
   job that fans a single Kafka stream of mixed events into three partial
   Fluss writes via Table API + side outputs.

For the deep walkthrough see [`WORKSHOP.md`](./WORKSHOP.md).

---

## Verification

```bash
bash scripts/verify-13.sh
```

Checks that the seeded `customer_360` rows have populated columns from
all three sources (KYC + AML + Marketing).

---

## See also

- **Scenario 11** — Fluss fundamentals; if "primary-key table" or "merge
  engine" are new terms, start there.
- **Scenario 14** — lookup joins from streaming queries into Fluss PK
  tables. Builds on what `customer_360` lets you do.
- **Scenario 04** — semantic dedup with TTL state. The Kafka-only version
  of what `first_row` merge engine gives you for free.
- [Apache Fluss — Understanding Partial Updates](https://fluss.apache.org/blog/partial-updates/)
- [FIP-21: Aggregation Merge Engine](https://cwiki.apache.org/confluence/display/FLUSS/FIP-21:+Aggregation+Merge+Engine)
