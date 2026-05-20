# Scenario 11 — Apache Fluss Fundamentals

> First scenario in the Fluss track. Assumes **zero prior knowledge of Fluss**.
> By the end you'll know what a log table is, what a primary-key table is,
> how to write the catalog DDL, and why people pick Fluss over Kafka for
> certain workloads.

This is the foundation for scenarios 12–16. Don't skip it.

---

## What you'll learn

| Concept | Where you see it |
|---|---|
| What Apache Fluss is and how it relates to Kafka | `sql/01-what-is-fluss.sql` |
| Catalog / database / table namespace | `sql/02-database-and-listing.sql` |
| Log tables (append-only, like Kafka topics) | `sql/03-log-table.sql` |
| Primary-key tables (upsert + point lookup) | `sql/04-pk-table.sql` |
| Streaming vs batch reads against the same Fluss table | `sql/03`, `sql/05` |
| Kafka compacted topic pain points vs Fluss PK tables | `sql/06-kafka-compacted-vs-fluss.sql` |
| DDL lifecycle | `sql/07-cleanup.sql` |

---

## Prerequisites

The Fluss cluster must be running and seeded:

```bash
podman-compose up -d --build         # brings up Fluss coordinator + tablets
bash scripts/init-fluss.sh           # creates the workshop database + tables
bash scripts/seed-fluss.sh           # seeds them with realistic financial data
```

If you skip the seed step, the read queries in steps 03, 04 and 05 will
return empty results.

---

## How to run it

The SQL files in [`sql/`](./sql/) are written for the **SQL Client / SQL Gateway**
(scenario 06's gateway works fine — same Flink session cluster).

```bash
bash scripts/sql-client.sh
# Then paste any of the .sql files in order:
#   01 → 02 → 03 → 04 → 05 → 06 → (07 only when you want to clean up)
```

For VVC deployment of the one deployable step, see
[`sql-deployments/`](./sql-deployments/) and the matching harness pipeline
`workshop-scenario-11-05-batch-aggregation-deploy.yaml`.

---

## The 7 steps

1. **What is Fluss** — register the catalog. The catalog DDL is identical
   for every subsequent step.
2. **Database and listing** — `SHOW DATABASES`, `SHOW TABLES`, `DESCRIBE`.
3. **Log table** — append-only `trade_log_wide`, batch + streaming reads.
4. **PK table** — upsert, point-lookup, changelog read of `account_profile`.
5. **Batch aggregation** — group-by over a wide log table; show the EXPLAIN
   plan and the projection list.
6. **Kafka compacted vs Fluss** — the centerpiece "why Fluss?" comparison.
7. **Cleanup** — DROP TABLE / DROP DATABASE for symmetry.

For the deep-dive walkthrough with the "Concept / Story / Observe / Aha"
sections per step, see [`WORKSHOP.md`](./WORKSHOP.md).

---

## Verification

```bash
bash scripts/verify-11.sh
```

The script checks:
- `SHOW CATALOGS` returns `fluss`.
- `SELECT COUNT(*) FROM account_profile` returns > 0 (seed ran).
- A point lookup on `account_profile WHERE account_id = 'ACC-0001'` returns 1 row.

---

## See also

- [`../scenario-12-fluss-wide-schemas/`](../scenario-12-fluss-wide-schemas/) —
  wide schemas + projection pushdown; the next step in the Fluss track.
- [`../scenario-10-event-time-sql/`](../scenario-10-event-time-sql/) — if you
  haven't done Flink SQL before, this is a softer SQL-only onramp.
- Apache Fluss site: <https://fluss.apache.org>
