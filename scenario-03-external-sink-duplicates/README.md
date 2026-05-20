# Scenario 03 — External Sink Duplicates: Exactly-Once Kafka ≠ Exactly-Once Everywhere

> App 1 is the same EXACTLY_ONCE Kafka producer from scenario 02 — no duplicates on
> `topic.mid`. App 2 reads `topic.mid` with `read_committed` and writes each event to
> **PostgreSQL**. The database is **outside** Flink's two-phase commit protocol, so a
> crash between a successful DB write and a successful checkpoint causes the write to
> be replayed on restart — and you get a duplicate side-effect.
>
> This scenario ships both the **buggy** plain-INSERT sink and the **fixed** upsert
> (`ON CONFLICT DO UPDATE`) sink, switchable at deploy time.

---

## What this teaches

| Concept | Where you see it |
|---|---|
| Exactly-once at the Kafka layer is not exactly-once at external systems | App 1 vs App 2 |
| Non-idempotent INSERT after a crash produces duplicate rows | `PostgresInsertSink` ("buggy" mode) |
| `INSERT ... ON CONFLICT DO UPDATE` (upsert) is idempotent under replay | `PostgresUpsertSink` ("fixed" mode) |
| External sinks need *idempotent design*, not just exactly-once Kafka | `SINK_MODE` env var toggles the two implementations |
| Flink SQL gets this right by default when the table has a PRIMARY KEY | See sql-notes |

---

## Architecture

```
                                 SINK_MODE=buggy           OR        SINK_MODE=fixed
                                       │                                    │
 topic.in ──► App1Pipeline ───tx──► topic.mid ──► App2Pipeline ──► PostgresInsertSink   PostgresUpsertSink
              (EXACTLY_ONCE                       (read_committed,        │                      │
               Kafka producer)                    crashes after           ▼                      ▼
                                                  CRASH_AFTER_RECORDS)   plain INSERT       ON CONFLICT DO UPDATE
                                                                                ▼                ▼
                                                                          duplicate rows    1 row per eventId
                                                                          after a replay    regardless of replay
```

The crash now lives in App 2 (between writing to Postgres and the next checkpoint).
App 2 has already written 50 rows to Postgres when it crashes. After Flink restarts
App 2 from the previous checkpoint, those 50 records are re-consumed from `topic.mid`
and re-written to Postgres.

- In **buggy** mode: `INSERT INTO processed_trades (...)` produces 50 duplicate rows.
- In **fixed** mode: `INSERT ... ON CONFLICT DO UPDATE` is a no-op for the existing rows.

---

## Run it

```bash
# quickstart picks SINK_MODE=fixed by default — the safe path.
./quickstart.sh 3

# To see the bug, deploy App 2 with SINK_MODE=buggy:
SINK_MODE=buggy ./quickstart.sh 3

bash scripts/verify-03.sh
```

Verify connects to Postgres and runs:

```sql
SELECT COUNT(*) FROM duplicate_trades;
```

The `duplicate_trades` view (defined in [`scripts/init-postgres.sql`](../scripts/init-postgres.sql))
returns any `event_id` that appears more than once. Buggy mode → > 0; fixed mode → 0.

---

## What you should see

### SINK_MODE=buggy

1. App 1 produces clean `topic.mid` (EXACTLY_ONCE).
2. App 2 reads from `topic.mid` with `read_committed`. No duplicates from the Kafka side.
3. App 2's `PostgresInsertSink.invoke(...)` executes a plain `INSERT INTO processed_trades`
   for each event.
4. After 50 inserts, `CrashTrigger` fires. The current Flink checkpoint has NOT yet
   committed — but those 50 rows are already in the Postgres table.
5. Flink restarts App 2. It re-reads the same 50 records from `topic.mid` and re-INSERTs
   them. Postgres now has 100 rows for those 50 eventIds.
6. `duplicate_trades` view returns 50 distinct eventIds.

### SINK_MODE=fixed

Same flow, but `PostgresUpsertSink` uses
`INSERT ... ON CONFLICT (event_id) DO UPDATE SET ...`. The second write hits the existing
row's primary-key constraint and updates rather than inserting a second row. Final row
count = 50 distinct eventIds.

---

## The "aha" moment

> *Why doesn't Flink's two-phase commit protect this Postgres write?*

The 2PC protocol only covers participants that Flink knows how to *abort*. Kafka has a
documented transaction protocol with `AbortTransaction` markers — Flink can roll it back.
A `RichSinkFunction` doing a plain JDBC `INSERT` has no such protocol; once `executeUpdate`
returns, the row is in the table forever.

You could write a JDBC sink that participates in 2PC (Flink ships one — see
`flink-connector-jdbc`'s `XaSinkFunction`). That requires a JDBC driver with XA support
and is operationally painful (XA transaction recovery, distributed deadlocks). The
**vastly more common** production answer is the one this scenario shows: design the
write to be idempotent on a business key, and replay becomes a no-op.

---

## Code highlights

- [`App2Pipeline.java`](src/main/java/com/workshop/flink/scenario03/App2Pipeline.java) —
  branches on `SINK_MODE` env var. Both sinks share the same input stream and crash trigger.
- [`sink/PostgresInsertSink.java`](src/main/java/com/workshop/flink/scenario03/sink/PostgresInsertSink.java) —
  the buggy variant. Bare `INSERT INTO`.
- [`sink/PostgresUpsertSink.java`](src/main/java/com/workshop/flink/scenario03/sink/PostgresUpsertSink.java) —
  the fixed variant. `ON CONFLICT DO UPDATE`. Idempotent.
- [`../scripts/init-postgres.sql`](../scripts/init-postgres.sql) — `processed_trades`
  table + `duplicate_trades` view used by the verify script.

---

## Pitfalls / things to remember

- **"Exactly-once" is a property of a *boundary*, not of a system.** Always ask: at
  what boundary? Kafka? Postgres? S3? A REST endpoint? Each has its own answer.
- **Idempotency requires a unique business key.** Here it's `event_id` (a UUID). If your
  events don't carry one, you need to manufacture one upstream — at the cost of
  scenario 04's semantic dedup overhead.
- **Upsert sinks are not free.** Each write becomes a primary-key lookup + conditional
  update. At 100k/s that may push a Postgres instance over. Consider batching or a
  CDC-aware sink instead at high throughput.
- **Flink SQL hides this footgun.** Declaring a `PRIMARY KEY NOT ENFORCED` on a JDBC
  sink table makes the connector auto-use upsert. The DataStream-API version (this
  scenario) makes the buggy path easy to write by accident.

---

## See also

- **Scenario 02** — exactly-once at the Kafka boundary; this scenario builds on it.
- **Scenario 04** — semantic dedup with TTL state, useful when the producer can emit
  the same business event twice.
- [`../sql-notes/scenario-sql-equivalents.md`](../sql-notes/scenario-sql-equivalents.md#scenario-03--external-sink-jdbc) —
  Flink SQL's `PRIMARY KEY NOT ENFORCED` shortcut.
- Flink's XA JDBC sink (the heavy alternative):
  <https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/jdbc/>
