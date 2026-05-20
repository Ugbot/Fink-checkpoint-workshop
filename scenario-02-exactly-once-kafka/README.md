# Scenario 02 — EXACTLY_ONCE Kafka: No Duplicates at the Kafka Boundary

> Same crash, same replay — but with Flink's **transactional Kafka sink** plus a
> `read_committed` consumer downstream. Internal replay still happens, but the
> only records visible on `topic.mid` are ones that were part of a committed
> transaction. The duplicate counts from scenario 01 disappear.

---

## What this teaches

| Concept | Where you see it |
|---|---|
| Two-phase commit between Flink checkpoints and Kafka transactions | `KafkaSinkFactory.exactlyOnce(...)` |
| Transactional ID prefix scope | `setTransactionalIdPrefix(...)` per job |
| `isolation.level = read_committed` on consumers | `KafkaSourceFactory.readCommitted(...)` |
| Aborted transactions are invisible to read_committed | App 2 never sees the replayed records |
| Exactly-once *at the Kafka boundary* is not exactly-once everywhere | Scenario 03 explores the gap |

---

## Architecture

```
FinancialDatagenJob (common)
    │
    ▼
 topic.in  ───►  App1Pipeline                ───tx─► topic.mid  ───►  App2Pipeline       ───►  topic.out
                 (EXACTLY_ONCE Kafka sink,                            (consumer with
                  2PC tied to checkpoints,                             isolation.level=
                  crashes after                                        read_committed)
                  CRASH_AFTER_RECORDS records)
```

Between the last completed checkpoint and the crash, App 1 has an *open* Kafka transaction.
On crash:

- The pending transaction is **aborted** (the broker times it out or the new Flink
  attempt explicitly cancels it).
- read_committed consumers — including App 2 — never see those records.
- Flink restarts App 1 from the last checkpoint and starts a fresh transaction.
- The replayed records land in a new transaction that eventually commits cleanly.

Net effect: every `eventId` shows up on `topic.mid` exactly once.

---

## Run it

```bash
./quickstart.sh 2

bash scripts/verify-02.sh   # asserts no duplicates in topic.mid under read_committed
```

`quickstart.sh` injects the same `CRASH_AFTER_RECORDS=50` as scenario 01. Compare the
verify output: scenario 01 reports inflated counts; scenario 02 reports clean ones.

---

## What you should see

1. App 1 starts. Every checkpoint barrier closes the current Kafka transaction (commit)
   and starts a new one.
2. App 1 crashes mid-transaction. The records written since the last checkpoint are in
   an **uncommitted** state on the broker.
3. App 2 has `isolation.level=read_committed` so it advances past the records of the
   aborted transaction — it cannot even see them.
4. App 1 restarts from the last checkpoint. It re-reads from `topic.in` at the
   pre-crash offset, re-writes the same records into a **new** transaction, and commits
   it cleanly at the next checkpoint.
5. App 2 sees each `eventId` exactly once.

---

## The "aha" moment

> *If App 1 wrote the same record into Kafka twice (once in the aborted transaction,
> once in the new one), why does App 2 only see it once?*

Because the aborted transaction's records exist on the Kafka log but are **filtered
out** by `read_committed`. They're never deleted — they're just invisible to
transactional consumers. They still take up disk space until log retention catches up.

The Kafka transaction protocol uses a control message (`AbortTransaction`) appended
to the log; consumers scan the log and skip records that belong to aborted transactions.

This is what makes exactly-once at the Kafka boundary possible without rolling back
already-written data.

---

## Code highlights

- [`App1Pipeline.java`](src/main/java/com/workshop/flink/scenario02/App1Pipeline.java) —
  identical shape to scenario 01's App 1, but uses `KafkaSinkFactory.exactlyOnce(...)`
  with `setTransactionalIdPrefix("s02-app1")`.
- [`App2Pipeline.java`](src/main/java/com/workshop/flink/scenario02/App2Pipeline.java) —
  identical shape to scenario 01's App 2, but uses `KafkaSourceFactory.readCommitted(...)`.
- [`../common/.../util/KafkaSinkFactory.java`](../common/src/main/java/com/workshop/flink/common/util/KafkaSinkFactory.java) —
  the only file that differs between scenarios 01 and 02 in any meaningful way.

---

## Pitfalls / things to remember

- **Transactional ID prefix must be stable across restarts.** Flink derives the full
  transactional ID as `prefix-<subtask>-<checkpoint>` so the broker can recognise the
  same logical producer across attempts. Changing the prefix orphans the in-flight
  transactions and risks duplicates.
- **`transaction.timeout.ms` must exceed your maximum checkpoint interval.** If the
  broker times the transaction out before Flink commits, the records are lost. The
  workshop's broker config sets a 15-minute timeout — plenty for a 10-second checkpoint.
- **read_committed on the consumer is mandatory.** A vanilla consumer reading `topic.mid`
  *will* see the aborted records. Many production "exactly-once is broken!" tickets
  come from downstream services that forgot this property.
- **Exactly-once at Kafka does not extend to your database.** That's scenario 03.

---

## See also

- **Scenario 01** — the baseline this scenario fixes.
- **Scenario 03** — exactly-once Kafka but a non-idempotent JDBC sink: external systems
  are outside Flink's two-phase commit and need separate idempotency.
- [`../sql-notes/scenario-sql-equivalents.md`](../sql-notes/scenario-sql-equivalents.md#scenario-02--exactly_once-kafka) —
  Flink SQL DDL with `sink.delivery-guarantee = 'exactly-once'`.
- Flink Kafka connector docs:
  <https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/#fault-tolerance>
