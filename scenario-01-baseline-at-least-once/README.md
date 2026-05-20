# Scenario 01 — Baseline AT_LEAST_ONCE: Replay Duplicates Are Expected

> A two-job pipeline running with `AT_LEAST_ONCE` semantics and no transactional Kafka sink.
> App 1 deliberately crashes after N records; the records written to `topic.mid` between the
> last checkpoint and the crash are replayed on restart, producing **duplicate eventIds**
> downstream. App 2 counts them with a tumbling window so the inflated counts are visible
> in `topic.out`. This is the baseline against which every later scenario is measured.

---

## What this teaches

| Concept | Where you see it |
|---|---|
| Recovery always replays from the last checkpoint | Crash inside App 1's pass-through; offsets resume from the last committed |
| AT_LEAST_ONCE Kafka sink does NOT prevent duplicates | `KafkaSinkFactory.atLeastOnce(...)` writes without transactions |
| The default consumer behaviour reads everything | App 2 has no `isolation.level=read_committed` — it sees every record App 1 wrote |
| Aggregations amplify duplicates | A 5-second tumbling count on `topic.mid` shows the inflated count |
| Failure ≠ malfunction | The duplicate is **the design** of at-least-once — not a bug |

---

## Architecture

```
FinancialDatagenJob (common)
    │
    ▼
 topic.in  ────►  App1Pipeline   ────► topic.mid  ────►  App2Pipeline  ───► topic.out
                  (AT_LEAST_ONCE          ▲                (5s tumbling
                   pass-through,          │                 count per
                   crashes after          │                 accountId)
                   CRASH_AFTER_RECORDS    │
                   records)               │
                                          │
                              records written here AFTER the last checkpoint
                              are re-emitted on App 1 restart  →  duplicates
```

---

## Run it

```bash
./quickstart.sh 1          # starts infra + datagen + App1 + App2

# In another terminal, watch the counts inflate on each replay:
bash scripts/verify-01.sh
```

`quickstart.sh` sets `CRASH_AFTER_RECORDS=50` on App 1 by default — enough to cross a
checkpoint boundary and replay something visible. Tune via the env var.

---

## What you should see

1. **Before the crash**: App 1 reads `topic.in`, passes through to `topic.mid`. App 2
   produces normal per-account counts.
2. **At the crash**: `CrashTrigger` throws inside App 1's `.map(...)`. The job restarts
   from its last completed checkpoint, which is *behind* the last record actually written
   to `topic.mid`.
3. **After the crash**: App 1 replays the records between the last checkpoint and the
   crash point. Those records hit `topic.mid` **a second time**, with the same `eventId`s.
4. **Downstream**: App 2's tumbling window sums them all — accountId counts now exceed
   what the datagen actually produced.

The verify script extracts a sample of `eventId`s from `topic.mid` (read with default
consumer isolation, i.e. `read_uncommitted`) and reports how many appear more than once.

---

## The "aha" moment

> *Why doesn't the second checkpoint just overwrite the first one's records?*

Kafka is an append-only log. Once a record is in `topic.mid`, it is in `topic.mid`.
There is no rewind. AT_LEAST_ONCE just guarantees "I will deliver at least once" — it
explicitly does **not** say "at most once".

Two ways to fix this:

- Move the **producer side** to transactional/exactly-once writes → **Scenario 02**.
- Move the **consumer side** to be idempotent by business key → **Scenario 04**.

Real production pipelines usually do both.

---

## Code highlights

- [`App1Pipeline.java`](src/main/java/com/workshop/flink/scenario01/App1Pipeline.java) —
  reads `topic.in`, optionally crashes via `CrashTrigger`, writes `topic.mid` with
  `KafkaSinkFactory.atLeastOnce(...)`.
- [`App2Pipeline.java`](src/main/java/com/workshop/flink/scenario01/App2Pipeline.java) —
  reads `topic.mid` with default isolation, tumbles 5 s per accountId, writes counts.
- [`../common/.../util/CrashTrigger.java`](../common/src/main/java/com/workshop/flink/common/util/CrashTrigger.java) —
  the transient counter that drives the crash. `count` is **transient on purpose** so
  it resets after restart (otherwise the job would crash forever).
- [`../common/.../util/KafkaSinkFactory.java`](../common/src/main/java/com/workshop/flink/common/util/KafkaSinkFactory.java) —
  contrast `.atLeastOnce(...)` vs `.exactlyOnce(...)` (scenario 02 uses the other).

---

## Pitfalls / things to remember

- **Checkpoint interval matters.** Shorter checkpoints → fewer records replayed per crash
  → fewer duplicates. Doesn't eliminate them.
- **App 2's window can hide a fix.** If you switch App 1 to exactly-once but App 2 still
  uses `read_uncommitted`, you'll still see duplicates because read_uncommitted does not
  filter out aborted transactions. See scenario 02.
- **`CrashTrigger.count` is `transient` by design.** Don't make it persistent or you'll
  fight infinite restart loops. The crash is a one-shot demo, not realistic chaos
  engineering.

---

## See also

- **Scenario 02** — exactly-once Kafka transactional sink + `read_committed` consumer.
  No duplicates at the Kafka layer.
- **Scenario 04** — exactly-once at the *transport* layer but the producer is sending
  duplicate business events. Semantic dedup with TTL state.
- [`../sql-notes/scenario-sql-equivalents.md`](../sql-notes/scenario-sql-equivalents.md#scenario-01--baseline-at_least_once) —
  the SQL version of this scenario.
- [`../scripts/verify-01.sh`](../scripts/verify-01.sh) — the verifier this README references.
