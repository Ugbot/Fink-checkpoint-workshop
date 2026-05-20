# Scenario 05 — Rescaling and Savepoint Replay: Maintenance Carries the Same Risk as Failure

> A stateful enrichment job (keyed by `accountId`) plus an event-time tumbling
> aggregator. Take a **savepoint** at a known offset, then restart at a different
> parallelism from that savepoint — Kafka replays records that downstream already
> consumed, so any *side-effecting* operator sees the same record twice. The
> takeaway: planned maintenance triggers replay exactly like an unplanned crash.

---

## What this teaches

| Concept | Where you see it |
|---|---|
| Savepoint = manual checkpoint with operator-state portability | `flink savepoint <jobId> file:///...` |
| Rescaling via savepoint preserves keyed state across new parallelism | `flink run -p N --fromSavepoint ...` |
| `pipeline.max-parallelism` controls key-group granularity | Set at job submission |
| Stateful operators stay correct under rescale; *external* side-effects don't | Same lesson as scenario 03 in operational clothing |
| Event-time windows survive rescaling unchanged | `App2Pipeline` watermark + 30 s tumble |

---

## Architecture

```
FinancialDatagenJob (common)
    │
    ▼
 topic.in   ──►  App1Pipeline                    ──►  topic.mid  ──►  App2Pipeline
                 (StatefulEnrichmentFunction:                          (event-time
                  per-accountId ValueState<long>                       30 s tumble
                  records the last seen tradeTime;                     per accountId,
                  emits TradeEvent with sourceApp =                    bounded
                  "stateful-enriched-pN")                              out-of-orderness
                 parallelism = APP1_PARALLELISM                        = 5 s)
```

The interesting thing about this topology isn't the topology itself — it's what
happens when you take a savepoint, change `-p`, and resume.

---

## Run it

```bash
./quickstart.sh 5

# Once it's running:
flink list                            # find the jobId for App1
flink savepoint <jobId> file:///tmp/flink-savepoints/s05

# Cancel App1 and restart at higher parallelism:
flink stop --savepointPath file:///tmp/flink-savepoints/s05-auto <jobId>
flink run -p 4 --fromSavepoint file:///tmp/flink-savepoints/s05 \
    scenario-05-rescaling-replay/target/scenario-05-app1-jar-with-dependencies.jar

bash scripts/verify-05.sh
```

---

## What you should see

1. App 1 runs at `APP1_PARALLELISM=2`. The `StatefulEnrichmentFunction` accumulates
   per-accountId state.
2. Take a savepoint. Flink writes operator-state files to disk.
3. Cancel App 1.
4. Restart App 1 at `-p 4` from the savepoint. Flink **redistributes key groups** —
   each new subtask owns roughly half the accounts the old subtasks owned. State
   is preserved correctly.
5. Kafka consumer offsets are also part of the savepoint. The new job resumes
   exactly where the old one left off. **No duplicates** on `topic.mid` for fixed-time
   identical inputs.

The interesting failure mode is when you restart from an **earlier** savepoint —
say, one taken before the cancel. Then Kafka replays records the new job already
saw downstream:

- App 2's windows are deterministic on event-time → unaffected.
- App 1's keyed state at that older savepoint is older → it'll re-emit records
  it had already emitted with the newer state. Downstream sees duplicates.

This is the same "replay produces duplicates" story as scenario 01, just with a
human operator pulling the trigger instead of a crash.

---

## The "aha" moment

> *Rescaling is supposed to be safe. Why is it dangerous?*

Rescaling itself is safe — Flink redistributes key groups correctly and resumes
from the savepoint's recorded offsets. The danger is what you do *around* the
rescale:

- If you cancel without taking a fresh savepoint, you lose anything written since
  the last checkpoint.
- If you restart from an older savepoint than the one you took at cancel, you replay.
- If your downstream sink is not idempotent (scenario 03), replay = duplicate side
  effects.

The dedup story from scenario 04 applies to **any** replay — crashes, rescales, or
hand-edited savepoint restores. Rescaling is not a safe shortcut.

---

## Code highlights

- [`App1Pipeline.java`](src/main/java/com/workshop/flink/scenario05/App1Pipeline.java) —
  reads `topic.in`, applies `StatefulEnrichmentFunction`, writes `topic.mid`.
- [`transform/StatefulEnrichmentFunction.java`](src/main/java/com/workshop/flink/scenario05/transform/StatefulEnrichmentFunction.java) —
  the per-accountId `ValueState<Long>` operator. Demonstrates state survives rescale.
- [`App2Pipeline.java`](src/main/java/com/workshop/flink/scenario05/App2Pipeline.java) —
  event-time tumbling count. Watermarks declared via
  `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))` — see scenario
  10's WORKSHOP.md for the deep dive on this strategy.

---

## Pitfalls / things to remember

- **`pipeline.max-parallelism` is sticky.** It's baked into the savepoint. Changing it
  later requires a state-migration tool (or a fresh state). Pick a power of 2 (128,
  256) at the *first* job submission so future rescaling fits exact divisors.
- **Take a savepoint **before** every cancel.** `flink stop --savepointPath ...` does
  this in one step.
- **Savepoints retain checkpoints.** Make sure your storage class doesn't auto-delete
  them. The workshop uses named Podman volumes for this reason.
- **Don't restart from an older savepoint as a "quick rollback".** You'll replay
  everything between that savepoint and current Kafka offsets — including any
  external side-effects. Scenario 03's idempotency story applies in full.

---

## See also

- **Scenario 02** — exactly-once Kafka. Required so replay does not produce duplicates
  on the Kafka layer.
- **Scenario 03** — external-sink idempotency. The same lesson with a different trigger.
- **Scenario 10** — WORKSHOP.md step 04 covers why event-time aggregators (App 2 here)
  are deterministic under replay.
- [`../sql-notes/scenario-sql-equivalents.md`](../sql-notes/scenario-sql-equivalents.md#scenario-05--rescaling) —
  rescaling SQL jobs follows the same workflow.
- Flink savepoints docs:
  <https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/savepoints/>
