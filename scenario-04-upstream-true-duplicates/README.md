# Scenario 04 — Upstream True Duplicates: Transport Guarantees Don't Catch Business Duplicates

> A producer deliberately sends the **same business event twice** with the same
> `eventId`. Flink's exactly-once transport faithfully delivers both copies — it
> prevents *recovery* duplicates but has no notion of "business duplicates". App 2
> ships in two variants: **naive** (counts both copies, inflated results) and
> **with dedup** (a `KeyedProcessFunction` over `ValueState<Boolean>` with TTL
> drops the second copy).

---

## What this teaches

| Concept | Where you see it |
|---|---|
| Exactly-once is a transport property, not a content property | `DuplicateEventProducer` sends the same `eventId` twice |
| Stateful dedup needs a business key | `DeduplicationFunction` keys by `eventId` |
| TTL on dedup state bounds memory growth | `StateTtlConfig.newBuilder(Duration.ofHours(N))` |
| First-write-wins semantics with `OnCreateAndWrite` update mode | Same TTL config |
| The cost is per-key state for the TTL window | Each unique eventId holds a tombstone |

---

## Architecture

```
DuplicateEventProducer
  emits 100 unique events;
  every 10th event sent
  twice with same eventId
    │
    ▼
 topic.in
    │
    ▼
 App1Pipeline  ───tx───►  topic.mid  ───►  App2PipelineNaive       (inflated counts)
 (pass-through                                     OR
  EXACTLY_ONCE)                              App2PipelineWithDedup ──►  DeduplicationFunction ──► clean counts
                                                                        keyed by eventId,
                                                                        TTL state
```

Two App 2 variants ship as separate JARs so you can compare them side-by-side.

---

## Run it

```bash
./quickstart.sh 4

# By default quickstart deploys App2PipelineWithDedup (the fixed path).
# To see the bug, deploy the naive App 2 separately:
flink run scenario-04-upstream-true-duplicates/target/scenario-04-app2-naive-jar-with-dependencies.jar

bash scripts/verify-04.sh
```

The producer is a **bounded BATCH** job: it sends `TOTAL_EVENTS=100` unique events,
duplicating every `DUPLICATE_EVERY=10`th one with a `DUPLICATE_DELAY_MS=200`
sleep. So 100 unique eventIds → 110 actual Kafka messages.

App 2 (naive) sums counts to 110.
App 2 (with dedup) sums counts to 100.

---

## What you should see

### App2PipelineNaive (the bug)

- Reads `topic.mid` with `read_committed`.
- For each event, increments a `KeyedProcessFunction`-managed counter per accountId.
- Writes the counter to `topic.out`.
- Total over all accountIds: 110 because each of the 10 duplicate eventIds was counted twice.

### App2PipelineWithDedup (the fix)

- Reads `topic.mid`.
- Passes each event through `DeduplicationFunction` keyed by `eventId`.
- Inside: `ValueState<Boolean>` per eventId. First time you see it → mark true, emit.
  Second time → state is already true → drop.
- TTL on the state is 1 hour by default (`DEDUP_TTL_HOURS`). After 1 hour of inactivity,
  the tombstone is GC'd. A third copy arriving *after* the TTL would be treated as new
  — choose the TTL based on the longest inter-duplicate interval you want to tolerate.

---

## The "aha" moment

> *Exactly-once Kafka delivered the same eventId twice. Why?*

Because two **different** Kafka records can carry the same payload-level `eventId`.
Exactly-once at the transport layer means "each Kafka *record* is delivered once".
The producer of those Kafka records is responsible for not emitting two records that
mean the same business thing. If the producer is buggy, retrying, or unreliable (the
common case for at-least-once HTTP webhooks, sensor uplinks, etc.), the consumer is
the one that has to deduplicate.

The dedup is **content-aware**, not transport-aware. It looks at a key inside the
event (`eventId`), not at Kafka offsets.

---

## Code highlights

- [`producer/DuplicateEventProducer.java`](src/main/java/com/workshop/flink/scenario04/producer/DuplicateEventProducer.java) —
  bounded BATCH source that emits N events, duplicating every Nth. Standalone
  `SourceFunction` so the duplication is deterministic for the demo.
- [`App1Pipeline.java`](src/main/java/com/workshop/flink/scenario04/App1Pipeline.java) —
  pass-through with EXACTLY_ONCE Kafka. Proves the duplicates traverse the transport
  layer unchanged.
- [`App2PipelineNaive.java`](src/main/java/com/workshop/flink/scenario04/App2PipelineNaive.java) —
  the buggy counter for comparison.
- [`App2PipelineWithDedup.java`](src/main/java/com/workshop/flink/scenario04/App2PipelineWithDedup.java) —
  pipes the source through `DeduplicationFunction` *before* the count.
- [`operator/DeduplicationFunction.java`](src/main/java/com/workshop/flink/scenario04/operator/DeduplicationFunction.java) —
  the dedup core. `OnCreateAndWrite` update type means the TTL resets when a duplicate
  arrives, so back-to-back duplicates always get caught.

---

## Pitfalls / things to remember

- **TTL is the contract.** Pick it to be safely longer than the longest legitimate
  re-delivery interval. Too short → late duplicates leak through. Too long → state
  grows unbounded across distinct eventIds.
- **State size = unique events × per-key tombstone**. With 1M events/hour and a
  1-hour TTL, you're holding ~1M tombstones in state. Plan storage accordingly.
- **`OnCreateAndWrite` vs `OnReadAndWrite`.** `OnCreateAndWrite` resets the TTL on
  write — the right choice here. `OnReadAndWrite` would reset on read too, which
  means every check extends the lifetime indefinitely as long as duplicates keep
  arriving. Usually wrong.
- **Don't conflate dedup with idempotency.** Dedup happens at one point in the
  topology; downstream sinks still need to handle their own replay (see scenario 03).

---

## See also

- **Scenario 02** — exactly-once Kafka transport; the prerequisite for this scenario
  to make sense (otherwise the recovery duplicates from scenario 01 dominate).
- **Scenario 09** — uses semantic state with TTL in a multi-way join context.
- [`../sql-notes/scenario-sql-equivalents.md`](../sql-notes/scenario-sql-equivalents.md#scenario-04--semantic-dedup) —
  the `ROW_NUMBER() OVER (PARTITION BY eventId ORDER BY PROCTIME())` SQL pattern.
- Flink state TTL docs:
  <https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/state/#state-time-to-live-ttl>
