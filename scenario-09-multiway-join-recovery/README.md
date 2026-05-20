# Scenario 09 — Multi-Way Join + Failure Recovery (Capstone)

> The production enrichment pipeline: `trade ⋈ quote (interval, ±5s) ⋈ fx (temporal,
> AS OF tradeTime) ⋈ account (async lookup)`. Then crash mid-flight after `N`
> enriched records and prove the join state recovers correctly. EXACTLY_ONCE Kafka
> sink + `read_committed` consumer downstream → zero duplicates on
> `topic.enriched.s09` even though the job crashed.
>
> This scenario combines every join concept from scenarios 07 and 08 into one
> pipeline, then re-exercises the reliability story from scenarios 01–05 on top.

---

## What this teaches

| Concept | Where you see it |
|---|---|
| Multi-input pipeline assembly | `App09MultiWayJoin.main()` — sources → interval → temporal → lookup → sink |
| Interval join (from S07) re-used as a building block | `trades.keyBy(ticker).intervalJoin(quotes.keyBy(ticker))` |
| Temporal join (from S08) on the output of an upstream join | `TradeWithQuoteFxJoin` |
| Async lookup (from S08) as the terminal stage | `AccountLookupAndCrash` |
| CrashTrigger inside an `AsyncFunction` | `crashTrigger.increment()` after each lookup completes |
| EXACTLY_ONCE Kafka sink survives the crash | `KafkaSink.setDeliveryGuarantee(EXACTLY_ONCE)` |
| Join state checkpoints and restores like any other state | The whole demo |

---

## Architecture

```
 trades       ──┐
                ├─(interval join, ticker, ±5s)─► tradeWithQuote ──┐
 quotes       ──┘                                                 │
                                                                  ├─(temporal join, currency, AS OF tradeTime)─┐
 fx_rates     ────────────────────────────────────────────────────┘                                             │
                                                                                                                │
                                                                                                                ├─(async lookup, accountId, Postgres)──► enriched
 accounts(PG) ─────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                                                                                │
                                                                                                                ▼
                                                                                            CrashTrigger after CRASH_AFTER_RECORDS
                                                                                                                │
                                                                                                                ▼
                                                                                                  KafkaSink(EXACTLY_ONCE)
                                                                                                                │
                                                                                                                ▼
                                                                                                       topic.enriched.s09
                                                                                                       (read_committed)
```

Four input streams, three join styles, one terminal sink — and a deliberate crash
in the middle.

---

## Run it

```bash
./quickstart.sh 9
    # starts: datagen + quote datagen + FX datagen + account seeder + multi-way job
    # quickstart sets CRASH_AFTER_RECORDS=5000 by default

# Wait until the crash is visible in the JobManager logs.
# Then re-run the verifier — under read_committed it should still report no duplicates:
bash scripts/verify-09.sh
```

The verifier counts duplicate `eventId`s on `topic.enriched.s09` with
`isolation.level=read_committed` and asserts they're zero.

---

## What you should see

1. **Steady state**: the job emits enriched records with all four fields populated —
   trade, prevailing quote, AS-OF FX rate, account info. Throughput is roughly the
   trade rate (~50/sec by default).
2. **At ~5000 enriched records**: `CrashTrigger` throws inside `AccountLookupAndCrash`.
   The job restarts from the last completed checkpoint.
3. **Post-recovery**: the join state restores. Pending interval-join state, pending
   temporal-join pending-trade buffer, in-flight async lookups — all checkpointed,
   all back. Records replayed from Kafka are re-joined to the same quotes/FX/accounts
   they joined with originally. The new records are emitted into a fresh EXACTLY_ONCE
   transaction; `topic.enriched.s09` consumers reading `read_committed` see each
   `eventId` exactly once.

Without the EXACTLY_ONCE Kafka sink + read_committed: duplicates would be visible (the
scenario 01 story). With it: clean.

---

## The "aha" moment

> *Join state is just keyed state, right?*

Right. The interval join's per-key buffered records, the temporal join's per-currency
rate history and pending-trade buffer, the keyed-by-eventId Caffeine cache (no, the
cache is not checkpointed — it rebuilds cold) — all of it lives in the same checkpoint
that Flink uses for any other state. Join recovery is recovery. The watermark
alignment between three event-time sources is what makes the recovery semantically
correct: under replay all three sources advance together, so the join logic re-derives
the same matches.

The crash hits inside an `AsyncFunction`. In-flight async calls are themselves
checkpointed — that's how async lookup integrates with exactly-once.

---

## Code highlights

- [`App09MultiWayJoin.java`](src/main/java/com/workshop/flink/scenario09/App09MultiWayJoin.java) —
  the pipeline assembly. Read this top-to-bottom; it's the production template for
  multi-source enrichment.
- [`operator/TradeWithQuoteFxJoin.java`](src/main/java/com/workshop/flink/scenario09/operator/TradeWithQuoteFxJoin.java) —
  variant of S08's `VersionedFxJoin` that consumes `TradeWithQuote` so the quote
  field is preserved through the temporal join.
- [`operator/AccountLookupAndCrash.java`](src/main/java/com/workshop/flink/scenario09/operator/AccountLookupAndCrash.java) —
  the final async lookup plus the crash trigger. Compare with
  S08's `AsyncAccountLookupFunction` to see the minimal additions.
- [`../common/.../util/CrashTrigger.java`](../common/src/main/java/com/workshop/flink/common/util/CrashTrigger.java) —
  same `transient int count` pattern from scenarios 01 and 03. Resets across restarts
  so the crash is a one-shot demo.

---

## Pitfalls / things to remember

- **Watermark alignment is critical.** With three event-time sources running at very
  different rates (trades, quotes, FX), the slowest one drags down the global watermark
  and joins on the faster ones buffer unboundedly. `withWatermarkAlignment(...)` (the
  DataStream equivalent of S10's `scan.watermark.alignment.group`) caps the drift.
- **Crash trigger placement matters.** Crashing **after** the lookup completes but
  **before** the sink commits is the most interesting case — it tests both async-state
  recovery and Kafka-transaction recovery. Crashing earlier or later misses one of
  those.
- **Async lookup is not part of the Kafka 2PC.** The Postgres SELECT is read-only so it
  can be safely retried. If you swap it for a write, you re-enter scenario 03's
  external-sink territory and need idempotent writes.
- **State size grows: regular > interval > window.** This pipeline uses interval +
  temporal + async. Memory pressure usually comes from the temporal join's
  `MapState<Long, Double>` if `FX_HISTORY_RETAIN_MS` is too generous.
- **The dedup story from scenario 04 is **not** applied here.** If your trade source
  could emit the same eventId twice, you'd want a `DeduplicationFunction` *before* the
  enrichment chain. Combine S04's dedup with this scenario for a complete production
  topology.

---

## Variations to try

1. Lower `CRASH_AFTER_RECORDS=200` to make the crash happen before the first checkpoint.
   The whole job state has to rebuild from scratch — but the EXACTLY_ONCE Kafka sink
   still leaves no duplicates on `topic.enriched.s09`.
2. Set `FX_HISTORY_RETAIN_MS=600000000` (forever) and watch state size in the Flink UI.
   The temporal-join operator dominates.
3. Drop watermark alignment (delete the `withWatermarkAlignment` line). Run the
   workshop's `QUOTE_RATE=5000` for a few minutes. Observe state on the interval-join
   operator climbing — quotes race ahead, trades lag, the interval-join holds
   accumulated quotes waiting for trades.

---

## See also

- **Scenario 07** — stream-stream joins (regular, interval, window). The interval-join
  building block this scenario reuses.
- **Scenario 08** — stream-table joins (lookup, temporal). Both join styles this
  scenario reuses.
- **Scenario 02** — EXACTLY_ONCE Kafka sink. The reliability foundation.
- **Scenario 04** — semantic dedup. Complementary pattern for upstream-duplicate-tolerance.
- **Scenario 10** — event-time + watermarks deep dive. Step 08 specifically covers
  the alignment that this scenario depends on.
- [`../sql-notes/scenario-sql-equivalents.md`](../sql-notes/scenario-sql-equivalents.md#scenario-09--multi-way-join--recovery) —
  the equivalent multi-way SQL query.
