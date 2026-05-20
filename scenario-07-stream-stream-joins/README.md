# Scenario 07 — Stream-Stream Joins: Regular, Interval, Window

> Three sibling apps in one module, each demonstrating a different stream-stream join.
> Together they cover every flavour Flink offers for joining two streams: **regular**
> (INNER / LEFT / RIGHT / FULL OUTER), **interval** (time-bounded), and **window**
> (co-windowed). After this scenario you can articulate which one to use when, and
> what state cost each one carries.

---

## What this teaches

| Concept | Where you see it |
|---|---|
| Regular join, all four kinds in one operator | `App07RegularJoin` + `OrderFillRegularJoin` |
| Bounded join state via per-key timers + StateTtlConfig | `OrderFillRegularJoin.processElement1/2/onTimer` |
| Interval join with `between(-5s, +5s)` | `App07IntervalJoin` |
| Watermarks + idleness for joins | `WatermarkStrategy.forBoundedOutOfOrderness(...)` on both sources |
| Tumbling-window co-aggregation | `App07WindowJoin` (per-side count + keyed cogroup) |
| When state is bounded automatically vs needs a TTL | Interval/window: automatic. Regular: TTL. |

---

## Architecture

```
                                                ┌─────────────────────────────┐
   topic.in  ──►  OrderFillSplitterJob ─────►   │ topic.orders                │
                  (drop 20% of fills,           │ topic.fills                 │
                   inject 5% orphan fills)      └──────────────┬──────────────┘
                                                                │
   ┌────────────────────────────────────────────────────────────┴──────────┐
   │                                                                       │
   ▼                                                                       │
 App07RegularJoin   ── KeyedCoProcessFunction by eventId                    │
                       ValueState<order>, ValueState<fill>,                 │
                       StateTtlConfig(1h) + processing-time timer (30s)     │
                       emits JoinedOrderFill { joinKind, order, fill }      │
                       (joinKind ∈ INNER / LEFT_ORPHAN / RIGHT_ORPHAN)      │
                       └──────► topic.enriched.s07                          │
                                                                            │
   topic.in    ──►  App07IntervalJoin ── intervalJoin(quote ±5s) by ticker  │
   topic.quotes ──►                       (event-time, auto-evicted)        │
                                          └──► topic.enriched.s07           │
                                                                            │
   topic.in    ──►  App07WindowJoin   ── per-side TUMBLE(60s) count         │
   topic.quotes ──►                       co-grouped by (ticker, windowStart)
                                          └──► topic.enriched.s07           │
```

The three apps all write to `topic.enriched.s07` so a single verify script can sample
across all three.

---

## Run it

```bash
./quickstart.sh 7    # starts datagen + quote datagen + order/fill splitter +
                     # all three S07 apps

bash scripts/verify-07.sh   # asserts INNER + LEFT_ORPHAN buckets non-empty,
                            # plus interval-joined rows + window rows
```

You can also deploy individual apps via:

```bash
flink run scenario-07-stream-stream-joins/target/scenario-07-regular-join-jar-with-dependencies.jar
flink run scenario-07-stream-stream-joins/target/scenario-07-interval-join-jar-with-dependencies.jar
flink run scenario-07-stream-stream-joins/target/scenario-07-window-join-jar-with-dependencies.jar
```

---

## The three join styles

### App07RegularJoin — orders ⋈ fills

Reads `topic.orders` and `topic.fills`, both keyed by `eventId`. The custom operator
`OrderFillRegularJoin` keeps both sides in keyed state and emits exactly **one** output
row per `eventId`, tagged with:

- `INNER` — order and fill both arrived
- `LEFT_ORPHAN` — order present, no fill within 30 s (the timer deadline)
- `RIGHT_ORPHAN` — fill present, no order ever matched

There's no `FULL_OUTER` enum because emitting the union of LEFT and RIGHT *is* the full
outer join. Tagging keeps all four SQL variants visible from one stream.

**State retention:**
- 30-second processing-time timer per key — the orphan emission deadline.
- 1-hour `StateTtlConfig` fallback — a safety net in case the timer was skipped (it
  shouldn't be).

In SQL the equivalent uses `table.exec.state.ttl` to bound regular-join state. See
[`../sql-notes/scenario-sql-equivalents.md`](../sql-notes/scenario-sql-equivalents.md#scenario-07--stream-stream-joins).

### App07IntervalJoin — trade ⋈ quote ±5 s

Reads `topic.in` (trades) and `topic.quotes`. For every trade, the operator finds all
quotes for the same ticker whose `quoteTime` is within ±5 s of the trade's `tradeTime`,
and emits one `TradeWithQuote` row per pair.

**State retention is automatic** — the interval itself bounds the window. Once the
trade's watermark advances past `tradeTime + 5s`, the matching quotes can be GC'd.
No TTL knob needed.

This is the most-used join in Flink streaming work. Whenever you want a "tag every
record with the most recent X for the same key" semantic, this is what you reach for.

### App07WindowJoin — per-minute trade-vs-quote volume

Reads the same two streams but uses a co-aggregation approach: tumble each side
independently into 1-minute event-time windows, then key-cogroup the two count
streams on (ticker, windowStart). One output row per (ticker, minute) with both counts.

State retention is bounded by `2 × window size` — once both sides' counts have been
emitted for a given window, the cogroup state for that key is cleared.

The DataStream `trades.join(quotes).where(...).equalTo(...).window(...)` API exists
but emits the cartesian product per window, which is too verbose for high cardinalities
— hence the count-then-cogroup pattern.

---

## What you should see

The verify script counts records per `joinKind` over a 30-second sample of
`topic.enriched.s07`:

```
── Regular join (App07RegularJoin) ───────────────────────
  INNER          : ~2000   (80% of orders matched a fill)
  LEFT_ORPHAN    : ~500    (20% drop rate from the splitter)
  RIGHT_ORPHAN   : ~100    (5% orphan-fill rate)

── Interval join (App07IntervalJoin) ─────────────────────
  trade+quote    : ~2500   (1 row per trade per matching quote)

── Window join (App07WindowJoin) ─────────────────────────
  ticker volume  : ~20     (~2 windows × 10 tickers)
```

The relative magnitudes tell you which join style produces how much output for the same
input rate. Regular joins are roughly 1:1 with input. Interval joins multiply by the
average number of quotes-per-trade within ±5s. Window joins compress to one row per
(key, window).

---

## The "aha" moment

> *Which of the three would I use to "join every trade with its prevailing quote"?*

Interval join with `between(-5s, 0s)` (asymmetric: the quote must arrive before or at
the trade, not after). Regular join would also work but would carry unbounded state.
Window join is wrong: it would emit only one (trade, quote) pair *per window* even if
there are many.

> *Which would I use to "find unfilled orders"?*

Regular join with LEFT OUTER (or this scenario's tagged variant). Interval/window
joins are inner-shaped — they don't naturally express "the LEFT side appeared but
the RIGHT didn't within time T".

---

## Code highlights

- [`operator/OrderFillRegularJoin.java`](src/main/java/com/workshop/flink/scenario07/operator/OrderFillRegularJoin.java) —
  the heart of the regular-join demo. Read this carefully — it's the canonical
  pattern for building stream-stream joins by hand in DataStream.
- [`App07IntervalJoin.java`](src/main/java/com/workshop/flink/scenario07/App07IntervalJoin.java) —
  the minimal interval-join idiom: `keyBy(...).intervalJoin(other.keyBy(...)).between(...).process(...)`.
- [`App07WindowJoin.java`](src/main/java/com/workshop/flink/scenario07/App07WindowJoin.java) —
  the count-then-cogroup pattern for window joins.
- [`../common/.../datagen/OrderFillSplitterJob.java`](../common/src/main/java/com/workshop/flink/common/datagen/OrderFillSplitterJob.java) —
  the synthetic order/fill source. `DROP_FILL_PCT` and `ORPHAN_FILL_PCT` env vars
  tune how many LEFT/RIGHT orphans appear.

---

## Pitfalls / things to remember

- **Regular joins grow state forever** unless you TTL it (or use timers). Workshop
  default is 1-hour TTL + a 30-second timer; production workloads often pick TTL based
  on the SLA for declaring an orphan.
- **Watermark alignment matters for joins.** If trades flow at 50/sec and quotes at
  500/sec, the faster source will be far ahead and the join state on the slow side
  bloats. Scenario 10 step 08 covers `scan.watermark.alignment.group` — the same lesson
  applies here at the DataStream level via `withWatermarkAlignment(...)`.
- **Interval-join timestamps are event-time.** The interval is measured in event-time,
  not wall-clock. A late quote can still join with an old trade.
- **Window joins drop late records by default.** Use `allowed-lateness` (scenario 10
  step 06) if you need late join matches.
- **Don't conflate "regular join + TTL" with "interval join".** They have different
  semantics for out-of-order records and for re-arrivals after eviction.

---

## See also

- **Scenario 08** — stream-table joins: async lookup vs temporal/versioned. The
  complementary picture to this scenario's stream-stream joins.
- **Scenario 09** — multi-way capstone: combines an interval join + temporal join +
  async lookup join, with crash recovery to prove join state checkpoints correctly.
- **Scenario 10** — event-time + watermark fundamentals. Read its WORKSHOP.md if any
  of the watermark talk here feels unfamiliar.
- [`../sql-notes/scenario-sql-equivalents.md`](../sql-notes/scenario-sql-equivalents.md#scenario-07--stream-stream-joins) —
  the SQL version of all three apps in this scenario.
