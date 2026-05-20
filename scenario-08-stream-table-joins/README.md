# Scenario 08 — Stream-Table Joins: Lookup vs Temporal/Versioned

> Two apps that look superficially similar — both enrich a trade stream by looking
> up "something on the other side" — but have **opposite** semantics. Lookup join
> answers "what is the current value, right now?" (processing time). Temporal join
> answers "what was the value at trade time?" (event time). They are routinely
> confused. Running both back-to-back makes the difference unmistakable.

---

## What this teaches

| Concept | Where you see it |
|---|---|
| Async lookup join with a Caffeine LRU cache | `App08LookupJoin` + `AsyncAccountLookupFunction` |
| `AsyncDataStream.unorderedWait` + `RichAsyncFunction` | `App08LookupJoin.main()` |
| Cache TTL trades staleness vs DB load | `LOOKUP_CACHE_TTL_SEC` env var |
| Temporal/versioned join keyed by currency | `App08TemporalJoin` + `VersionedFxJoin` |
| `MapState<Long rateTime, Double rate>` for the rate history | `VersionedFxJoin.rateHistory` |
| Event-time semantics: "AS OF tradeTime" | `findRateTimeAtOrBefore(t)` |
| Deterministic enrichment under replay (temporal) vs non-deterministic (lookup) | The whole scenario |

---

## Architecture

### App08LookupJoin — trade ⋈ accounts (Postgres, processing time)

```
 topic.in  ──►  Kafka source ──►  AsyncDataStream.unorderedWait
                                    │
                                    ▼
                              AsyncAccountLookupFunction
                                    │  Caffeine cache (LRU, TTL=5min)
                                    │  ▼ on miss → Postgres SELECT
                                    │  Postgres ◄── threadPool of 8
                                    │
                                    ▼
                              TradeWithAccount  ──►  topic.enriched.s08
```

### App08TemporalJoin — trade ⋈ FX rate (AS OF tradeTime, event time)

```
 topic.in       ──►  WatermarkStrategy ──►  keyBy(currency)
                                                 │
 topic.fxrates  ──►  WatermarkStrategy ──►  keyBy(currency)
                                                 │
                                                 ▼
                                       VersionedFxJoin (KeyedCoProcessFunction)
                                       MapState<rateTime, rate>  per currency
                                       MapState<eventId, trade>  pending
                                       on trade: find rate ≤ trade.tradeTime
                                                 │
                                                 ▼
                                              TradeWithFx ──►  topic.enriched.s08
```

The currency for a trade is derived deterministically from its `accountId` via
[`CurrencyOf.fromAccountId(...)`](../common/src/main/java/com/workshop/flink/common/util/CurrencyOf.java) —
this is a workshop simplification (no extra lookup), but the temporal-join logic
itself is realistic.

---

## Run it

```bash
./quickstart.sh 8    # datagen + FX datagen + account seeder + both 08 apps

bash scripts/verify-08.sh   # asserts records have non-null account / fxRate
                            # depending on which app produced them
```

---

## The two join styles, side by side

### Lookup join (08a) — "what is the value right now?"

- Cache check first; if hit, complete immediately on the mailbox thread (cheap).
- On miss, dispatch a Postgres SELECT to a thread pool.
- `unorderedWait` allows other records to make progress while this lookup pends.
- 5-second per-call timeout; on timeout the record passes through with `account=null`
  (LEFT-JOIN semantics).
- Cache TTL is **wall-clock** (Caffeine `expireAfterWrite`). Insert a new account row
  in Postgres at 12:00 — trades for that account between 12:00 and the next cache
  miss won't be enriched.

The output is **non-deterministic** under replay: if you re-run the same Kafka offsets
24 h later with a new accounts row, you'll get different enrichment than the original
run produced.

### Temporal join (08b) — "what was the value at the time the trade happened?"

- Both streams are event-time, both watermarked.
- Per-currency `MapState<Long, Double>` holds the entire FX rate history.
- For each trade, scan that map for the largest `rateTime ≤ trade.tradeTime`.
- Output is **deterministic** under replay — the FX history is stable, the predicate
  is deterministic, so the same input always produces the same output.
- A trade whose currency has no rate yet is buffered in `pendingTrades` and emitted
  when a covering rate arrives, or evicted with `fxRate=null` after `FX_MAX_PENDING_MS`
  if no covering rate ever shows up.

State retention:
- Rate history is GC'd via event-time timer once it's `FX_HISTORY_RETAIN_MS` behind the
  watermark (with the youngest pre-cutoff entry preserved so late trades can still
  resolve).
- Pending trades are bounded by `FX_MAX_PENDING_MS` (default 1 min).

---

## What you should see

A sample of `topic.enriched.s08` will mix `TradeWithAccount` rows (from 08a) and
`TradeWithFx` rows (from 08b). The verify script counts each separately:

```
── Lookup join (App08LookupJoin) ─────────────────────────
  with account     : N      ← every trade should be matched, accounts seeded
  null account     : ~0
  cache hits       : grows with time as the working set warms up

── Temporal join (App08TemporalJoin) ─────────────────────
  with FX rate     : N
  null FX rate     : ~0     ← non-zero only if a currency had no rates yet
```

Stop and re-deploy 08b: the per-trade FX rates are identical to the original run. Stop
and re-deploy 08a after inserting a new accounts row: the enrichment may differ.

---

## The "aha" moment

> *Lookup join is simpler — why use temporal join at all?*

Three reasons:
1. **Determinism under replay.** Audit, billing, and reconciliation jobs cannot
   tolerate "the same Kafka offset produced different output on the rerun".
2. **No external dependency for the join.** Temporal joins resolve from in-cluster
   state, not a database. No connection pool, no DB outage cascading into job failure.
3. **Bounded staleness is explicit.** Lookup joins have *implicit* staleness equal to
   the cache TTL. Temporal joins are exact at event-time — staleness is whatever the
   FX rate stream's delivery latency is.

The reverse is also true: lookup joins are simpler for dimensions that change rarely
relative to the cache TTL and don't need replay-determinism.

---

## Code highlights

- [`operator/AsyncAccountLookupFunction.java`](src/main/java/com/workshop/flink/scenario08/operator/AsyncAccountLookupFunction.java) —
  the production-shaped async-lookup pattern: Caffeine LRU + a JDBC fallback on a
  bounded thread pool. Customizable timeout + capacity.
- [`operator/VersionedFxJoin.java`](src/main/java/com/workshop/flink/scenario08/operator/VersionedFxJoin.java) —
  the temporal-join logic. Worth reading top-to-bottom — every state field and timer
  has a comment explaining why it's there.
- [`App08LookupJoin.java`](src/main/java/com/workshop/flink/scenario08/App08LookupJoin.java) —
  surface env vars: `LOOKUP_CACHE_MAX`, `LOOKUP_CACHE_TTL_SEC`, `LOOKUP_ASYNC_THREADS`,
  `LOOKUP_TIMEOUT_MS`, `LOOKUP_CAPACITY`.
- [`App08TemporalJoin.java`](src/main/java/com/workshop/flink/scenario08/App08TemporalJoin.java) —
  surface env vars: `FX_HISTORY_RETAIN_MS`, `FX_MAX_PENDING_MS`.

---

## Pitfalls / things to remember

- **Async lookup needs **idempotent** lookups.** The lookup function may be called
  with the same accountId multiple times under replay. That's fine for read-only
  SELECTs; not fine if your lookup *mutates* state.
- **Cache capacity is per task.** With 8 parallel subtasks and `LOOKUP_CACHE_MAX=1000`,
  the cluster holds 8000 entries. Plan accordingly for high-cardinality dimensions.
- **Temporal join's `MapState` grows per (key × distinct rateTime).** GC the history
  aggressively with `FX_HISTORY_RETAIN_MS`. A retain window of "forever" combined with
  a high-rate changelog will eventually blow up.
- **Pending-trade buffer is the back-pressure valve.** Too short and trades that arrive
  before their currency's first rate get LEFT-JOIN-null'd; too long and state size
  spikes during slow startups.
- **Flink SQL has both shapes built in.** `LEFT JOIN accounts FOR SYSTEM_TIME AS OF
  PROCTIME() ON ...` is the lookup-join syntax with JDBC connector caching; `LEFT JOIN
  fx_rates FOR SYSTEM_TIME AS OF trade_ts ON ...` is the temporal-join syntax. See
  [`../sql-notes/scenario-sql-equivalents.md`](../sql-notes/scenario-sql-equivalents.md#scenario-08--stream-table-joins-lookup-vs-temporal).

---

## See also

- **Scenario 07** — stream-stream joins (regular, interval, window). The "two streams"
  side of the join taxonomy that complements this scenario's "stream + table" side.
- **Scenario 09** — multi-way capstone combining 07's interval join + 08's temporal
  join + 08's async lookup join in one topology, plus crash recovery.
- **Scenario 10** — WORKSHOP.md step 08 covers `scan.watermark.alignment.group`, which
  becomes important if you scale the FX rate source independently of trades.
- [`../sql-notes/scenario-sql-equivalents.md`](../sql-notes/scenario-sql-equivalents.md#scenario-08--stream-table-joins-lookup-vs-temporal) —
  the SQL version.
- Flink async I/O docs:
  <https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/asyncio/>
