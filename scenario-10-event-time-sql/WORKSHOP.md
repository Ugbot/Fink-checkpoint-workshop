# Scenario 10 Workshop — Event Time, Watermarks & Late Data in Flink SQL

> **Audience:** developers who can read SQL and understand "streaming vs batch" at a
> rough level but have not yet shipped a production Flink job dealing with late or
> out-of-order data.
>
> **Time:** ~60 minutes if you walk through all 8 steps. ~25 minutes for the
> condensed path (steps 02 → 04 → 05 → 06 → 07).
>
> **Format:** SQL-first. Each step is one self-contained `.sql` file that you
> deploy to Ververica Cloud via the test harness, observe in the VVC UI, then
> discuss before moving on.

---

## What you will learn

By the end of this scenario you should be able to:

1. Explain the difference between **processing time** and **event time** in one
   sentence — and pick the right one for a given requirement.
2. Read and write a `WATERMARK FOR ts AS ts - INTERVAL '5' SECOND` clause and
   say what it means operationally.
3. Diagnose a Flink job whose windows have **stopped emitting** and identify
   which of the four common causes is at fault.
4. Configure the three production knobs that almost every event-time job
   eventually needs: `scan.watermark.idle-timeout`,
   `table.exec.window-allowed-lateness`, and `scan.watermark.alignment.*`.
5. Build a **late-data side channel** so records that miss the watermark are
   captured instead of silently dropped.

---

## Mental model — read this first

A streaming SQL job has two distinct clocks running at the same time:

- **Processing time** is wall-clock time on whichever machine is currently
  running the operator. Cheap, simple, **not reproducible**.
- **Event time** is the timestamp that lives *inside* every record (here:
  `trade_time`). Deterministic across replays — but the engine has to *guess*
  when it has seen enough records to declare a point in event-time "done".

That guess is called a **watermark**. A watermark of `W` on an operator means
*"I believe no record with event-time ≤ W will arrive after this point"*. The
operator uses watermarks to decide when to close a window, when to emit a
join match, when to expire state — anything that is gated on "we're past time T".

Two complications follow:

1. The engine's belief is sometimes wrong — a record older than the watermark
   *does* show up. That record is **late**. Default behaviour: dropped silently.
2. Watermarks are a per-partition minimum. If one Kafka partition stops emitting
   ("idle"), or runs hours ahead of another partition ("skew"), the operator's
   watermark misbehaves. Each has a dedicated knob.

Almost every problem you will hit with event-time Flink reduces to one of those
two complications. The steps below give you each in isolation.

---

## Prerequisites

You need:

- A VVC workspace + namespace with at least one running session/PER_JOB target.
  The pipelines default to `default-queue` — override at run time if yours has a
  different name.
- The trade datagen job running so `topic.in` is populated. Deploy
  `workshop-datagen` first (it's the standard `FinancialDatagenJob` from common/).
- For **Step 08 only**: a quotes datagen too. Deploy `workshop-quote-datagen`.
- The Ververica test harness running locally (`./start.sh` in `ververica-test-harness/`)
  so you can fire the pipelines.

All of the `.sql` files in `sql-deployments/` sink to `'connector' = 'blackhole'`
so they are safe to run anywhere — they consume from Kafka, do real work, and
discard the output. Observation is through the **VVC UI**, the **Flink job
graph**, and the operator-level **watermark** displayed on each operator.

---

## How to use this workshop

For each step you'll do four things:

1. **Read the concept** in this guide (one paragraph).
2. **Deploy** via the harness — either click the corresponding
   `workshop-scenario-10-NN-*` pipeline in the harness UI or POST it via the API.
3. **Observe** in the VVC UI: the job graph, the operator-level watermark, the
   exception tab (sometimes), and `flinkConfiguration` on the deployment page.
4. **Reflect** — answer the "aha" question before reading the answer below it.

Pipelines are listed in [the README](./README.md#deploy-via-the-ververica-cloud-test-harness).
Underlying SQL files are in [`sql-deployments/`](./sql-deployments/) (deployable
variants) and [`sql/`](./sql/) (richer teaching variants for the SQL Client).

---

## Step 01 — `PROCTIME()` baseline

### Concept
There is no event-time anywhere in this job. The window groups records by
*wall-clock time on whichever subtask processes them*.

### Real-world story
A team builds a "live trades-per-second" dashboard. They use processing time
because it's the path of least resistance. The dashboard looks great — until
they need to **replay yesterday's Kafka offsets to debug an incident** and
discover the replay produces *completely different counts* than the original run.

### Deploy
`workshop-scenario-10-01-source-proctime`

### What to look at
- Job graph: a Kafka source → window aggregate → blackhole sink.
- **Operator watermark** column on the window aggregate: **never moves**. Flink
  has no event-time concept here. (The window still fires — it uses processing
  time directly.)
- `deployment.get`'s status: `RUNNING`. Output rates ~ datagen rate.

### Aha
> *Why is this output non-reproducible?*

Because the window assigns each record to a window based on `PROCTIME()` — the
operator's wall clock when the record happens to arrive. Re-run the job and
the windows fire at different real-world moments. Replay Kafka from an earlier
offset and an old record might land in a "future" window.

### Pitfall to remember
Processing-time windows are fine for **live monitoring** dashboards where
"close enough" beats "deterministic." Never use them for billing, compliance,
SLAs, or anywhere downstream replays.

### Try this
Stop the job, then re-deploy. Watch the per-window counts shift around — same
input data, different output, because the window boundaries float with the
restart wall-clock.

---

## Step 02 — Event-time source with `WATERMARK`

### Concept
The same source DDL but with one extra clause:
`WATERMARK FOR trade_ts AS trade_ts - INTERVAL '5' SECOND`.
That turns `trade_ts` into the operator's clock.

### Real-world story
"OK we'll switch to event time." The team adds the `WATERMARK` clause and
nothing else. **It now matters how far out of order their data is.** The 5-second
delay is a *promise to the optimizer*: "I claim no record will arrive more
than 5s after the maximum timestamp I have seen so far."

If that promise is wrong (a record arrives 10s late), it will be classified as
**late** and dropped. We will fix that in steps 06 and 07. For now, the
promise holds because the synthetic datagen is in-order.

### Deploy
`workshop-scenario-10-02-source-event-time`

### What to look at
- Job graph as before, but now the operator watermark **advances**. It lags the
  current `max(trade_ts)` by exactly the declared 5 seconds.
- The new computed column on the source: `trade_ts AS TO_TIMESTAMP_LTZ(trade_time, 3)`.
- Watermark stays a few seconds behind the most recent record.

### Aha
> *Why subtract 5 seconds rather than just using `max(trade_ts)` directly?*

Because `max(trade_ts)` would mean "I am 100% certain no record older than this
will arrive." Real systems can't promise that — networks reorder, retries, etc.
Subtracting a buffer (here 5s) tells Flink: "treat the last 5 seconds of records
as still in-flight; only consider time > watermark as settled."

### Pitfall to remember
Tune the buffer to your **observed 99th-percentile out-of-order delay**, not
to your worst case. A 1-hour buffer for safety means 1-hour latency on every
window output. Most teams pick 5–30 seconds; some go to a few minutes for
cross-region traffic.

### Try this
Compare the operator watermark gauge between steps 01 and 02 in the VVC UI.
Step 01 shows nothing because there is no event-time. Step 02 shows a
monotonically-increasing watermark trailing wall-clock.

---

## Step 03 — Processing-time tumbling window (30 s)

### Concept
A 30-second `TUMBLE` over `PROCTIME()`. The job emits one row per
(ticker, 30s-of-wall-clock).

### Real-world story
Last quarter the team built a "trades per ticker, every 30 seconds" report on
top of step 01's source. They put dashboards on it. Now the SRE team wants the
same numbers in a **replayed-from-yesterday** test to validate an alert. The
numbers don't match the dashboard. Confusion ensues.

### Deploy
`workshop-scenario-10-03-tumbling-proctime`

### What to look at
- Records flowing through at the datagen rate.
- Window emissions happen roughly every 30 wall-clock seconds — independent of
  how fast events are flowing through the source.
- Stop and restart the job: window boundaries shift.

### Aha
> *If you stop this job and restart it at a different wall-clock moment, do the
> 30-second buckets contain the same trades they used to?*

No. Each restart re-clocks the windows from scratch.

### Pitfall to remember
Anything that ever needs to be **reconciled against a re-run** must not use
processing-time windows.

---

## Step 04 — Event-time tumbling window (30 s)

### Concept
Same SQL, but the window is over `trade_ts` (the event-time column). The
window emits when **the watermark crosses the window end**, not when wall-clock
does.

### Real-world story
The team rewrites step 03 to use event time. Now replays produce the *same*
numbers as the original run. They also notice **output latency is now bounded
by the watermark lag** — every window takes ~5 seconds longer to emit than
before, because the watermark = `max(trade_ts) - 5s`.

### Deploy
`workshop-scenario-10-04-tumbling-event-time`

### What to look at
- Window emissions are gated by the watermark. If you pause the source (no new
  records), the watermark stops moving and **windows stop firing** even though
  wall-clock is rolling.
- Restart from an earlier Kafka offset: the windows re-emit the same counts.
- Per-window result is deterministic.

### Aha
> *Why does this job appear to "freeze" when you stop the upstream producer?*

The watermark is a function of `max(trade_ts)` over records that have actually
*arrived*. No new arrivals → no advance → no windows close. Step 05 fixes the
case where one partition pauses but the rest are still moving; step 06 fixes the
case where late records do arrive.

### Pitfall to remember
Watermark stalls produce **lots of false bug reports**. ("Why is my count not
updating?") Always check the per-operator watermark in the VVC UI before
suspecting code. It's almost always the answer.

### Try this
Pause `workshop-datagen` (stop the deployment, don't delete it), wait two
minutes, restart it. Watch the operator watermark and window emissions on
`workshop-scenario-10-04-tumbling-event-time` while the datagen is paused vs
flowing again.

---

## Step 05 — Idle source timeout

### Concept
`scan.watermark.idle-timeout = '30 s'` on the source DDL declares: "if a
partition produces no records for 30 seconds, **stop counting it** in the
watermark minimum."

### Real-world story
A 16-partition Kafka topic. One partition has no traders assigned to it
during off-hours, so it produces zero records. Without `idle-timeout`, the
operator watermark is pinned at *that* partition's last record forever, and
every window emission stalls. With it, the partition is excluded from the
watermark calculation until it produces something again.

### Deploy
`workshop-scenario-10-05-idle-source-timeout`

### What to look at
- The source table DDL: `'scan.watermark.idle-timeout' = '30 s'` line.
- Behaviour identical to step 04 under steady-state load. The difference shows
  up only when a partition goes quiet.
- In the VVC UI you can see the partition watermarks per subtask.

### Aha
> *Idle timeout has a dangerous-feeling tradeoff. What is it?*

It tells the operator to **pretend the silent partition is caught up** so the
overall watermark can advance. If that partition later produces a record older
than the now-advanced watermark, that record is **late** and gets dropped.

### Pitfall to remember
Pick the timeout based on the **longest legitimate quiet period** for the
slowest-but-still-real partition. Too short → late drops. Too long → window
emissions stall during quiet periods. 30 s is the common default; rarely below
10 s, rarely above a few minutes.

### Try this
In step 04 in the VVC UI, find the per-subtask watermark display. Now imagine
one subtask is reading from a partition that just went quiet — its watermark
would freeze and pull down `min(...)`. That's what `idle-timeout` rescues.

---

## Step 06 — Allowed lateness (retract + revise)

### Concept
`table.exec.window-allowed-lateness = '1 min'` tells Flink to keep window state
around for an extra minute after the watermark first crossed its end. When a
late record arrives within that minute, the window **retracts** its previous
result and **re-emits** the corrected one.

### Real-world story
A trading firm needs to publish a 30-second VWAP. Most trades are on time;
~0.1% arrive up to 45 seconds late due to cross-region network jitter. The firm
is willing to publish a number quickly and revise it if late data shows up,
but **not willing to silently drop 0.1% of trades** when accuracy is what
they're selling.

### Deploy
`workshop-scenario-10-06-allowed-lateness`

### What to look at
- The deployment's `flinkConfiguration` panel shows the
  `table.exec.window-allowed-lateness: 1 min` entry.
- The sink is declared with a `PRIMARY KEY` so it accepts the retract stream.
  (A plain Kafka append-only sink would *reject* this query — that's part of
  the lesson.)
- Output records carry a row-kind flag (`+I`, `-U`, `+U`) — visible if you
  swap the blackhole sink for a print/Kafka one.

### Aha
> *What changes about the downstream system when you turn on allowed-lateness?*

It now has to be **retraction-aware**. Materialized views, upsert sinks,
dashboards that show "the latest value" are fine. Append-only logs, fan-out to
non-idempotent APIs, billing emails — not fine.

### Pitfall to remember
Allowed-lateness is a contract between you, the optimizer, and your downstream
sink. All three must agree. If a downstream consumer doesn't handle retracts,
they'll see the original *and* corrected count as two unrelated records and
double-count.

### Try this
Set allowed-lateness very high (`'24 h'`) and watch the window operator's
state size in the VVC UI. State is held for every still-open window across
the full lateness window — pick a value, don't max it out.

---

## Step 07 — Late-data side channel

### Concept
A predicate built on `CURRENT_WATERMARK(trade_ts)` selects every record whose
event-time is older than the current watermark, and INSERTs them into a
**dedicated** "late records" table. No window operator involved — just routing.

### Real-world story
Compliance needs *every* trade audited, even ones that arrived too late for
the real-time analytics. The main job drops late records as designed; this
side-channel captures them for offline reconciliation. This pattern complements
allowed-lateness — use both for tight numerical accuracy *plus* an audit trail
of records that exceeded the lateness budget.

### Deploy
`workshop-scenario-10-07-late-data-side-channel`

### What to look at
- A `WHERE CURRENT_WATERMARK(trade_ts) IS NOT NULL AND trade_ts < CURRENT_WATERMARK(trade_ts)`
  clause — this is the late-record filter.
- The job runs but emits zero records under steady load (our datagen is
  in-order). You won't see late routing happen unless you induce lateness.

### Aha
> *Why isn't this an out-of-the-box `side-output:` annotation like the DataStream API has?*

Flink SQL doesn't expose window-late side outputs as first-class syntax (yet).
`CURRENT_WATERMARK()` was added in Flink 1.17 specifically so this pattern is
expressible without DataStream gymnastics.

### Pitfall to remember
The predicate evaluates **per record**, not per window. It catches records
that miss the watermark *altogether*, which is the input to a window. If the
record makes it into a window but is dropped because allowed-lateness has
expired, this side channel won't see it — those need to be caught by an
allowed-lateness-aware sink instead.

### Try this
To make this job produce output, run a synthetic source that emits records with
timestamps in the past. The datagen here writes "now"-ish timestamps so nothing
ever looks late. A modified datagen with a configurable `LATENESS_PCT` is left
as an exercise — or rerun this against a paused-and-resumed datagen and watch
records get classified as late after the gap.

---

## Step 08 — Watermark alignment

### Concept
Two Kafka sources joined by an interval-join. Both sources declare
`scan.watermark.alignment.group = 's10-aligned'` and `max-drift = '30 s'`.
Whichever source is running ahead in event-time gets *paused* until the lagging
source catches up to within 30 s.

### Real-world story
A trade ⋈ quote interval-join needs both streams roughly in sync. Quotes flow
at 10× the trade rate. Without alignment, the quote source races ahead and the
join operator holds the leading 10–30 minutes of quotes in state, waiting for
trades to catch up. State explodes. With alignment, the quote source self-stalls
when it gets too far ahead.

### Deploy
`workshop-scenario-10-08-watermark-alignment`. Requires `workshop-quote-datagen`
to also be running.

### What to look at
- Both source DDLs share the same `alignment.group` name.
- The job graph shows both sources, the interval join, and a blackhole sink.
- In the VVC UI's per-subtask metrics, look at **source idle time** —
  whichever source is leading periodically goes idle as alignment back-pressures
  it.

### Aha
> *Alignment trades throughput on the leading source for what?*

**Bounded state.** Without it, the interval join holds unbounded buffered
records on the leading side. With it, state is capped roughly at
`max-drift × records/sec`.

### Pitfall to remember
Alignment only helps **between sources that share a group name**. If your job
has two unaligned sources and one aligned source, the aligned one will stall
relative to the lagging unaligned one — usually surprising.

### Try this
Compare the operator state size between
`workshop-scenario-10-08-watermark-alignment` (aligned) and a variant with
the alignment properties removed. Use a higher quote rate (e.g. 5000/sec via
`QUOTE_RATE=5000`) to make the difference dramatic.

---

## Quick-reference cheat sheet

| Symptom | Likely cause | Fix |
|---|---|---|
| Windows never emit anything | Watermark stuck at `MIN_VALUE` — no records yet, or no watermark declared | Add `WATERMARK FOR ts AS ...` to the source DDL |
| Windows stop emitting after a partition pauses | One partition's watermark frozen, dragging the global min | `scan.watermark.idle-timeout` |
| Output is missing records you can see in Kafka | Records arrive after the watermark; they get dropped silently | `table.exec.window-allowed-lateness` and/or step-07 side channel |
| Join state grows unboundedly | One side races ahead of the other | `scan.watermark.alignment.group` + `max-drift` |
| Numbers change on replay | Window over `PROCTIME()` | Use `WATERMARK FOR ts AS ...` + `TUMBLE(ts, ...)` |
| Downstream double-counts after allowed-lateness | Sink isn't retract-aware | Use upsert/PK sink or remove lateness |

---

## Where these concepts re-appear in other scenarios

- **Scenario 05** — first appearance of event-time tumbling windows in DataStream
  (Java). Same `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))`
  pattern that step 02 captures in SQL.
- **Scenario 07** — interval join (07b) and window join (07c) both use the same
  bounded-out-of-orderness watermarks; the SQL equivalent uses exactly the
  syntax from step 02.
- **Scenario 08** — temporal join is `LEFT JOIN ... FOR SYSTEM_TIME AS OF
  event_time_col`. Without watermarks declared on both sides, the planner
  refuses to compile the query.
- **Scenario 09** — multi-way join uses `withWatermarkAlignment(...)` across
  three event-time sources. The SQL equivalent uses
  `scan.watermark.alignment.group` per source as in step 08.

---

## Production checklist

When you ship an event-time SQL job to production, check off:

- [ ] `WATERMARK FOR ts AS ts - INTERVAL 'N' SECOND` on every event-time source.
      N matched to observed p99 out-of-order delay, not worst case.
- [ ] `scan.watermark.idle-timeout` on every source whose partitions can
      legitimately go quiet during off-peak hours.
- [ ] **Decide explicitly** whether late records are tolerable. If not:
  - Use `table.exec.window-allowed-lateness` + a retract-aware sink, **or**
  - Add a step-07 style late-record side channel for audit.
- [ ] If joining 2+ event-time sources, use `scan.watermark.alignment.group`
      and pick a `max-drift` that bounds in-flight state.
- [ ] Monitor the operator watermark in the VVC UI; create an alert if it
      stops advancing for longer than your idle-timeout.
- [ ] Document the watermark contract in the deployment description. Future
      maintainers will not figure it out from the SQL alone.

---

## Glossary

- **Event time** — the timestamp inside a record (e.g. `trade_time`).
- **Processing time** — wall-clock time on the machine running the operator.
- **Watermark** — a per-operator promise that no record with event-time ≤
  `W` will arrive afterwards. Always conservative; always a guess.
- **Bounded out-of-orderness** — the watermark generation strategy used in
  this scenario: `max(seen_event_time) - delay`.
- **Late record** — a record whose event-time is less than the current
  watermark. Default: dropped silently.
- **Allowed lateness** — a per-window grace period during which late records
  retract+revise the window's emitted result.
- **Idle source** — a partition or subtask that produces no records for some
  duration. Without `idle-timeout` it stalls the operator's watermark.
- **Alignment group** — a named set of sources whose watermarks must stay
  within `max-drift` of each other. Backpressures the fastest source.
- **Retract stream** — a changelog stream emitting `-U` (retract) followed by
  `+U` (update) when an aggregate result changes. Required by allowed-lateness.

---

## Next steps

Once you're comfortable with this scenario, the natural next reads in this
workshop are:

- **Scenario 07** (`scenario-07-stream-stream-joins/`) — see the same
  watermarks at work in interval / window / regular joins.
- **Scenario 09** (`scenario-09-multiway-join-recovery/`) — see watermark
  alignment used in production-style multi-source enrichment with crash
  recovery.

If you want to *write* this kind of code at work, the canonical CI/CD pattern
for SQL deployments lives at the harness's classpath builtin
`sql-deployment-cicd` — copy that YAML and parametrise it for your team's
deployments.
