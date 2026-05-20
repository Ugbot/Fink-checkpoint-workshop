# Deployable variants — Scenario 14

The teaching files in [`../sql/`](../sql/) are written for the SQL Client,
where DDL plus a bare terminal `SELECT` is the natural shape and the row-by-row
output of each lookup join is the whole point of the demo. The Ververica Cloud
test harness wants a *streaming job*, so deployable variants of every step
that ends in a meaningful streaming pipeline live here. Each one ends in
`INSERT INTO blackhole_sink` so the job is a valid streaming deployment that
can be submitted, observed in the Flink UI, then killed.

| Step | Deployable file                              | Why / why not |
|------|----------------------------------------------|---------------|
| 01 — basic lookup join                | `01-lookup-join-instrument-master.sql` | Streaming enrichment — perfect deployable shape. |
| 02 — three-sided lookup join          | `02-lookup-join-multi-side.sql`        | Same shape, three lookup sides. |
| 03 — projection-pushed lookup join    | `03-lookup-join-with-projection.sql`   | Narrower lookup payload; compare operator I/O vs 02. |
| 04 — regular join vs lookup join      | `04-regular-join-vs-lookup-join.sql`   | Run alongside 01/03 to compare checkpoint sizes and operator state in the Flink UI. |
| 05 — delta join concept               | *not deployable*                       | Concept walkthrough only — requires Flink 2.2+ to exhibit the optimisation. The SQL is identical to step 04; submitting it on Flink 1.20.4 just gives you the step-04 plan. |

## What to look at in the UI when these are running

The high-leverage observation is the **operator state size** on the Join
operator(s):

- Deployment 01 / 02 / 03 → the join is a `LookupJoin`; state on the right
  side is effectively zero. Checkpoint sizes stay flat.
- Deployment 04 → the join is a regular `Join` over the Fluss changelog;
  state grows with the cardinality of `instrument_master`. Checkpoint sizes
  grow until the table is fully held.

If you have a Flink 2.2 + Fluss 0.8+ cluster available, re-run the step-04
SQL there and confirm the planner picks `DeltaJoin` with `state=[None]` —
that's the punchline of step 05.
