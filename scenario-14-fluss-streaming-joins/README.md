# Scenario 14 — Fluss Streaming & Lookup Joins

> Fourth scenario in the Fluss track. Assumes you've done scenarios 11, 12
> and 13 — you know what a Fluss catalog is, what a PK table looks like,
> what "wide schema + projection pushdown" buys you, and how partial
> updates compose multiple sources into one PK row.
>
> This scenario is about **joining a streaming source against Fluss PK
> tables** — the operational workload Fluss is designed around, and the
> first place a typical user actually feels the difference between Fluss
> and a Kafka-shaped storage system.

---

## What you'll learn

| Concept | Where you see it |
|---|---|
| The lookup-join idiom (`FOR SYSTEM_TIME AS OF`) against a Fluss PK table | `sql/01-lookup-join-instrument-master.sql` |
| Composing lookup joins across multiple sides — two Fluss + one Postgres | `sql/02-lookup-join-multi-side.sql` |
| Projection pushdown into the lookup path (narrower lookups, lower cost) | `sql/03-lookup-join-with-projection.sql` |
| Regular streaming join over the Fluss changelog — and why it costs state | `sql/04-regular-join-vs-lookup-join.sql` |
| Delta join (Flink 2.2 + Fluss 0.8+) — concept walkthrough only | `sql/05-delta-join-concept.sql` |

The headline takeaway: a `LEFT JOIN ... FOR SYSTEM_TIME AS OF <proctime>`
against a Fluss PK table is **state-free on the right side**. A regular
`JOIN` over the same Fluss table is correct but pays for the right side
in operator state. Pick the join shape that matches the semantics you
actually need — most enrichment use-cases are lookups.

---

## Prerequisites

The full workshop infra must be running:

```bash
podman-compose up -d --build         # Kafka + Flink + Fluss + Postgres
bash scripts/init-fluss.sh           # creates fluss.workshop.<tables>
bash scripts/seed-fluss.sh           # seeds instrument_master and account_profile
bash scripts/seed-accounts.sh        # seeds the Postgres `accounts` dim
./quickstart.sh 14                   # ensures the trade datagen is producing
```

`scripts/seed-fluss.sh` populates the wide Fluss PK tables used in every
step here. Without it the lookup-join queries run but produce all-NULL
right-side columns.

If you've done scenario 11 you already have everything from the first three
lines; only the last two are scenario-14-specific.

---

## How to run it

The SQL files in [`sql/`](./sql/) are written for the **SQL Client / SQL
Gateway**. Run them in order:

```bash
bash scripts/sql-client.sh
# Then paste any of the .sql files in order:
#   01 → 02 → 03 → 04 → 05
```

Each file is self-contained — it (re)registers the Fluss catalog at the
top, creates the temporary Kafka source it needs, and ends with a SELECT
or pseudo-EXPLAIN. There's no shared session state between files; you can
hop straight to step 03 or step 04 if you only want the comparison.

For VVC deployment of steps 01–04, see [`sql-deployments/`](./sql-deployments/)
and the matching harness pipeline
`workshop-scenario-14-NN-<step>-deploy.yaml`. Step 05 is concept-only
and has no deployable variant.

---

## The 5 steps

1. **Basic lookup join** — Kafka trade stream `LEFT JOIN fluss.workshop.instrument_master FOR SYSTEM_TIME AS OF t.proc_time`.
   The single-side idiom. Zero state on the right.
2. **Multi-side lookup join** — same probe stream, three lookup sides
   (`instrument_master`, `account_profile`, Postgres `accounts`). Shows
   how the pattern composes across connectors and how JDBC needs
   `lookup.cache` while Fluss serves lookups natively.
3. **Projection pushdown** — same multi-side join, narrowed to two
   columns per Fluss side. The `EXPLAIN` diff vs step 02 shows the
   planner pushing the `SELECT` list into the lookup operator so the
   tablet server only returns what's used.
4. **Regular join vs lookup join** — the same enrichment expressed as a
   plain `JOIN` (no `FOR SYSTEM_TIME AS OF`). The planner reads the
   Fluss table as a changelog stream and holds it in operator state.
   This is the headline state-cost comparison of the scenario.
5. **Delta join — concept** — a SQL file plus long block comment
   explaining how Flink 2.2 + Fluss 0.8+ rewrite the step-04 query into
   a `DeltaJoin` operator that has the *expressiveness* of a regular
   join with the *state cost* of a lookup join. The workshop cluster
   is on Flink 1.20.4, so we walk through the optimisation via expected
   `EXPLAIN` diffs rather than running it.

For the deep-dive walkthrough with the "Concept / Story / Observe / Aha"
sections per step, see [`WORKSHOP.md`](./WORKSHOP.md).

---

## Verification

```bash
bash scripts/verify-14.sh
```

The script checks:
- `SHOW CATALOGS` returns `fluss`.
- `SELECT COUNT(*) FROM instrument_master` returns > 0 (seed ran).
- A small smoke-test lookup join against `instrument_master` returns rows
  for synthetic probe values.

---

## See also

- [`../scenario-11-fluss-fundamentals/`](../scenario-11-fluss-fundamentals/) —
  the catalog DDL and PK-table fundamentals this scenario builds on.
- [`../scenario-12-fluss-wide-schemas/`](../scenario-12-fluss-wide-schemas/) —
  projection pushdown explored as a first-class topic; step 03 here
  applies the same idea to the lookup path.
- [`../scenario-08-stream-table-joins/`](../scenario-08-stream-table-joins/) —
  the original `FOR SYSTEM_TIME AS OF` against Postgres / Paimon. This
  scenario is "the Fluss-native version" of that one.
- [`../scenario-09-multiway-join-recovery/`](../scenario-09-multiway-join-recovery/) —
  multi-source join with crash recovery; useful contrast for thinking
  about state size and checkpoint cost.
- Apache Fluss site: <https://fluss.apache.org>
- Flink 2.2 release post (delta join):
  <https://flink.apache.org/2025/12/04/apache-flink-2.2.0-advancing-real-time-data--ai-and-empowering-stream-processing-for-the-ai-era/>
