# When to use Fluss vs Paimon vs Spark, side-by-side

| Query shape | Best surface | Why |
|---|---|---|
| Point lookup by PK on current state | Fluss native (SQL `WHERE pk = ?` or Java SDK `Table.lookup`) | O(log N) on the index; no scan |
| Streaming enrichment of a fast log against a PK dim | Fluss lookup join (Flink SQL) | Zero in-Flink state on the right side |
| Live "right now" aggregate | Fluss batch mode | Reads both hot + tiered |
| Heavy historical aggregate spanning weeks | Spark on Paimon | Cheap columnar scan, projection pushdown |
| Audit query: "what did the trade look like yesterday?" | Spark on Paimon with time-travel | Paimon snapshots / `VERSION AS OF` |
| Cross-engine: business analyst with Trino | Spark/Trino on Paimon | Open table format — any engine works |
| Ad-hoc SQL with sub-second response on the most recent slice | Fluss batch mode | Hot segments serve directly without S3 hops |

## Latency / freshness vs cost

```
                                   freshness (lower = fresher)
                                            │
                          Fluss native       │
                            (sub-second)     │
                                ▲            │
                                │            │
                  Fluss read    │            │
                  with tiered   │            │
                    table       │            │
                  (1 min lag)   │            │
                                │            │
                                │   Paimon directly via Spark
                                │   (1 min lag, but cheap large scans)
                                │            ▼
                                └──────────────────►  cost (lower = cheaper for large scans)
```

The headline trade: Spark-on-Paimon is the cheapest way to scan a year of
data; Fluss is the freshest way to query the same logical table at all.
Pick by your latency and cost SLAs.

## What the workshop demonstrates

- **Scenario 16 step 05** (`SparkPaimonBatchJob`) — Spark reading Paimon
  directly. Used for monthly volume reports, regulatory exports, year-end
  reconciliation.
- **Scenario 16 step 06** (`SparkFlussLiveJob`) — Spark reading the live
  Fluss table via the Fluss-Spark connector. Used for "current snapshot
  analytics" where the 1-minute Paimon lag is unacceptable.
- **Scenario 14** — Flink SQL lookup join from a Kafka trade stream into
  Fluss `instrument_master`. The streaming-enrichment shape.
- **Scenario 15** — Java SDK clients that bypass Flink entirely.

## A note on Fluss's Spark connector maturity

As of Fluss 0.9.x the Spark connector is functional but actively
evolving. Read paths are stable; the write path is more limited than the
Flink connector. For batch analytics this is fine — the typical Spark
workload here is read-only.
