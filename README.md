# Flink Workshop: Reliability, Failover and Deduplication

A hands-on workshop exploring how Apache Flink handles failures, replay, and duplicate data across five progressively more complex scenarios. Each scenario is a self-contained pair of streaming jobs wired together through Kafka, with PostgreSQL as an external sink in Scenario 3.

---

## Prerequisites

| Tool | Purpose |
|---|---|
| Java 17 | Build the JARs |
| Maven 3.9+ | Build tool |
| Podman + `podman compose` | Run the local infrastructure |
| `curl`, `jq` | Used by the quickstart script |

---

## Quick Start

```bash
# Build and run a single scenario (infra starts automatically)
./quickstart.sh 1          # datagen + scenario 01
./quickstart.sh all        # datagen + all 5 scenarios
./quickstart.sh status     # show running jobs
./quickstart.sh stop       # tear everything down
```

The script:
1. Builds the project if JARs are missing
2. Starts Zookeeper, Kafka, Kafka UI, PostgreSQL, Flink JobManager + TaskManager via `podman compose`
3. Creates the three Kafka topics (`topic.in`, `topic.mid`, `topic.out`)
4. Copies each JAR into the Flink container and submits it with the correct env vars

**UIs once running:**
- Flink Web UI: http://localhost:8081
- Kafka UI: http://localhost:8080

---

## Architecture

Every scenario shares the same topology skeleton:

```
FinancialDatagenJob  →  topic.in  →  App 1  →  topic.mid  →  App 2  →  topic.out
                                                                   ↓
                                                          (Scenario 3 only)
                                                          PostgreSQL / external sink
```

App 1 is the producer-side job. App 2 is the consumer/aggregator-side job. The interesting failure and deduplication behaviour happens at the boundary between them.

---

## Scenarios

### Scenario 1 — Baseline AT_LEAST_ONCE: replay duplicates are expected

![Scenario 1](workshop1.png)

App 1 runs with `AT_LEAST_ONCE` semantics and no transactional Kafka sink. When it crashes and restarts from a checkpoint, records that were already written to `topic.mid` but not yet checkpointed are replayed — producing duplicate `eventId`s downstream. App 2 counts them with a tumbling window, so the inflated counts are visible in `topic.out`.

**What this demonstrates:**
- Recovery always replays data from the last checkpoint
- Without transactions or dedup, duplicates are the expected outcome
- This is the clearest baseline failure story

**Run:** `./quickstart.sh 1`  
**Verify:** `bash scripts/verify-01.sh`

---

### Scenario 2 — EXACTLY_ONCE Kafka: no duplicates at the Kafka boundary

![Scenario 2](workshop4.png)

App 1 uses Flink's Kafka transactional sink. On crash and restart, uncommitted transaction output is invisible to `topic.mid` consumers. App 2 reads with `isolation.level=read_committed`, so it only ever sees committed records — no duplicates reach `topic.out`.

**What this demonstrates:**
- Exactly-once is about checkpoint-aligned transactional visibility
- Internal replay can happen without duplicate committed output
- Correctness is proven at the Kafka boundaries

**Run:** `./quickstart.sh 2`  
**Verify:** `bash scripts/verify-02.sh`

---

### Scenario 3 — External sink duplicates: Kafka exactly-once does not cover your database

![Scenario 3](workshop5.png)

App 1 is exactly-once at the Kafka layer (same as Scenario 2). App 2 reads from `topic.mid` exactly-once and writes to PostgreSQL. The database is outside the Flink checkpoint protocol, so a crash between a successful DB write and a successful checkpoint causes the write to be replayed on restart — a duplicate side effect. The scenario ships both a buggy (`INSERT`) and a fixed (`ON CONFLICT DO UPDATE` upsert) sink, toggled by `SINK_MODE`.

**What this demonstrates:**
- Exactly-once in Kafka is not exactly-once in external systems
- Non-idempotent side effects can happen twice after replay
- Use idempotency, upserts, dedup, or outbox patterns for external sinks

**Run:** `./quickstart.sh 3`  
**Verify:** `bash scripts/verify-03.sh`

---

### Scenario 4 — Upstream true duplicates: transport guarantees do not collapse business duplicates

![Scenario 4](workshop2.png)

The upstream producer intentionally sends the same business event twice (same `eventId`, different Kafka messages). Flink's exactly-once transport delivers both faithfully — it prevents *recovery* duplicates but has no knowledge of *business* duplicates. App 2 is available in two variants:

- **Naive** (`App2PipelineNaive`) — counts both copies, inflating results
- **With dedup** (`App2PipelineWithDedup`) — keys by `eventId` with a TTL `ValueState`; the second copy is dropped

**What this demonstrates:**
- Exactly-once prevents duplicates from recovery mechanics
- It does not identify duplicate business events automatically
- Semantic dedup needs a business key such as `eventId`

**Run:** `./quickstart.sh 4`  
**Verify:** `bash scripts/verify-04.sh`

---

### Scenario 5 — Rescaling and savepoint replay: operational actions carry the same risk as failures

![Scenario 5](workshop3.png)

App 1 is a stateful enrichment job keyed by `accountId`. App 2 uses event-time tumbling windows with a 5-second watermark tolerance. Restoring from a savepoint taken at an earlier offset causes Kafka offset replay, injecting records that downstream may have already processed — the same duplication risk as a crash, but triggered deliberately by an operator action.

**What this demonstrates:**
- Maintenance actions (rescale, restore) trigger replay just like failures
- External side effects still need idempotent design
- Dedup state retention matters: older duplicates may no longer be recognized

**Run:** `./quickstart.sh 5`  
**Verify:** `bash scripts/verify-05.sh`

---

## Infrastructure

| Service | Image | Port |
|---|---|---|
| Zookeeper | `bitnami/zookeeper:3.9` | 2181 |
| Kafka | `bitnami/kafka:3.7` | 9092 (host), 9093 (internal) |
| Kafka UI | `provectuslabs/kafka-ui:latest` | 8080 |
| PostgreSQL | `postgres:16` | 5432 |
| Flink JobManager | `apache/flink:1.20.4-java17` | 8081 |
| Flink TaskManager | `apache/flink:1.20.4-java17` | — |

Kafka topics: `topic.in` · `topic.mid` · `topic.out` (4 partitions each)

PostgreSQL credentials: `workshop / workshop / workshop`

Checkpoints and savepoints are stored in named Podman volumes (`flink-checkpoints`, `flink-savepoints`) so they survive container restarts.

---

## Project Structure

```
flink-examples/
├── quickstart.sh                        # one-command local runner
├── podman-compose.yml                   # full local stack
├── common/                              # shared utilities + FinancialDatagenJob
├── scenario-01-baseline-at-least-once/
├── scenario-02-exactly-once-kafka/
├── scenario-03-external-sink-duplicates/
├── scenario-04-upstream-true-duplicates/
├── scenario-05-rescaling-replay/
└── scripts/
    ├── create-topics.sh
    ├── verify-01.sh … verify-05.sh
    └── init-postgres.sql
```

---

## Building Manually

```bash
mvn clean package -DskipTests
```

The shade plugin produces a `*-jar-with-dependencies.jar` per app inside each module's `target/` directory. The quickstart script skips the build if those JARs already exist.
