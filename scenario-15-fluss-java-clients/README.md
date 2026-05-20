# Scenario 15 — Fluss Java Clients (SDK + HTTP + Flink DataStream)

> Fluss is a queryable streaming store. You do not need Flink to read it.
> This scenario demonstrates both surfaces: the **pure Fluss Java SDK**
> (no Flink at all) and the **Flink DataStream API + Fluss connector**
> (the Flink-native shape).

---

## What you'll learn

| Concept | Where you see it |
|---|---|
| Open a Fluss connection from Java | `FlussPointInTimeLookupClient` |
| Point-in-time PK lookup via the Java SDK | `FlussPointInTimeLookupClient.lookupAccounts(...)` |
| Wrap Fluss as a REST microservice — no sidecar materialiser | `FlussLookupHttpService` |
| Use the Fluss connector inside a Flink DataStream lookup join | `FlussFlinkConnectorEnrichmentJob` |

---

## Prerequisites

- Fluss running: `podman-compose up -d --build`
- Tables created + seeded:
  ```bash
  bash scripts/init-fluss.sh
  bash scripts/seed-fluss.sh
  ```

The seed leaves several hundred populated rows in `account_profile` and
`instrument_master` — enough for the lookups to hit.

---

## App 1 — `FlussPointInTimeLookupClient`

Pure Java SDK. **No Flink.** Run as a standalone JAR:

```bash
mvn -pl scenario-15-fluss-java-clients -am package -DskipTests

JAR=scenario-15-fluss-java-clients/target/scenario-15-point-in-time-lookup-jar-with-dependencies.jar

# From host (uses external port):
FLUSS_BOOTSTRAP=localhost:19123 java -jar "$JAR" 100

# From inside the workshop network (uses internal hostname):
podman cp "$JAR" workshop-jobmanager:/tmp/lookup.jar
podman exec workshop-jobmanager java -jar /tmp/lookup.jar 100
```

Outputs lookup latency + hit-rate stats. With ~1000 seeded accounts, you'll
see >90% hits and sub-millisecond lookup time per call.

---

## App 2 — `FlussLookupHttpService`

A microservice that exposes the same lookups as REST endpoints:

```
GET /accounts/{accountId}      → JSON
GET /instruments/{isin}        → JSON
GET /health                    → "OK"
```

Run it:

```bash
JAR=scenario-15-fluss-java-clients/target/scenario-15-http-service-jar-with-dependencies.jar
FLUSS_BOOTSTRAP=localhost:19123 java -jar "$JAR"

# Then in another shell:
curl http://localhost:18099/accounts/ACC-0042
curl http://localhost:18099/instruments/ISIN00000042
curl http://localhost:18099/health
```

This is the canonical "Fluss as the materialised state behind a
microservice" pattern. Compare with the Kafka equivalent: a Kafka consumer
maintaining an in-memory map, with all the failover, restart-from-earliest,
and operational complexity that brings.

---

## App 3 — `FlussFlinkConnectorEnrichmentJob`

Flink DataStream + Fluss connector. Reads `topic.in` (the workshop trade
stream) from Kafka, lookup-joins against Fluss `instrument_master`, sinks
to blackhole.

```bash
JAR=scenario-15-fluss-java-clients/target/scenario-15-flink-enrichment-jar-with-dependencies.jar
podman cp "$JAR" workshop-jobmanager:/tmp/scenario-15-flink.jar
podman exec workshop-jobmanager flink run -d \
    -c com.workshop.flink.scenario15.FlussFlinkConnectorEnrichmentJob \
    /tmp/scenario-15-flink.jar
```

Watch the job in the Flink UI: <http://localhost:18081>

The lookup join uses Flink's `FOR SYSTEM_TIME AS OF PROCTIME()` syntax
against the Fluss table. Right-side state is zero — every lookup goes to
Fluss directly.

---

## Verification

```bash
bash scripts/verify-15.sh
```

The script:
- Builds the three JARs.
- Runs `FlussPointInTimeLookupClient` for 10 lookups and asserts >0 hits.
- Probes `FlussLookupHttpService` for a known account and asserts the
  response is JSON with `tier` populated (if the service isn't running, it
  is skipped with a warning).
- Confirms the Flink enrichment job's class can be loaded (`flink run
  --help` smoke).

---

## When to use which surface

| You want… | Use… |
|---|---|
| A microservice answering point lookups | `FlussLookupHttpService` (SDK) |
| A batch job hydrating a cache | `FlussPointInTimeLookupClient` (SDK) |
| Enriching a streaming pipeline | `FlussFlinkConnectorEnrichmentJob` (Flink) |
| SQL-only enrichment | Scenario 14's SQL lookup-join |

---

## See also

- Scenario 11 — what Fluss is.
- Scenario 12 — wide schemas + projection pushdown (these apps deliberately
  project just a few columns for that reason).
- Scenario 14 — the SQL equivalent of `FlussFlinkConnectorEnrichmentJob`.
- [Fluss Java Client docs](https://fluss.apache.org/docs/apis/java-client/)
