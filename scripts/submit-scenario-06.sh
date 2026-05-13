#!/usr/bin/env bash
# Scenario 06 — submit the Kafka -> Paimon streaming ingest job.

set -euo pipefail
cd "$(dirname "$0")/.."

JAR="scenario-06-paimon-lakehouse/target/scenario-06-ingest-jar-with-dependencies.jar"
if [[ ! -f "$JAR" ]]; then
  echo "Ingest jar not found at $JAR"
  echo "Build with: mvn -pl common,scenario-06-paimon-lakehouse -am package"
  exit 1
fi

echo "Copying ingest jar into ingest JobManager ..."
podman cp "$JAR" workshop-jobmanager:/tmp/ingest.jar

echo "Submitting PaimonIngestJob (detached) ..."
podman exec workshop-jobmanager \
  /opt/flink/bin/flink run -d \
    /tmp/ingest.jar \
    --kafka workshop-kafka:9093

echo ""
echo "Submitted. Flink UI: http://localhost:18081"
echo "Query the data:      http://localhost:3000  or  ./scripts/query-rest.sh"
