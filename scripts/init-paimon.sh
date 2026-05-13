#!/usr/bin/env bash
# Scenario 06 — bootstrap the Paimon catalog/database/table on MinIO.
#
# Idempotent: re-running is safe (uses CREATE ... IF NOT EXISTS). Run this
# once after `podman-compose up -d` to make the table visible to the SQL
# Gateway and Web UI even before the ingest job has produced any data.

set -euo pipefail
cd "$(dirname "$0")/.."

JAR="scenario-06-paimon-lakehouse/target/scenario-06-bootstrap-jar-with-dependencies.jar"
if [[ ! -f "$JAR" ]]; then
  echo "Bootstrap jar not found at $JAR"
  echo "Build with: mvn -pl common,scenario-06-paimon-lakehouse -am package"
  exit 1
fi

echo "Copying bootstrap jar into ingest JobManager ..."
podman cp "$JAR" workshop-jobmanager:/tmp/bootstrap.jar

echo "Running PaimonCatalogBootstrap on the ingest cluster ..."
podman exec workshop-jobmanager \
  /opt/flink/bin/flink run /tmp/bootstrap.jar

echo ""
echo "Paimon catalog bootstrap complete."
echo "Verify in MinIO console: http://localhost:19001 (workshop / workshopsecret)"
