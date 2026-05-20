#!/usr/bin/env bash
# Seed the Postgres `accounts` dim table for scenarios 08 and 09.
# Idempotent — uses INSERT ... ON CONFLICT DO NOTHING.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

JAR="$PROJECT_DIR/common/target/flink-workshop-common-account-seed-jar-with-dependencies.jar"
ENTRY="com.workshop.flink.common.setup.AccountSeedJob"

if [[ ! -f "$JAR" ]]; then
    echo "Account seeder JAR not found: $JAR"
    echo "Run: mvn -pl common -am package -DskipTests"
    exit 1
fi

KAFKA_INTERNAL="${KAFKA_INTERNAL:-workshop-kafka:9093}"
PG_URL_INTERNAL="${PG_URL_INTERNAL:-jdbc:postgresql://workshop-postgres:5432/workshop}"
PG_USER="${PG_USER:-workshop}"
PG_PASSWORD="${PG_PASSWORD:-workshop}"
ACCOUNT_COUNT="${ACCOUNT_COUNT:-50}"

# 1. Make sure the tables exist. init-postgres.sql only runs on a fresh volume,
#    so re-create-if-needed inside the existing container.
podman exec workshop-postgres psql -U "$PG_USER" -d workshop <<'SQL'
CREATE TABLE IF NOT EXISTS accounts (
    account_id    VARCHAR(20)  PRIMARY KEY,
    account_name  VARCHAR(64)  NOT NULL,
    tier          VARCHAR(16)  NOT NULL,
    region        VARCHAR(16)  NOT NULL,
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fills_orphan_log (
    event_id    VARCHAR(36)  PRIMARY KEY,
    ticker      VARCHAR(10)  NOT NULL,
    fill_time   TIMESTAMPTZ  NOT NULL,
    reason      VARCHAR(64)  NOT NULL,
    logged_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
SQL

echo "Seeding $ACCOUNT_COUNT accounts into $PG_URL_INTERNAL ..."

podman cp "$JAR" "workshop-jobmanager:/tmp/account-seed.jar"

podman exec \
    -e KAFKA_BOOTSTRAP="$KAFKA_INTERNAL" \
    -e PG_URL="$PG_URL_INTERNAL" \
    -e PG_USER="$PG_USER" \
    -e PG_PASSWORD="$PG_PASSWORD" \
    -e ACCOUNT_COUNT="$ACCOUNT_COUNT" \
    workshop-jobmanager \
    flink run -c "$ENTRY" /tmp/account-seed.jar

echo "Done."
