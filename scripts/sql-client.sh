#!/usr/bin/env bash
# Scenario 06 — open an interactive Flink SQL Client connected to the
# workshop SQL Gateway. Catalog / database are auto-selected so you can
# go straight to SELECT * FROM trades.

set -euo pipefail

echo "Opening Flink SQL Client against http://workshop-sql-gateway:8083 ..."
echo "  Try:  USE CATALOG paimon; USE workshop; SHOW TABLES;"
echo "        SELECT COUNT(*) FROM trades;"
echo

podman exec -it workshop-sql-gateway \
  /opt/flink/bin/sql-client.sh gateway \
    --endpoint http://workshop-sql-gateway:8083
