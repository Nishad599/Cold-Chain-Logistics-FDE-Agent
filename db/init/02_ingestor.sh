#!/bin/bash
# Insert-only login for the Kafka consumer (scripts/telemetry_consumer.py).
# Runs automatically on the first DB start. On an existing DB, run it once by hand:
#   docker compose exec db bash /docker-entrypoint-initdb.d/02_ingestor.sh
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE fde_ingestor LOGIN PASSWORD '${INGEST_DB_PASSWORD}';
    GRANT INSERT ON tbl_sc_fleet_hist_raw TO fde_ingestor;
EOSQL
