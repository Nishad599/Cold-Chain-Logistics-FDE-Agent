#!/bin/bash
# ====================================================================
# FDE DATABASE BOOTSTRAP (runs once, on the first start of the db container)
# Replaces the old manual Phase 2 (security + view) and Phase 4 (audit log).
# A .sh file is used instead of .sql so passwords come from .env, not the repo.
# ====================================================================
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS vector;

    -- 1. The "legacy" raw table, with cryptic 2000s-style column names.
    --    Created here (not by pandas) so the view below can depend on it.
    CREATE TABLE tbl_sc_fleet_hist_raw (
        ts_utc          TIMESTAMP,
        v_lat           DOUBLE PRECISION,
        v_lon           DOUBLE PRECISION,
        iot_temp_val_c  DOUBLE PRECISION,
        cgo_cond_cd     DOUBLE PRECISION,
        risk_cls_txt    TEXT,
        delay_prob_dec  DOUBLE PRECISION,
        prt_cng_lvl     DOUBLE PRECISION,
        rt_rsk_idx      DOUBLE PRECISION,
        sys_ingest_flag CHAR(1)
    );

    -- 2. Semantic layer: the ONLY fleet object the agent can see.
    --    Lowercase names on purpose: Postgres folds unquoted names to lowercase,
    --    so the LLM's SQL works without double quotes.
    CREATE SCHEMA fde_views;
    CREATE VIEW fde_views.vw_fleet_history AS
    SELECT ts_utc         AS event_time,
           v_lat          AS latitude,
           v_lon          AS longitude,
           iot_temp_val_c AS current_temperature_c,
           cgo_cond_cd    AS cargo_condition_score,
           risk_cls_txt   AS risk_classification,
           delay_prob_dec AS delay_probability,
           prt_cng_lvl    AS port_congestion_level,
           rt_rsk_idx     AS route_risk_index
    FROM tbl_sc_fleet_hist_raw;

    -- 3. SOP chunks + embeddings for RAG (replaces Pinecone).
    --    "vector" without a size accepts both 1024 (bge-m3) and 1536 (OpenAI) dims.
    CREATE TABLE sop_chunks (
        id          TEXT PRIMARY KEY,
        source_file TEXT,
        content     TEXT,
        metadata    JSONB,
        embedding   VECTOR
    );

    -- 4. Append-only audit trail of every agent step.
    CREATE TABLE agent_audit_log (
        log_id        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        logged_at     TIMESTAMPTZ DEFAULT now(),
        session_id    VARCHAR(50),
        node_executed VARCHAR(50),
        tool_name     VARCHAR(100),
        content       TEXT
    );

    -- 5. Least-privilege logins.
    --    fde_agent  : read the view + SOP vectors, nothing else (no raw table, no writes)
    --    fde_auditor: can only INSERT audit rows (cannot read or edit them)
    CREATE ROLE fde_agent   LOGIN PASSWORD '${AGENT_DB_PASSWORD}';
    CREATE ROLE fde_auditor LOGIN PASSWORD '${AUDIT_DB_PASSWORD}';

    GRANT USAGE  ON SCHEMA fde_views              TO fde_agent;  -- needed before any grant inside the schema works
    GRANT SELECT ON fde_views.vw_fleet_history    TO fde_agent;  -- a view runs with its OWNER's rights, so no grant on the raw table is needed
    GRANT SELECT ON sop_chunks                    TO fde_agent;
    GRANT INSERT ON agent_audit_log               TO fde_auditor;

    -- Defense in depth: even if a write slipped past the grants, the session is read-only.
    ALTER ROLE fde_agent SET default_transaction_read_only = on;
EOSQL
