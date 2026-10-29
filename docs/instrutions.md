# Cold-Chain FDE Project: Local Setup (Docker Desktop)

Everything runs on your machine. No cloud account is needed except, optionally, an LLM API key.

| Piece | Where it runs |
|---|---|
| Postgres 16 + pgvector (fleet data, SOP vectors, audit log) | `db` container |
| Streamlit UI + LangGraph agent | `app` container |
| Live telemetry stream (Kafka, KRaft single node) | `kafka`, `telemetry-producer`, `telemetry-consumer` containers |
| LLM (default `qwen2.5:7b`) | Ollama on the host, or Groq's free API |
| Weather | Open-Meteo public API (free, no key) |

## Phase 0: Prerequisites

1. Install and start **Docker Desktop**.
2. Install **Ollama** (https://ollama.com), then run `ollama pull qwen2.5:7b`.
   Or, with no install: get a free key at https://console.groq.com/keys, then in `.env` set
   `Agent_llm=GROQ` and `GROQ_API_KEY=...`. (`DEEPSEEK` / `OPENAI` also work with their keys.)
3. Install the Python packages on the host (used by the ingest scripts):
   `pip install -r requirements.txt`
4. Create `.env` from the template and set your own passwords:
   `copy .env.example .env`

## Phase 1: Start the database

```
docker compose up -d db
```

On the **first** start, `db/init/01_init.sh` runs automatically and creates:

- `tbl_sc_fleet_hist_raw`: the "legacy" raw table (cryptic column names)
- `fde_views.vw_fleet_history`: the clean view, the only fleet object the agent can read
- `sop_chunks`: SOP text + embeddings (replaces Pinecone)
- `agent_audit_log`: append-only trail of every agent step
- Logins: `fde_agent` (read-only view + SOP) and `fde_auditor` (insert-only audit log)
- `db/init/02_ingestor.sh` adds `fde_ingestor` (insert-only raw table, used by the Kafka consumer)

> The init script runs only when the data volume is empty. To start over from scratch:
> `docker compose down -v` (this deletes all DB data).

## Phase 2: Load the data

```
python scripts\ingest_legacy_data.py
python scripts\ingest_sop_pgvector.py
```

The first SOP run downloads the `bge-m3` embedding model (~2 GB, once).
Re-running the SOP script skips files whose content hasn't changed.

## Phase 3: Check the security model

```
docker compose exec db psql -U fde_agent -d fde
```

```sql
-- TEST 1: works (clean view)
SELECT * FROM fde_views.vw_fleet_history LIMIT 5;

-- TEST 2: fails with "permission denied" (raw legacy table)
SELECT * FROM tbl_sc_fleet_hist_raw LIMIT 5;

-- TEST 3: fails (the agent's session is read-only)
DELETE FROM sop_chunks;
```

Optional GUI: the VS Code "PostgreSQL" extension, host `127.0.0.1`, port = `PG_PORT` from `.env`, database `fde`.

## Phase 4: Test the agent in the terminal

```
python src\agent_tools.py      # runs each tool once
python src\orchestrator.py     # chat loop: type exit to quit
```

- Question 1 (the "domino effect" test: should use all three tools)
```
Find any active shipments near Los Angeles (Latitude ~33.8, Longitude ~-118.1). Check the local weather there, and tell me if the current cargo temperature violates the SOP for fresh perishables.
```
- Question 2 (the "restraint" test: should not query telemetry)
```
I'm a new dispatcher on the night shift. Can you quickly explain the difference between a Tier 1 and Tier 2 escalation?
```

## Phase 5: Run the app

```
docker compose up -d --build
```

Open http://localhost:8501. The "Fleet Dashboard" page shows historic KPIs from the same read-only view the agent uses. The "Security & Audit Logs" page needs the `POSTGRES_USER` / `POSTGRES_PASSWORD` from `.env`.

## Phase 6: Simulate live telemetry with Kafka

`docker compose up -d --build` (Phase 5) also starts the stream:

```
telemetry-producer  ->  Kafka topic "fleet-telemetry"  ->  telemetry-consumer  ->  tbl_sc_fleet_hist_raw
```

- The producer replays the dataset as a fake IoT gateway: one reading per second
  (`TELEMETRY_RATE` in `.env`), stamped with the current UTC time, looping forever.
- The consumer writes each reading into the raw table as `fde_ingestor` and marks it `sys_ingest_flag = 'K'`.
- The agent needs no change: it reads the same view, which now keeps growing.

If your database volume existed before this step, create the ingestor login once:

```
docker compose exec db bash /docker-entrypoint-initdb.d/02_ingestor.sh
```

Watch it work:

```
docker compose logs -f telemetry-consumer
docker compose exec db psql -U postgres -d fde -c "SELECT count(*), max(ts_utc) FROM tbl_sc_fleet_hist_raw WHERE sys_ingest_flag = 'K';"
```

Try asking the agent: *"What are the 5 most recent readings and are any of them above 8 °C?"*

To run the producer or consumer on the host instead: `python scripts\telemetry_producer.py` (uses `KAFKA_BOOTSTRAP=127.0.0.1:29092`).

Without Docker for the app (for quick code changes): `streamlit run src\ui.py`
