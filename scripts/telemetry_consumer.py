import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text
from confluent_kafka import Consumer

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

from src.db import get_engine, LEGACY_MAPPING

TOPIC = "fleet-telemetry"

# fde_ingestor can only INSERT into the raw table: a bad message cannot delete or read anything
engine = get_engine("fde_ingestor", os.getenv("INGEST_DB_PASSWORD"))
consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:29092"),
    "group.id": "telemetry-to-postgres",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # commit only after the row is in Postgres (at-least-once)
})
consumer.subscribe([TOPIC])

columns = list(LEGACY_MAPPING.values())
insert_sql = text(
    f"INSERT INTO tbl_sc_fleet_hist_raw ({', '.join(columns)}, sys_ingest_flag) "
    f"VALUES ({', '.join(':' + c for c in columns)}, 'K')"  # 'K' marks rows that came in live via Kafka
)

print(f"📥 Consuming '{TOPIC}' into tbl_sc_fleet_hist_raw (Ctrl+C to stop)...")
written = 0
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue
        try:
            reading = json.loads(msg.value())
            row = {legacy: reading[clean] for clean, legacy in LEGACY_MAPPING.items()}
        except (ValueError, KeyError, TypeError) as e:
            # Skip malformed messages instead of crashing the whole pipeline
            print(f"Skipping malformed message at offset {msg.offset()}: {e}")
            consumer.commit(msg)
            continue
        with engine.begin() as conn:
            conn.execute(insert_sql, row)
        consumer.commit(msg)
        written += 1
        if written % 50 == 0:
            print(f"   wrote {written} readings")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print(f"Stopped after {written} readings.")
