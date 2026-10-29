import os
import sys
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from confluent_kafka import Producer

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

from src.db import LEGACY_MAPPING

TOPIC = "fleet-telemetry"
data_path = project_root / "data" / "raw" / "dynamic_supply_chain_logistics_dataset.csv"

# Simulated IoT gateway: replays the historic dataset, one reading at a time, stamped "now".
df = pd.read_csv(data_path, usecols=list(LEGACY_MAPPING.keys()))
rate = float(os.getenv("TELEMETRY_RATE", "1"))
producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:29092")})

print(f"📡 Streaming {len(df)} readings to '{TOPIC}' at {rate}/s (loops forever, Ctrl+C to stop)...")
sent = 0
try:
    while True:
        for reading in df.to_dict(orient="records"):
            reading["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            producer.produce(TOPIC, json.dumps(reading).encode())
            producer.poll(0)  # serve delivery callbacks so the internal queue does not fill up
            sent += 1
            if sent % 50 == 0:
                print(f"   sent {sent} readings")
            time.sleep(1 / rate)
except KeyboardInterrupt:
    pass
finally:
    producer.flush(10)
    print(f"Stopped after {sent} readings.")
