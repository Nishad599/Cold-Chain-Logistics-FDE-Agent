import os
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

from src.db import get_engine, LEGACY_MAPPING

data_path = project_root / "data" / "raw" / "dynamic_supply_chain_logistics_dataset.csv"

# 1. Load the raw dataset
print(f"Loading CSV from {data_path}...")
df = pd.read_csv(data_path)

# 2. Map clean columns to a messy 2000s legacy enterprise schema
df_legacy = df[list(LEGACY_MAPPING.keys())].rename(columns=LEGACY_MAPPING)
df_legacy['ts_utc'] = pd.to_datetime(df_legacy['ts_utc'])
df_legacy['sys_ingest_flag'] = 'Y'

# 3. Connect as the Postgres admin (only the admin may write to the raw table)
engine = get_engine(os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PASSWORD"))

# 4. Reload the table. TRUNCATE + append (not if_exists='replace'), because
#    'replace' would DROP the table, and the agent's view depends on it.
table_name = 'tbl_sc_fleet_hist_raw'
print(f"Ingesting {len(df_legacy)} rows into {table_name}...")
with engine.begin() as conn:
    conn.execute(text(f"TRUNCATE {table_name}"))
    df_legacy.to_sql(table_name, conn, if_exists='append', index=False)

print("✅ Legacy data ingestion complete!")
