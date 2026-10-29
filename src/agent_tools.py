import os
import sys
import requests
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text

from langchain_core.tools import tool
from langchain_openai import OpenAIEmbeddings

# ==========================================
# 1. ENVIRONMENT & EMBEDDINGS
# ==========================================
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root))

load_dotenv(dotenv_path=project_root / ".env")

from src.db import get_engine


def get_cached_huggingface_embeddings(model_name: str):
    """
    Loads and locks the HuggingFace model weights into the machine's global RAM.
    If called again during any subsequent script rerun, it returns instantly.
    """
    import streamlit as st
    
    # We wrap the inner call with st.cache_resource dynamically 
    @st.cache_resource(show_spinner=False)
    def _load_model(name: str):
        print(f"🧠 MEMORY SEED: Permanently caching local model [{name}] in global RAM...")
        from langchain_huggingface import HuggingFaceEmbeddings
        return HuggingFaceEmbeddings(
            model_name=name,
            model_kwargs={'device': 'cpu'}
        )
    return _load_model(model_name)

EMBEDDINGS_MODEL_SETTING = os.getenv("Embeddings_model", "LOCAL").strip().upper()

# The agent connects as fde_agent: it can SELECT the fleet view and SOP chunks, nothing else.
# This is the real security boundary; the SELECT check in the tool below is only a first filter.
engine = get_engine("fde_agent", os.getenv("AGENT_DB_PASSWORD"))

# Must be the same embedding model used by scripts/ingest_sop_pgvector.py
if EMBEDDINGS_MODEL_SETTING == "OPENAI":
    print("🤖 Mode: OpenAI Embeddings (1536 Dim Space)...")
    embeddings = OpenAIEmbeddings()
else :
    local_model_target = os.getenv("Local_Embedding_Model", "BAAI/bge-m3").strip()

    print(f"🤗 Mode: Local Embeddings [{local_model_target}] (1024 Dim Space)...")

    try:
        import streamlit as st
        if st.runtime.exists():
            embeddings = get_cached_huggingface_embeddings(local_model_target)
        else:
            from langchain_huggingface import HuggingFaceEmbeddings
            embeddings = HuggingFaceEmbeddings(model_name=local_model_target, model_kwargs={'device': 'cpu'})
    except ImportError:
        from langchain_huggingface import HuggingFaceEmbeddings
        embeddings = HuggingFaceEmbeddings(model_name=local_model_target, model_kwargs={'device': 'cpu'})

# ==========================================
# 2. CORE FDE AGENT TOOLS
# ==========================================

@tool
def query_telemetry_db(sql_query: str) -> str:
    """
    Executes a SQL SELECT query against the fde_views.vw_fleet_history view.
    Columns available:
    event_time, latitude, longitude, current_temperature_c, cargo_condition_score,
    risk_classification, delay_probability, port_congestion_level, route_risk_index.
    Always write PostgreSQL queries (use LIMIT, not TOP).
    """
    try:
        if not sql_query.strip().upper().startswith("SELECT"):
            return "SECURITY BLOCK: Only SELECT operations are authorized on this view."
            
        with engine.connect() as conn:
            cursor = conn.execute(text(sql_query))
            columns = list(cursor.keys())
            rows = cursor.fetchmany(10)
            
            if not rows:
                return "No records matched the query criteria."
                
            formatted_output = f"COLUMNS: {', '.join(columns)}\n"
            for row in rows:
                formatted_output += str(tuple(row)) + "\n"
                
            return formatted_output
    except Exception as e:
        return f"Database Error: {str(e)}"

@tool
def fetch_corridor_conditions(latitude: float, longitude: float) -> str:
    """
    Fetches real-time weather and corridor conditions from a live REST API for given GPS coordinates.
    Provides temperature, wind speed, and computed corridor congestion index.
    """
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
        response = requests.get(url, timeout=6)
        response.raise_for_status()
        
        payload = response.json().get("current_weather", {})
        temp = payload.get("temperature", "N/A")
        wind = payload.get("windspeed", 0.0)
        
        congestion_index = 8.5 if wind > 10.0 else 2.5
        status_note = "High Transit Disruption" if wind > 10.0 else "Corridor Normal"
        
        return (
            f"--- LIVE CORRIDOR TELEMETRY ---\n"
            f"Target GPS: {latitude}, {longitude}\n"
            f"External Temp: {temp}°C | Wind Speed: {wind} km/h\n"
            f"Corridor Risk: {status_note} (Congestion Index: {congestion_index}/10)\n"
            f"-------------------------------"
        )
    except Exception as e:
        return f"Corridor API Communication Failure: {str(e)}"

@tool
def search_compliance_sop(query: str) -> str:
    """
    Searches enterprise Standard Operating Procedures (SOPs) stored in the pgvector database.
    Use this to retrieve regulatory thresholds, cold-chain breach mitigations, and rerouting rules.
    """
    try:
        query_vector = embeddings.embed_query(query)
        with engine.connect() as conn:
            # "<=>" is pgvector's cosine distance: smallest = most similar. Top 2 chunks, like before.
            matched = conn.execute(text("""
                SELECT source_file, metadata->>'file_format', content
                FROM sop_chunks
                ORDER BY embedding <=> CAST(:q AS VECTOR)
                LIMIT 2
            """), {"q": str(query_vector)}).fetchall()

        if not matched:
            return "No matching compliance clauses found."

        formatted_context = "\n\n".join(
            [f"[Source: {source} | Format: {fmt}]\n{content}" for source, fmt, content in matched]
        )
        return f"--- COMPLIANCE SOP CONTEXT ---\n{formatted_context}\n------------------------------"
    except Exception as e:
        return f"Vector Store Retrieval Error: {str(e)}"

# ==========================================
# 3. LOCAL VERIFICATION
# ==========================================
if __name__ == "__main__":
    print("\n--- Testing Tool 1: SQL Telemetry View ---")
    print(query_telemetry_db.invoke("SELECT latitude, longitude, current_temperature_c FROM fde_views.vw_fleet_history LIMIT 2"))
    
    print("\n--- Testing Tool 2: Live Corridor API ---")
    print(fetch_corridor_conditions.invoke({"latitude": 33.77, "longitude": -118.19}))
    
    print("\n--- Testing Tool 3: pgvector SOP Retrieval ---")
    print(search_compliance_sop.invoke("What are the temperature rules for fresh perishables?"))
