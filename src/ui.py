import os
import sys
import uuid
import json
from pathlib import Path
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
from sqlalchemy import text
from langchain_core.messages import HumanMessage, ToolMessage

# ==========================================
# 1. IMMEDIATE PATH & ENVIRONMENT RESOLUTION
# ==========================================
script_dir = Path(__file__).resolve().parent  # points to src/
project_root = script_dir.parent              # climbs to project root

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

load_dotenv(project_root / ".env")

# Import the compiled graph and tools list dynamically
from src.orchestrator import fde_agent
from src.db import get_engine
from src.agent_tools import engine as agent_engine  # read-only fde_agent login, reused for the dashboard

# ==========================================
# 2. AUDIT LOG CONNECTION
# ==========================================
# Separate login from the agent: fde_auditor can ONLY insert audit rows (it cannot read or change them)
log_engine = get_engine("fde_auditor", os.getenv("AUDIT_DB_PASSWORD"))

def write_audit_log(session_id, node_name, tool_name, content):
    """Silently writes agent execution traces to the audit table using the insert-only login."""
    try:
        with log_engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO agent_audit_log (session_id, node_executed, tool_name, content)
                VALUES (:session_id, :node_name, :tool_name, :content)
            """), {
                "session_id": session_id,
                "node_name": node_name,
                "tool_name": tool_name,
                "content": content
            })
            conn.commit()
    except Exception as e:
        print(f"Audit Log Failed (Silent): {e}")

# ==========================================
# 3. PAGE CONFIGURATION & ENTERPRISE THEME
# ==========================================
st.set_page_config(
    page_title="FDE Supply Chain Dispatch Console",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .stApp { background-color: #0B0E14; color: #E2E8F0; }
    div[data-testid="stSidebar"] { background-color: #111622; border-right: 1px solid #1E293B; }
    .stMarkdown code { background-color: #1E293B !important; color: #38BDF8 !important; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 4. MULTI-USER STATE & THREAD MANAGEMENT
# ==========================================
if "thread_id" not in st.session_state:
    st.session_state.thread_id = str(uuid.uuid4())

if "ui_messages" not in st.session_state:
    st.session_state.ui_messages = []

thread_config = {"configurable": {"thread_id": st.session_state.thread_id}}

# ==========================================
# 5. SIDEBAR NAVIGATION & METADATA
# ==========================================
with st.sidebar:
    st.image(str(script_dir / "image_L25X5q.png") if (script_dir / "image_L25X5q.png").exists() else "https://cdn-icons-png.flaticon.com/512/2830/2830305.png", width=65)
    st.title("FDE Command Center")
    
    app_mode = st.radio("System Mode", ["🧊 Dispatch Console", "📡 Live Telemetry", "📊 Historic Data", "🛡️ Security & Audit Logs"])
    
    st.markdown("---")
    st.caption(f"Session Token: `{st.session_state.thread_id[:8]}...`")
    st.markdown(f"**Reasoning Architecture:** `{os.getenv('Agent_llm', 'DEEPSEEK')}`")
    
    st.markdown("---")
    if st.button("🗑️ Purge Dispatch Workspace Session", use_container_width=True):
        st.session_state.ui_messages = []
        st.session_state.thread_id = str(uuid.uuid4())
        st.rerun()

# ==========================================
# 6. VIEW ROUTING (DISPATCH VS AUDIT)
# ==========================================

if app_mode == "🧊 Dispatch Console":
    # ------------------------------------------
    # TAB 1: CHAT UI & AGENT EXECUTION
    # ------------------------------------------
    st.title("Cold-Chain Incident Control Dashboard")
    st.caption("Production Data Engineering Pipeline • Real-Time Decision Optimization Platform")

    for entry in st.session_state.ui_messages:
        with st.chat_message(entry["role"], avatar="👤" if entry["role"] == "user" else "🤖"):
            if "traces" in entry:
                for trace in entry["traces"]:
                    if trace["type"] == "tool_input":
                        st.markdown(f"**⚡ Intent Recognized:** `{trace['name']}`")
                        with st.expander(f"📥 View Generated Input ({trace['name']})", expanded=False):
                            st.json(trace["args"])
                    elif trace["type"] == "tool_output":
                        with st.expander(f"📤 View Raw Output ({trace['name']})", expanded=False):
                            st.code(trace["content"], language="text")
            st.markdown(entry["content"])

    if user_input := st.chat_input("Query fleet telemetry, corridor updates, or compliance thresholds..."):
        
        st.session_state.ui_messages.append({"role": "user", "content": user_input})
        with st.chat_message("user", avatar="👤"):
            st.markdown(user_input)

        with st.chat_message("assistant", avatar="🤖"):
            final_response = ""
            current_traces = [] 
            
            with st.status("🧠 Initializing Core Reasoner Node...", expanded=True) as status:
                events = fde_agent.stream(
                    {"messages": [HumanMessage(content=user_input)]}, 
                    config=thread_config,
                    stream_mode="updates"
                )
                
                for event in events:
                    for node_name, node_state in event.items():
                        
                        if node_name == "reasoner":
                            latest_msg = node_state["messages"][-1]
                            
                            # A. Intercept Tool Call Requests (Inputs)
                            if hasattr(latest_msg, "tool_calls") and latest_msg.tool_calls:
                                status.update(label="🧠 Agent generated tool parameters...")
                                for tool_call in latest_msg.tool_calls:
                                    st.markdown(f"**⚡ Intent Recognized:** `{tool_call['name']}`")
                                    with st.expander(f"📥 View Generated Input ({tool_call['name']})", expanded=False):
                                        st.json(tool_call['args'])
                                    
                                    current_traces.append({
                                        "type": "tool_input",
                                        "name": tool_call['name'],
                                        "args": tool_call['args']
                                    })
                                    
                                    write_audit_log(
                                        session_id=st.session_state.thread_id,
                                        node_name="reasoner",
                                        tool_name=tool_call['name'],
                                        content=json.dumps(tool_call['args'])
                                    )
                            
                            # B. Intercept Final Generation
                            if latest_msg.content:
                                final_response = latest_msg.content
                                status.update(label="📝 Generating Operational Resolution Report...")
                                
                                write_audit_log(
                                    session_id=st.session_state.thread_id,
                                    node_name="reasoner_final",
                                    tool_name="LLM Text Synthesis",
                                    content=final_response
                                )
                                
                        elif node_name == "tools":
                            status.update(label="🔧 Executing Enterprise Subsystem Tools...")
                            for msg in node_state.get("messages", []):
                                if isinstance(msg, ToolMessage):
                                    with st.expander(f"📤 View Raw Output ({msg.name})", expanded=False):
                                        st.code(msg.content, language="text")
                                        
                                    current_traces.append({
                                        "type": "tool_output",
                                        "name": msg.name,
                                        "content": msg.content
                                    })
                                    
                                    write_audit_log(
                                        session_id=st.session_state.thread_id,
                                        node_name="tools",
                                        tool_name=msg.name,
                                        content=msg.content
                                    )
                
                status.update(label="Incident Matrix Evaluation Complete", state="complete", expanded=False)
                
            if final_response:
                st.markdown(final_response)
                st.session_state.ui_messages.append({
                    "role": "assistant",
                    "content": final_response,
                    "traces": current_traces
                })
            else:
                error_fallback = "⚠️ Execution Timeout: System engine encountered an unresolved processing edge case."
                st.error(error_fallback)


elif app_mode == "📡 Live Telemetry":
    # ------------------------------------------
    # TAB 2: LIVE KAFKA STREAM (readings stamped in the last few minutes)
    # ------------------------------------------
    st.title("📡 Live Fleet Telemetry")
    st.caption("Kafka topic `fleet-telemetry` → Postgres → this page, refreshed every 2 seconds")

    window_min = st.select_slider("Time window (minutes)", options=[5, 15, 30, 60], value=15)
    limit_c = st.number_input("Temperature alert above (°C)", value=8.0, step=0.5)

    # Only this fragment re-runs on the timer, so the rest of the page (and the chat) is untouched
    @st.fragment(run_every="2s")
    def live_panel():
        with agent_engine.connect() as conn:
            # event_time is stored as UTC without a time zone
            live = pd.read_sql(text("""
                SELECT event_time, latitude, longitude, current_temperature_c,
                       cargo_condition_score, risk_classification, delay_probability
                FROM fde_views.vw_fleet_history
                WHERE event_time >= (now() AT TIME ZONE 'UTC') - make_interval(mins => :mins)
                ORDER BY event_time DESC
            """), conn, params={"mins": window_min})

        if live.empty:
            st.info("No live readings in this window. Start the stream: `docker compose up -d telemetry-producer telemetry-consumer`")
            return

        breaches = live[live["current_temperature_c"] > limit_c]
        last_age = (pd.Timestamp.utcnow().tz_localize(None) - live["event_time"].iloc[0]).total_seconds()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Readings in window", f"{len(live):,}")
        c2.metric("Latest temp", f"{live['current_temperature_c'].iloc[0]:.1f} °C")
        c3.metric(f"Above {limit_c:g} °C", f"{len(breaches):,}")
        c4.metric("Last reading", f"{last_age:.0f} s ago")

        left, right = st.columns(2)
        with left:
            st.subheader("Cargo temperature (°C)")
            st.line_chart(live, x="event_time", y="current_temperature_c")
        with right:
            st.subheader("Latest positions")
            st.map(live.head(100), latitude="latitude", longitude="longitude", size=20000)

        st.subheader("Latest readings")
        st.dataframe(
            live.head(25).style.apply(
                lambda r: ["background-color: #7F1D1D" if r["current_temperature_c"] > limit_c else ""] * len(r), axis=1
            ),
            hide_index=True, use_container_width=True,
        )

    live_panel()


elif app_mode == "📊 Historic Data":
    # ------------------------------------------
    # TAB 3: HISTORIC FLEET KPIs + DATA BROWSER (read-only view, same one the agent queries)
    # ------------------------------------------
    st.title("📊 Historic Fleet Data")

    # Aggregate inside Postgres so only a few rows travel to the app; cache for 60s
    # so switching tabs does not re-run the queries.
    @st.cache_data(ttl=60)
    def load_dashboard():
        with agent_engine.connect() as conn:
            kpi = pd.read_sql("""
                SELECT COUNT(*)                                      AS readings,
                       AVG(current_temperature_c)                    AS avg_temp,
                       AVG(delay_probability)                        AS avg_delay,
                       AVG((risk_classification = 'High Risk')::int) AS high_risk_share
                FROM fde_views.vw_fleet_history
            """, conn).iloc[0]
            risk = pd.read_sql("""
                SELECT risk_classification, COUNT(*) AS readings
                FROM fde_views.vw_fleet_history
                GROUP BY 1
            """, conn)
            monthly = pd.read_sql("""
                SELECT date_trunc('month', event_time)        AS month,
                       AVG(current_temperature_c)             AS avg_temp_c,
                       AVG(delay_probability)                 AS avg_delay_probability
                FROM fde_views.vw_fleet_history
                GROUP BY 1 ORDER BY 1
            """, conn)
        return kpi, risk, monthly

    try:
        kpi, risk, monthly = load_dashboard()
    except Exception as e:
        st.error(f"Dashboard query failed (is the database up and loaded?): {e}")
        st.stop()

    if kpi["readings"] == 0:
        st.info("No telemetry yet. Run scripts/ingest_legacy_data.py first.")
        st.stop()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Readings", f"{int(kpi['readings']):,}")
    c2.metric("Avg cargo temp", f"{kpi['avg_temp']:.1f} °C")
    c3.metric("Avg delay probability", f"{kpi['avg_delay']:.0%}")
    c4.metric("High-risk readings", f"{kpi['high_risk_share']:.0%}")

    left, right = st.columns(2)
    with left:
        st.subheader("Readings by risk class")
        st.bar_chart(risk, x="risk_classification", y="readings")
    with right:
        st.subheader("Monthly average cargo temperature (°C)")
        st.line_chart(monthly, x="month", y="avg_temp_c")

    st.subheader("Monthly average delay probability")
    st.line_chart(monthly, x="month", y="avg_delay_probability")

    st.markdown("---")
    st.subheader("🔎 Browse readings")
    f1, f2, f3 = st.columns([2, 2, 1])
    with f1:
        date_range = st.date_input("Date range", value=(pd.Timestamp("2024-08-01").date(), pd.Timestamp("2024-08-29").date()))
    with f2:
        risk_pick = st.multiselect("Risk class", risk["risk_classification"].tolist(), default=risk["risk_classification"].tolist())
    with f3:
        max_rows = st.selectbox("Max rows", [500, 2000, 5000], index=1)

    # The date picker returns one date while the user is still choosing the second one
    if len(date_range) == 2 and risk_pick:
        with agent_engine.connect() as conn:
            rows = pd.read_sql(text("""
                SELECT event_time, latitude, longitude, current_temperature_c, cargo_condition_score,
                       risk_classification, delay_probability, port_congestion_level, route_risk_index
                FROM fde_views.vw_fleet_history
                WHERE event_time >= :start AND event_time < CAST(:end AS DATE) + 1
                  AND risk_classification = ANY(:risks)
                ORDER BY event_time DESC
                LIMIT :n
            """), conn, params={"start": date_range[0], "end": date_range[1], "risks": risk_pick, "n": max_rows})
        st.caption(f"{len(rows):,} rows (newest first, capped at {max_rows:,}). Use the table's download icon to export CSV.")
        st.dataframe(rows, hide_index=True, use_container_width=True, height=450)


elif app_mode == "🛡️ Security & Audit Logs":
    # ------------------------------------------
    # TAB 3: AUDIT LOG VIEWER (REQUIRES ADMIN CREDENTIALS FROM .ENV OR INPUT)
    # ------------------------------------------
    st.title("🛡️ Enterprise Agent Audit Trail")
    st.caption("Secure database inspection of agent_audit_log")

    st.markdown("### Database Authorization Gate")
    st.markdown("Enter the Postgres admin credentials (defined in `.env` as `POSTGRES_USER` / `POSTGRES_PASSWORD`) to query audit logs.")

    with st.form("admin_auth_form"):
        col1, col2 = st.columns(2)
        with col1:
            input_user = st.text_input("Admin Username", value=os.getenv("POSTGRES_USER", ""))
        with col2:
            input_pass = st.text_input("Admin Password", type="password", value="")
            
        submit_admin = st.form_submit_button("Authenticate & Load Logs", use_container_width=True)

    if submit_admin:
        expected_admin_user = os.getenv("POSTGRES_USER")
        expected_admin_pass = os.getenv("POSTGRES_PASSWORD")

        if input_user == expected_admin_user and input_pass == expected_admin_pass:
            try:
                # Isolated admin connection, used only for viewing the audit trail
                admin_engine = get_engine(input_user, input_pass)

                with admin_engine.connect() as conn:
                    query = """
                        SELECT log_id, logged_at, session_id, node_executed, tool_name, content
                        FROM agent_audit_log
                        ORDER BY logged_at DESC
                    """
                    df = pd.read_sql(query, conn)
                
                st.success("✅ Authenticated successfully as Admin.")
                
                if not df.empty:
                    st.dataframe(
                        df,
                        column_config={
                            "log_id": st.column_config.NumberColumn("ID", format="%d"),
                            "logged_at": st.column_config.DatetimeColumn("Execution Time", format="DD/MM/YYYY-h:mm a"),
                            "session_id": "Session Token",
                            "node_executed": "Graph Node",
                            "tool_name": "Tool Triggered",
                            "content": "Raw Payload Data"
                        },
                        hide_index=True,
                        use_container_width=True,
                        height=600
                    )
                else:
                    st.info("No audit logs found in the database. Run a query in the Dispatch Console first.")
                    
            except Exception as e:
                st.error(f"Database Query Failed: {e}")
        else:
            st.error("❌ Invalid Administrator Credentials.")