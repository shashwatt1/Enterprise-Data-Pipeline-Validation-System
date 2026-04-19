"""
Streamlit Dashboard for Enterprise Data Pipeline
Premium Analytics Command Center with Glassmorphic Design and Plotly Dark Themes.

Run with: streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os

# --- Page Config ---
st.set_page_config(
    page_title="Enterprise Pipeline Command Center",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Database Connection ---
DB_URL = os.getenv("DATABASE_URL", "sqlite:///./pipeline.db")
engine = create_engine(DB_URL, connect_args={"check_same_thread": False})
Session = sessionmaker(bind=engine)


def run_query(query: str) -> pd.DataFrame:
    """Execute a SQL query and return results as a DataFrame."""
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


# --- Premium Custom CSS ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    /* Main App Background (Dark Mode enforcement) */
    .stApp {
        background-color: #0f172a;
        color: #f8fafc;
    }
    
    /* Sidebar Styling */
    [data-testid="stSidebar"] {
        background-color: #1e293b !important;
        border-right: 1px solid rgba(255,255,255,0.05);
    }

    hr { border-top-color: rgba(255,255,255,0.1) !important; }

    /* Glassmorphism Metric Cards */
    .metric-card {
        background: rgba(30, 41, 59, 0.6);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        padding: 1.75rem;
        border-radius: 16px;
        color: #f8fafc;
        text-align: center;
        margin: 0.5rem 0 1.5rem 0;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2), 0 2px 4px -1px rgba(0, 0, 0, 0.1);
        transition: transform 0.25s ease, box-shadow 0.25s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.3), 0 4px 6px -2px rgba(0, 0, 0, 0.2);
        border: 1px solid rgba(255, 255, 255, 0.15);
    }
    
    .metric-card h2 { 
        margin: 0; 
        font-size: 2.8rem; 
        font-weight: 800;
        background: linear-gradient(135deg, #38bdf8 0%, #818cf8 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        line-height: 1.2;
    }
    .metric-card p { 
        margin: 0.5rem 0 0 0; 
        font-size: 0.95rem; 
        font-weight: 600;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 1.5px;
    }

    /* Badges */
    .pass-badge {
        background: rgba(16, 185, 129, 0.15);
        color: #34d399;
        border: 1px solid rgba(16, 185, 129, 0.3);
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.85rem;
        display: inline-block;
        margin-bottom: 4px;
    }
    .fail-badge {
        background: rgba(239, 68, 68, 0.15);
        color: #f87171;
        border: 1px solid rgba(239, 68, 68, 0.3);
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.85rem;
        display: inline-block;
        margin-bottom: 4px;
    }

    /* Styled Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 16px;
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 12px 24px;
        border-radius: 8px 8px 0 0;
        font-weight: 600;
        color: #94a3b8;
    }
    .stTabs [aria-selected="true"] {
        background-color: rgba(56, 189, 248, 0.08) !important;
        border-bottom-color: #38bdf8 !important;
        color: #f8fafc !important;
    }
    
    /* Headers & Text overrides */
    h1, h2, h3 {
        font-weight: 800 !important;
        letter-spacing: -0.5px;
        color: #f8fafc;
    }
    .stMarkdown p { color: #cbd5e1; }
    
    /* Info/Warning blocks */
    div[data-testid="stWarning"] {
        background-color: rgba(245, 158, 11, 0.1);
        border: 1px solid rgba(245, 158, 11, 0.3);
        color: #fbbf24;
    }
    div[data-testid="stInfo"] {
        background-color: rgba(56, 189, 248, 0.1);
        border: 1px solid rgba(56, 189, 248, 0.3);
        color: #7dd3fc;
    }
</style>
""", unsafe_allow_html=True)


# --- Sidebar ---
st.sidebar.markdown("<h2 style='font-size:2rem;text-align:center;'>⚡ Pipeline</h2>", unsafe_allow_html=True)
st.sidebar.markdown("---")
st.sidebar.markdown("**Command Center**  \nMonitor data ingestion, schema health, and business analytics.")
st.sidebar.markdown("---")
st.sidebar.caption("SYSTEM STATUS")
st.sidebar.success("🟢 API Services Online")
st.sidebar.success("🟢 Database Connected")
st.sidebar.info(f"💾 {DB_URL.split(':///')[0].upper()} Backend")


# --- Main Title ---
st.title("Data Pipeline Command Center")
st.markdown("Ensure robust data quality, monitor validation workflows, and gain instant executive analytics from a single unified portal.")
st.markdown("<br>", unsafe_allow_html=True)


# --- Main Tabs (Analytics promoted to Tab 1) ---
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Analytics Hub",
    "✅ Validation Matrix",
    "📋 Execution Timeline",
    "🗃️ Raw Data Explorer",
])

# ═══════════════════════════════════════════
# TAB 1: Analytics Hub
# ═══════════════════════════════════════════
with tab1:
    try:
        txn_check = run_query("SELECT COUNT(*) as cnt FROM transactions")
        has_txn = txn_check.iloc[0]["cnt"] > 0
        
        if not has_txn:
            st.info("No transaction data available. Run the pipeline pipeline to populate analytics.")
        else:
            # --- Executive KPIs ---
            kpi_data = run_query("""
                SELECT 
                    (SELECT COUNT(*) FROM customers) as total_customers,
                    (SELECT COUNT(*) FROM transactions) as total_txns,
                    (SELECT SUM(amount) FROM transactions) as total_rev
            """).iloc[0]

            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(f'<div class="metric-card"><p>Total Processed Revenue</p><h2>${kpi_data["total_rev"]:,.2f}</h2></div>', unsafe_allow_html=True)
            with col2:
                st.markdown(f'<div class="metric-card"><p>Validated Customers</p><h2>{kpi_data["total_customers"]:,}</h2></div>', unsafe_allow_html=True)
            with col3:
                st.markdown(f'<div class="metric-card"><p>Cleaned Transactions</p><h2>{kpi_data["total_txns"]:,}</h2></div>', unsafe_allow_html=True)
            
            # --- Dual Charts ---
            st.markdown("<br>", unsafe_allow_html=True)
            col_chart1, col_chart2 = st.columns([1.8, 1.2])

            with col_chart1:
                st.subheader("📅 Revenue vs Volume Trajectory")
                monthly_df = run_query("""
                    SELECT strftime('%Y-%m', transaction_date) as month,
                           COUNT(*) as txn_count,
                           SUM(amount) as total_amount
                    FROM transactions
                    WHERE transaction_date IS NOT NULL
                    GROUP BY month
                    ORDER BY month
                """)
                if not monthly_df.empty:
                    fig_monthly = go.Figure()
                    fig_monthly.add_trace(go.Bar(
                        x=monthly_df["month"], y=monthly_df["total_amount"],
                        name="Revenue ($)", marker_color="rgba(129, 140, 248, 0.7)",
                        marker_line_color="#818cf8", marker_line_width=1.5
                    ))
                    fig_monthly.add_trace(go.Scatter(
                        x=monthly_df["month"], y=monthly_df["txn_count"],
                        name="Volume", yaxis="y2",
                        line=dict(color="#34d399", width=3, shape="spline"), mode='lines+markers',
                        marker=dict(size=8, color="#059669", line=dict(color="#34d399", width=2))
                    ))
                    fig_monthly.update_layout(
                        template="plotly_dark",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        yaxis=dict(title="Revenue ($)", showgrid=True, gridcolor="rgba(255,255,255,0.05)"),
                        yaxis2=dict(title="Volume", overlaying="y", side="right", showgrid=False),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        margin=dict(l=0, r=0, t=50, b=0),
                        hovermode="x unified"
                    )
                    st.plotly_chart(fig_monthly, use_container_width=True)

            with col_chart2:
                st.subheader("🏷️ Category Distribution")
                cat_df = run_query("""
                    SELECT category, SUM(amount) as total_amount
                    FROM transactions
                    WHERE category IS NOT NULL
                    GROUP BY category
                """)
                if not cat_df.empty:
                    fig_cat = px.pie(
                        cat_df, values="total_amount", names="category",
                        hole=0.7, template="plotly_dark",
                        color_discrete_sequence=['#38bdf8', '#818cf8', '#c084fc', '#f472b6', '#fb7185', '#34d399']
                    )
                    total_rev = cat_df["total_amount"].sum()
                    fig_cat.add_annotation(
                        text=f"<b>${total_rev/1000:,.1f}k</b><br>Total", 
                        x=0.5, y=0.5, font_size=20, showarrow=False, font_color="#f8fafc"
                    )
                    fig_cat.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        margin=dict(l=0, r=0, t=20, b=0),
                        showlegend=True,
                        legend=dict(orientation="h", x=0.1, y=-0.1)
                    )
                    st.plotly_chart(fig_cat, use_container_width=True)

            # --- Bottom Row ---
            st.markdown("<br>", unsafe_allow_html=True)
            col_b1, col_b2 = st.columns([1, 1.2])
            
            with col_b1:
                st.subheader("🗺️ Regional Density")
                state_df = run_query("SELECT state, COUNT(*) as cc FROM customers WHERE state IS NOT NULL GROUP BY state ORDER BY cc ASC")
                if not state_df.empty:
                    fig_state = px.bar(
                        state_df, x="cc", y="state", orientation="h",
                        template="plotly_dark", color="cc", color_continuous_scale="Tealgrn"
                    )
                    fig_state.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", 
                        coloraxis_showscale=False,
                        xaxis_gridcolor="rgba(255,255,255,0.05)",
                        xaxis_title="Registered Customers", yaxis_title=""
                    )
                    st.plotly_chart(fig_state, use_container_width=True)

            with col_b2:
                st.subheader("💎 Premium Clients Setup")
                spend_df = run_query("""
                    SELECT c.name as "Customer", 
                           SUM(t.amount) as "Total Value", 
                           COUNT(t.id) as "Txns",
                           c.state as "Region"
                    FROM customers c JOIN transactions t ON c.customer_id = t.customer_id
                    GROUP BY c.customer_id ORDER BY "Total Value" DESC LIMIT 8
                """)
                if not spend_df.empty:
                    # Format DataFrame natively for streamlit output
                    st.dataframe(
                        spend_df.style.format({"Total Value": "${:,.2f}"}),
                        use_container_width=True, height=350
                    )

    except Exception as e:
        st.error(f"Analytics error: {e}")


# ═══════════════════════════════════════════
# TAB 2: Validation Results
# ═══════════════════════════════════════════
with tab2:
    try:
        val_df = run_query("SELECT * FROM validation_reports ORDER BY pipeline_run_id, id")
        if not val_df.empty:
            run_ids = val_df["pipeline_run_id"].unique()
            selected_run = st.selectbox("🎯 Select Pipeline Run ID to Inspect", run_ids, key="val_run")
            
            run_data = val_df[val_df["pipeline_run_id"] == selected_run]

            # Mini KPIs
            total = len(run_data)
            passed = len(run_data[run_data["status"] == "PASS"])
            failed = len(run_data[run_data["status"] == "FAIL"])
            
            st.markdown("<br>", unsafe_allow_html=True)
            vcol1, vcol2 = st.columns([1.2, 2])
            
            with vcol1:
                st.markdown("### Check Summary")
                fig_val = px.pie(
                    names=["Passed", "Failed"],
                    values=[passed, failed],
                    hole=0.6,
                    color=["Passed", "Failed"],
                    color_discrete_map={"Passed": "#10b981", "Failed": "#ef4444"},
                    template="plotly_dark"
                )
                fig_val.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    margin=dict(t=10, b=10, l=10, r=10),
                    showlegend=False
                )
                fig_val.add_annotation(text=f"{int((passed/total)*100)}%<br>Pass Rate", x=0.5, y=0.5, font_size=24, showarrow=False)
                st.plotly_chart(fig_val, use_container_width=True)
                
            with vcol2:
                st.markdown("### Rule Execution Logs")
                st.markdown('<div style="background: rgba(30,41,59,0.5); padding: 1.5rem; border-radius: 12px; border: 1px solid rgba(255,255,255,0.05); height: 350px; overflow-y: auto;">', unsafe_allow_html=True)
                for _, row in run_data.iterrows():
                    badge = "pass-badge" if row["status"] == "PASS" else "fail-badge"
                    st.markdown(
                        f"""
                        <div style="margin-bottom: 12px; border-bottom: 1px solid rgba(255,255,255,0.05); padding-bottom: 8px;">
                            <span class="{badge}">{row["status"]}</span> 
                            <span style="color:#f8fafc; font-weight:600; font-size:1.05rem; margin-left:8px;">{row["check_name"]}</span>
                            <br>
                            <span style="color:#94a3b8; font-size:0.9rem; margin-left:2px;">{row["details"]} <i>(Affected: {row["affected_rows"]})</i></span>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
                st.markdown('</div>', unsafe_allow_html=True)
                
        else:
            st.info("No validation reports available yet.")
    except Exception as e:
        st.warning(f"Could not load validation reports: {e}")


# ═══════════════════════════════════════════
# TAB 3: Pipeline Status
# ═══════════════════════════════════════════
with tab3:
    try:
        logs_df = run_query("SELECT * FROM pipeline_logs ORDER BY pipeline_run_id, timestamp")
        if not logs_df.empty:
            run_ids = logs_df["pipeline_run_id"].unique()
            selected_run = st.selectbox("🎯 Trace Execution Graph for Run ID", run_ids, key="log_run")
            
            run_logs = logs_df[logs_df["pipeline_run_id"] == selected_run]
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Using Streamlit Status elements natively
            st.markdown("### Action Trace Details")
            for _, log in run_logs.iterrows():
                if log["status"] == "FAILED":
                    st.error(f"**{log['step']}** FAILED at {log['timestamp']}")
                    st.code(log["message"])
                elif log["status"] == "COMPLETED":
                    with st.expander(f"✅ {log['step']} — COMPLETED", expanded=False):
                        scol1, scol2 = st.columns(2)
                        scol1.metric("Processed", f"{log['records_processed']}")
                        scol2.metric("Failed/Skipped", f"{log['records_failed']}")
                        if log["message"]:
                            st.caption("System Messages:")
                            st.code(log["message"], language="text")
                else:
                    st.info(f"🔵 {log['step']} — {log['status']} @ {log['timestamp']}")
        else:
            st.info("No pipeline logs available yet.")
    except Exception as e:
        st.warning(f"Could not load pipeline logs: {e}")


# ═══════════════════════════════════════════
# TAB 4: Raw Data Explorer
# ═══════════════════════════════════════════
with tab4:
    col_d1, col_d2 = st.columns(2)

    with col_d1:
        st.markdown("### 👥 Processed Customers Table")
        try:
            customers_df = run_query("SELECT * FROM customers ORDER BY customer_id")
            if not customers_df.empty:
                st.dataframe(customers_df, use_container_width=True, height=500)
            else:
                st.info("No validated customer data available.")
        except Exception as e:
            st.warning(f"Could not load customers: {e}")

    with col_d2:
        st.markdown("### 💳 Normalized Transactions")
        try:
            txn_df = run_query("SELECT * FROM transactions ORDER BY transaction_id")
            if not txn_df.empty:
                # Format money column nicely if it exists
                if "amount" in txn_df.columns:
                    styled_df = txn_df.style.format({"amount": "${:,.2f}"})
                    st.dataframe(styled_df, use_container_width=True, height=500)
                else:
                    st.dataframe(txn_df, use_container_width=True, height=500)
            else:
                st.info("No validated transaction data available.")
        except Exception as e:
            st.warning(f"Could not load transactions: {e}")

# --- Footer ---
st.markdown("<br><br><br>", unsafe_allow_html=True)
st.markdown("""
<div style="text-align:center; color:#64748b; font-size:0.9rem;">
    Enterprise Code Architecture ⚡ Reactivity powered by FastAPI, Pandas & Streamlit
</div>
""", unsafe_allow_html=True)
