"""
Streamlit Dashboard for Enterprise Data Pipeline
Provides visual insights into data, validation, pipeline status, and analytics.

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
    page_title="Enterprise Data Pipeline Dashboard",
    page_icon="🔄",
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


# --- Custom CSS ---
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 12px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .metric-card h2 { margin: 0; font-size: 2.2rem; }
    .metric-card p { margin: 0.3rem 0 0 0; font-size: 0.95rem; opacity: 0.85; }
    .pass-badge {
        background: #10B981; color: white; padding: 2px 10px;
        border-radius: 6px; font-weight: 600;
    }
    .fail-badge {
        background: #EF4444; color: white; padding: 2px 10px;
        border-radius: 6px; font-weight: 600;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 24px;
        border-radius: 8px 8px 0 0;
    }
</style>
""", unsafe_allow_html=True)


# --- Sidebar ---
st.sidebar.title("🔄 Data Pipeline")
st.sidebar.markdown("---")
st.sidebar.markdown("**Enterprise Data Pipeline**  \n& Validation System")
st.sidebar.markdown("---")

# --- Main Tabs ---
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Data Preview",
    "✅ Validation Results",
    "📋 Pipeline Status",
    "📈 Analytics",
])


# ═══════════════════════════════════════════
# TAB 1: Data Preview
# ═══════════════════════════════════════════
with tab1:
    st.header("Data Preview")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("👥 Customers")
        try:
            customers_df = run_query("SELECT * FROM customers ORDER BY customer_id")
            if not customers_df.empty:
                st.dataframe(customers_df, use_container_width=True, height=400)
                st.caption(f"Total: {len(customers_df)} customers")
            else:
                st.info("No customer data yet. Run the pipeline first.")
        except Exception as e:
            st.warning(f"Could not load customers: {e}")

    with col2:
        st.subheader("💳 Transactions")
        try:
            txn_df = run_query("SELECT * FROM transactions ORDER BY transaction_id")
            if not txn_df.empty:
                st.dataframe(txn_df, use_container_width=True, height=400)
                st.caption(f"Total: {len(txn_df)} transactions")
            else:
                st.info("No transaction data yet. Run the pipeline first.")
        except Exception as e:
            st.warning(f"Could not load transactions: {e}")


# ═══════════════════════════════════════════
# TAB 2: Validation Results
# ═══════════════════════════════════════════
with tab2:
    st.header("Validation Results")

    try:
        val_df = run_query(
            "SELECT * FROM validation_reports ORDER BY pipeline_run_id, id"
        )
        if not val_df.empty:
            run_ids = val_df["pipeline_run_id"].unique()
            selected_run = st.selectbox(
                "Select Pipeline Run", run_ids, key="val_run"
            )

            run_data = val_df[val_df["pipeline_run_id"] == selected_run]

            # Summary metrics
            total = len(run_data)
            passed = len(run_data[run_data["status"] == "PASS"])
            failed = len(run_data[run_data["status"] == "FAIL"])

            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Total Checks", total)
            mc2.metric("Passed ✅", passed)
            mc3.metric("Failed ❌", failed)

            # Pass/Fail chart
            fig = px.pie(
                run_data,
                names="status",
                color="status",
                color_discrete_map={"PASS": "#10B981", "FAIL": "#EF4444"},
                title="Validation Check Distribution",
                hole=0.4,
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, use_container_width=True)

            # Detailed table
            st.subheader("Check Details")
            for _, row in run_data.iterrows():
                badge = "pass-badge" if row["status"] == "PASS" else "fail-badge"
                st.markdown(
                    f'<span class="{badge}">{row["status"]}</span> '
                    f'**{row["check_name"]}** — {row["details"]} '
                    f'(affected rows: {row["affected_rows"]})',
                    unsafe_allow_html=True,
                )
        else:
            st.info("No validation reports available yet.")
    except Exception as e:
        st.warning(f"Could not load validation reports: {e}")


# ═══════════════════════════════════════════
# TAB 3: Pipeline Status
# ═══════════════════════════════════════════
with tab3:
    st.header("Pipeline Execution History")

    try:
        logs_df = run_query(
            "SELECT * FROM pipeline_logs ORDER BY pipeline_run_id, timestamp"
        )
        if not logs_df.empty:
            run_ids = logs_df["pipeline_run_id"].unique()
            selected_run = st.selectbox(
                "Select Pipeline Run", run_ids, key="log_run"
            )

            run_logs = logs_df[logs_df["pipeline_run_id"] == selected_run]

            # Timeline visualization
            st.subheader("Pipeline Steps")
            for _, log in run_logs.iterrows():
                status_icon = {
                    "STARTED": "🔵",
                    "COMPLETED": "🟢",
                    "FAILED": "🔴",
                }.get(log["status"], "⚪")

                with st.expander(
                    f"{status_icon} {log['step']} — {log['status']}",
                    expanded=(log["status"] == "FAILED"),
                ):
                    col_a, col_b, col_c = st.columns(3)
                    col_a.metric("Records Processed", log["records_processed"])
                    col_b.metric("Records Failed", log["records_failed"])
                    col_c.metric("Status", log["status"])
                    if log["message"]:
                        st.code(log["message"], language="text")
                    st.caption(f"Timestamp: {log['timestamp']}")
        else:
            st.info("No pipeline logs available yet.")
    except Exception as e:
        st.warning(f"Could not load pipeline logs: {e}")


# ═══════════════════════════════════════════
# TAB 4: Analytics
# ═══════════════════════════════════════════
with tab4:
    st.header("Business Analytics")

    try:
        # Check if data exists
        txn_check = run_query("SELECT COUNT(*) as cnt FROM transactions")
        if txn_check.iloc[0]["cnt"] == 0:
            st.info("No transaction data available for analytics. Run the pipeline first.")
        else:
            # --- Spend by Customer ---
            st.subheader("💰 Top Customers by Spend")
            spend_df = run_query("""
                SELECT c.customer_id, c.name,
                       SUM(t.amount) as total_spend,
                       COUNT(t.id) as txn_count
                FROM customers c
                JOIN transactions t ON c.customer_id = t.customer_id
                GROUP BY c.customer_id, c.name
                ORDER BY total_spend DESC
                LIMIT 15
            """)

            if not spend_df.empty:
                fig_spend = px.bar(
                    spend_df,
                    x="name",
                    y="total_spend",
                    color="txn_count",
                    color_continuous_scale="viridis",
                    labels={
                        "name": "Customer",
                        "total_spend": "Total Spend ($)",
                        "txn_count": "Transactions",
                    },
                )
                fig_spend.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_spend, use_container_width=True)

            # --- Monthly Trends ---
            st.subheader("📅 Monthly Transaction Trends")
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
                    x=monthly_df["month"],
                    y=monthly_df["total_amount"],
                    name="Total Amount",
                    marker_color="#667eea",
                ))
                fig_monthly.add_trace(go.Scatter(
                    x=monthly_df["month"],
                    y=monthly_df["txn_count"],
                    name="Transaction Count",
                    yaxis="y2",
                    line=dict(color="#f093fb", width=3),
                ))
                fig_monthly.update_layout(
                    yaxis=dict(title="Total Amount ($)"),
                    yaxis2=dict(
                        title="Transaction Count",
                        overlaying="y", side="right",
                    ),
                    xaxis_tickangle=-45,
                    legend=dict(x=0, y=1.1, orientation="h"),
                )
                st.plotly_chart(fig_monthly, use_container_width=True)

            # --- Category Breakdown ---
            col_cat1, col_cat2 = st.columns(2)

            with col_cat1:
                st.subheader("🏷️ Category Breakdown")
                cat_df = run_query("""
                    SELECT category,
                           COUNT(*) as txn_count,
                           SUM(amount) as total_amount
                    FROM transactions
                    WHERE category IS NOT NULL
                    GROUP BY category
                    ORDER BY total_amount DESC
                """)
                if not cat_df.empty:
                    fig_cat = px.pie(
                        cat_df,
                        values="total_amount",
                        names="category",
                        color_discrete_sequence=px.colors.qualitative.Set2,
                        hole=0.35,
                    )
                    st.plotly_chart(fig_cat, use_container_width=True)

            with col_cat2:
                st.subheader("📊 Status Distribution")
                status_df = run_query("""
                    SELECT status, COUNT(*) as cnt
                    FROM transactions
                    WHERE status IS NOT NULL
                    GROUP BY status
                """)
                if not status_df.empty:
                    fig_status = px.pie(
                        status_df,
                        values="cnt",
                        names="status",
                        color_discrete_sequence=px.colors.qualitative.Pastel,
                        hole=0.35,
                    )
                    st.plotly_chart(fig_status, use_container_width=True)

            # --- State Distribution ---
            st.subheader("🗺️ Customer Distribution by State")
            state_df = run_query("""
                SELECT state, COUNT(*) as customer_count
                FROM customers
                WHERE state IS NOT NULL
                GROUP BY state
                ORDER BY customer_count DESC
            """)
            if not state_df.empty:
                fig_state = px.bar(
                    state_df,
                    x="state",
                    y="customer_count",
                    color="customer_count",
                    color_continuous_scale="tealgrn",
                    labels={"state": "State", "customer_count": "Customers"},
                )
                st.plotly_chart(fig_state, use_container_width=True)

    except Exception as e:
        st.warning(f"Could not load analytics: {e}")


# --- Footer ---
st.sidebar.markdown("---")
st.sidebar.markdown(
    "Built with ❤️ using FastAPI, Pandas, SQLAlchemy & Streamlit"
)
