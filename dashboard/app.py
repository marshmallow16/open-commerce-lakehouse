"""
ONDC Lakehouse Dashboard — Streamlit
---------------------------------------
Unified view of the entire pipeline:
  1. Pipeline status (record counts per layer)
  2. Data preview at each Medallion layer
  3. Gold layer analytics charts
  4. NL Query interface (Claude → SQL → ClickHouse)
"""

import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

BRONZE_PATH = os.getenv("BRONZE_PATH", "./data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "./data/silver")
GOLD_PATH   = os.getenv("GOLD_PATH", "./data/gold")

st.set_page_config(page_title="ONDC Lakehouse", layout="wide")
st.title("ONDC Open-Commerce Data Lakehouse")
st.caption("Medallion Architecture: Bronze > Silver > Gold | Streaming + OLAP + NL Query")

# ─────────────────────────────────────────
# Refresh controls
# ─────────────────────────────────────────
REFRESH_OPTIONS = {"Manual": 0, "Every 5 minutes": 300, "Every 10 minutes": 600}
ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 1, 2])
with ctrl_col1:
    refresh_choice = st.selectbox("Auto-refresh", list(REFRESH_OPTIONS.keys()), index=0)
with ctrl_col2:
    if st.button("Refresh now"):
        st.rerun()
with ctrl_col3:
    st.markdown(f"**Last refreshed:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

refresh_secs = REFRESH_OPTIONS[refresh_choice]
if refresh_secs > 0:
    st.markdown(
        f'<meta http-equiv="refresh" content="{refresh_secs}">',
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────
# Helper: count parquet records
# ─────────────────────────────────────────
def count_records(path: str) -> int:
    try:
        con = duckdb.connect()
        result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}/**/*.parquet', hive_partitioning=true, union_by_name=true)").fetchone()[0]
        con.close()
        return result
    except Exception:
        try:
            con = duckdb.connect()
            result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}/*.parquet')").fetchone()[0]
            con.close()
            return result
        except Exception:
            return 0

def load_gold_table(name: str) -> pd.DataFrame:
    try:
        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM read_parquet('{GOLD_PATH}/{name}.parquet')").fetchdf()
        con.close()
        return df
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────
# Section 1: Pipeline Status
# ─────────────────────────────────────────
st.header("1. Pipeline Status")
col1, col2, col3 = st.columns(3)

bronze_count = count_records(BRONZE_PATH)
silver_count = count_records(SILVER_PATH)
gold_count = count_records(GOLD_PATH)

col1.metric("Bronze (Raw)", f"{bronze_count:,} records", help="Raw events from Kafka, as-is Parquet")
col2.metric("Silver (Clean)", f"{silver_count:,} records", delta=f"-{bronze_count - silver_count} dupes removed" if bronze_count > silver_count else None, help="Deduplicated, type-cast, null-handled")
col3.metric("Gold (Analytics)", f"{gold_count:,} records", help="Star schema: facts + dimensions + aggregations")

# ─────────────────────────────────────────
# Section 2: Data Preview
# ─────────────────────────────────────────
st.header("2. Data Preview")
tab_bronze, tab_silver, tab_gold = st.tabs(["Bronze (Raw)", "Silver (Clean)", "Gold (Analytics)"])

with tab_bronze:
    try:
        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM read_parquet('{BRONZE_PATH}/**/*.parquet', hive_partitioning=true) LIMIT 10").fetchdf()
        con.close()
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.warning(f"No Bronze data yet. Run the producer + consumer first. ({e})")

with tab_silver:
    try:
        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM read_parquet('{SILVER_PATH}/orders.parquet') LIMIT 10").fetchdf()
        con.close()
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.warning(f"No Silver data yet. Run the Silver transform first. ({e})")

with tab_gold:
    fact_df = load_gold_table("fact_orders")
    if not fact_df.empty:
        st.dataframe(fact_df.head(10), use_container_width=True)
    else:
        st.warning("No Gold data yet. Run the Gold aggregation first.")

# ─────────────────────────────────────────
# Section 3: Analytics Charts (Gold layer)
# ─────────────────────────────────────────
st.header("3. Analytics (Gold Layer)")

fact_df = load_gold_table("fact_orders")
agg_df  = load_gold_table("agg_seller_daily")

if not fact_df.empty:
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.subheader("Orders by Category")
        cat_counts = fact_df["category"].value_counts().reset_index()
        cat_counts.columns = ["Category", "Orders"]
        st.bar_chart(cat_counts, x="Category", y="Orders")

    with chart_col2:
        st.subheader("Order Status Distribution")
        status_counts = fact_df["status"].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]
        st.bar_chart(status_counts, x="Status", y="Count")

    chart_col3, chart_col4 = st.columns(2)

    with chart_col3:
        st.subheader("GMV by Category")
        gmv_by_cat = fact_df.groupby("category")["total_amount"].sum().reset_index()
        gmv_by_cat.columns = ["Category", "GMV"]
        gmv_by_cat = gmv_by_cat.sort_values("GMV", ascending=False)
        st.bar_chart(gmv_by_cat, x="Category", y="GMV")

    with chart_col4:
        st.subheader("Top 10 Sellers by GMV")
        if not agg_df.empty:
            top_sellers = agg_df.groupby("seller_id")["total_gmv"].sum().nlargest(10).reset_index()
            top_sellers.columns = ["Seller", "GMV"]
            st.bar_chart(top_sellers, x="Seller", y="GMV")
else:
    st.info("Run the full pipeline to see analytics charts.")

# ─────────────────────────────────────────
# Section 4: NL Query
# ─────────────────────────────────────────
st.header("4. Ask a Question (Natural Language → SQL)")
st.caption("Powered by Claude API — converts your question to SQL and runs it on Gold layer")

question = st.text_input("Ask about the ONDC data:", placeholder="e.g., Which category has the highest GMV?")

if st.button("Run Query") and question:
    try:
        from nl_query.engine import ask
        with st.spinner("Claude is generating SQL..."):
            result = ask(question)

        st.code(result["sql"], language="sql")

        if result["error"]:
            st.error(f"Query error: {result['error']}")
        elif result["result"] is not None:
            st.dataframe(result["result"], use_container_width=True)
    except ValueError as e:
        st.error(str(e))
    except Exception as e:
        st.error(f"Error: {e}")

# ─────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────
with st.sidebar:
    st.header("Architecture")
    st.markdown("""
    ```
    Kafka (Streaming)
        |
    Bronze (Raw Parquet)
        |
    Silver (Clean + Dedup)
        |
    Gold (Star Schema)
        |
    ┌───┴───┐
    ClickHouse  DuckDB
    (OLAP)    (NL Query)
    ```
    """)
    st.divider()
    st.markdown("**Tech Stack:** Kafka, DuckDB, ClickHouse, PyArrow, Claude API, Streamlit")
    st.markdown("**Pattern:** Medallion Architecture (Bronze/Silver/Gold)")
    st.markdown("**Format:** Parquet + Snappy compression")
