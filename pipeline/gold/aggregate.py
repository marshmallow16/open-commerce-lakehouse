"""
ONDC Gold Layer — Aggregations & ClickHouse Load
---------------------------------------------------
Reads clean Silver Parquet data, builds:
  1. fact_orders   → every order event (loaded to ClickHouse)
  2. dim_seller    → unique sellers
  3. dim_product   → unique products
  4. agg_seller_daily → GMV per seller per day

Also writes Gold Parquet locally for DuckDB NL queries.
"""

import os
from pathlib import Path

import clickhouse_connect
import duckdb
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

SILVER_PATH = os.getenv("SILVER_PATH", "./data/silver")
GOLD_PATH   = os.getenv("GOLD_PATH", "./data/gold")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "admin")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "ondc1234")
CH_DB   = os.getenv("CLICKHOUSE_DB", "ondc")


def run():
    Path(GOLD_PATH).mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    # ── Load Silver data ──
    logger.info(f"Reading Silver data from {SILVER_PATH}/orders.parquet...")
    con.execute(f"""
        CREATE TABLE silver AS
        SELECT * FROM read_parquet('{SILVER_PATH}/orders.parquet')
    """)
    total = con.execute("SELECT COUNT(*) FROM silver").fetchone()[0]
    logger.info(f"Silver records loaded: {total}")

    # ── Build fact_orders ──
    logger.info("Building fact_orders...")
    con.execute("""
        CREATE TABLE fact_orders AS
        SELECT
            order_id,
            buyer_id,
            seller_id,
            product_id,
            category,
            quantity,
            unit_price,
            total_amount,
            status,
            event_ts,
            order_date,
            order_hour,
            is_cancelled,
            buyer_city,
            seller_city
        FROM silver
        WHERE dq_flag = 'GOOD'
    """)
    con.execute(f"COPY fact_orders TO '{GOLD_PATH}/fact_orders.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)")

    # ── Build dim_seller ──
    logger.info("Building dim_seller...")
    con.execute("""
        CREATE TABLE dim_seller AS
        SELECT DISTINCT
            seller_id,
            FIRST(seller_name) AS seller_name,
            FIRST(seller_city) AS city,
            FIRST(category)    AS category,
            MIN(event_ts)      AS registered_at
        FROM silver
        GROUP BY seller_id
    """)
    con.execute(f"COPY dim_seller TO '{GOLD_PATH}/dim_seller.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)")

    # ── Build dim_product ──
    logger.info("Building dim_product...")
    con.execute("""
        CREATE TABLE dim_product AS
        SELECT DISTINCT
            product_id,
            FIRST(product_name) AS product_name,
            FIRST(category)     AS category,
            FIRST(seller_id)    AS seller_id,
            ROUND(AVG(unit_price), 2) AS base_price
        FROM silver
        GROUP BY product_id
    """)
    con.execute(f"COPY dim_product TO '{GOLD_PATH}/dim_product.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)")

    # ── Build agg_seller_daily ──
    logger.info("Building agg_seller_daily...")
    con.execute("""
        CREATE TABLE agg_seller_daily AS
        SELECT
            order_date                                       AS date,
            seller_id,
            category,
            COUNT(*)                                         AS total_orders,
            ROUND(SUM(total_amount), 2)                      AS total_gmv,
            SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END)   AS cancelled_orders
        FROM silver
        WHERE dq_flag = 'GOOD'
        GROUP BY order_date, seller_id, category
    """)
    con.execute(f"COPY agg_seller_daily TO '{GOLD_PATH}/agg_seller_daily.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)")

    # ── Load into ClickHouse ──
    logger.info("Loading Gold tables into ClickHouse...")
    ch = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS, database=CH_DB)

    # Truncate existing data to avoid duplicates on re-runs
    for table in ["fact_orders", "dim_seller", "dim_product", "agg_seller_daily"]:
        ch.command(f"TRUNCATE TABLE {table}")

    # fact_orders
    fact_df = con.execute("SELECT order_id, buyer_id, seller_id, product_id, category, quantity, unit_price, total_amount, status, event_ts FROM fact_orders").fetchdf()
    fact_df["event_ts"] = fact_df["event_ts"].dt.tz_localize(None)
    ch.insert_df("fact_orders", fact_df)
    logger.info(f"  fact_orders: {len(fact_df)} rows loaded")

    # dim_seller
    seller_df = con.execute("SELECT seller_id, seller_name, city, category, registered_at FROM dim_seller").fetchdf()
    seller_df["registered_at"] = seller_df["registered_at"].dt.tz_localize(None)
    ch.insert_df("dim_seller", seller_df)
    logger.info(f"  dim_seller: {len(seller_df)} rows loaded")

    # dim_product
    product_df = con.execute("SELECT product_id, product_name, category, seller_id, base_price FROM dim_product").fetchdf()
    ch.insert_df("dim_product", product_df)
    logger.info(f"  dim_product: {len(product_df)} rows loaded")

    # agg_seller_daily
    agg_df = con.execute("SELECT date, seller_id, category, total_orders, total_gmv, cancelled_orders FROM agg_seller_daily").fetchdf()
    ch.insert_df("agg_seller_daily", agg_df)
    logger.info(f"  agg_seller_daily: {len(agg_df)} rows loaded")

    # ── Summary ──
    logger.info("--- Gold Layer Summary ---")
    gmv = con.execute("SELECT ROUND(SUM(total_gmv), 2) FROM agg_seller_daily").fetchone()[0]
    sellers = con.execute("SELECT COUNT(*) FROM dim_seller").fetchone()[0]
    products = con.execute("SELECT COUNT(*) FROM dim_product").fetchone()[0]
    logger.info(f"  Total GMV:  Rs.{gmv}")
    logger.info(f"  Sellers:    {sellers}")
    logger.info(f"  Products:   {products}")
    logger.success("Gold layer complete. Data loaded into ClickHouse.")

    ch.close()
    con.close()


if __name__ == "__main__":
    run()
