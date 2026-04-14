"""
ONDC Silver Layer — Clean & Deduplicate
-----------------------------------------
Reads raw Parquet from Bronze, applies:
  1. Deduplication on order_id
  2. Type casting (event_ts → proper timestamp)
  3. Null handling (drop rows with no order_id, fill defaults)
  4. Data quality flags

Writes clean Parquet to data/silver/, partitioned by date and category.

In production this would run on Spark; locally we use DuckDB (same SQL semantics).
"""

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

BRONZE_PATH = os.getenv("BRONZE_PATH", "./data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "./data/silver")


def run():
    Path(SILVER_PATH).mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    # ── Step 1: Read all Bronze Parquet files ──
    logger.info(f"Reading Bronze data from {BRONZE_PATH}...")
    con.execute(f"""
        CREATE TABLE bronze AS
        SELECT * FROM read_parquet('{BRONZE_PATH}/**/*.parquet', hive_partitioning=true)
    """)

    total_bronze = con.execute("SELECT COUNT(*) FROM bronze").fetchone()[0]
    logger.info(f"Bronze records loaded: {total_bronze}")

    # ── Step 2: Deduplicate on order_id (keep latest by event_ts) ──
    logger.info("Deduplicating on order_id...")
    con.execute("""
        CREATE TABLE deduped AS
        SELECT * FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_ts DESC) AS rn
            FROM bronze
        )
        WHERE rn = 1
    """)
    con.execute("ALTER TABLE deduped DROP COLUMN rn")

    total_deduped = con.execute("SELECT COUNT(*) FROM deduped").fetchone()[0]
    dupes_removed = total_bronze - total_deduped
    logger.info(f"After dedup: {total_deduped} records ({dupes_removed} duplicates removed)")

    # ── Step 3: Type casting & null handling ──
    logger.info("Applying type casts and null handling...")
    con.execute("""
        CREATE TABLE silver AS
        SELECT
            order_id,
            buyer_id,
            seller_id,
            COALESCE(seller_name, 'Unknown')       AS seller_name,
            COALESCE(seller_city, 'Unknown')        AS seller_city,
            product_id,
            COALESCE(product_name, 'Unknown')       AS product_name,
            COALESCE(category, 'Uncategorized')     AS category,
            COALESCE(quantity, 1)                    AS quantity,
            COALESCE(unit_price, 0.0)               AS unit_price,
            COALESCE(total_amount, 0.0)             AS total_amount,
            COALESCE(status, 'UNKNOWN')             AS status,
            COALESCE(buyer_city, 'Unknown')         AS buyer_city,
            CAST(event_ts AS TIMESTAMP)             AS event_ts,
            CAST(ingested_at AS TIMESTAMP)          AS ingested_at,
            -- derived fields for analytics
            CAST(event_ts AS DATE)                  AS order_date,
            HOUR(CAST(event_ts AS TIMESTAMP))       AS order_hour,
            CASE
                WHEN status = 'CANCELLED' THEN true
                ELSE false
            END                                     AS is_cancelled,
            -- data quality flag
            CASE
                WHEN order_id IS NULL OR seller_id IS NULL OR total_amount <= 0 THEN 'BAD'
                ELSE 'GOOD'
            END                                     AS dq_flag
        FROM deduped
        WHERE order_id IS NOT NULL
    """)

    total_silver = con.execute("SELECT COUNT(*) FROM silver").fetchone()[0]
    bad_records = con.execute("SELECT COUNT(*) FROM silver WHERE dq_flag = 'BAD'").fetchone()[0]
    logger.info(f"Silver records: {total_silver} (good: {total_silver - bad_records}, flagged: {bad_records})")

    # ── Step 4: Write to Parquet, partitioned by order_date and category ──
    logger.info(f"Writing Silver data to {SILVER_PATH}...")
    con.execute(f"""
        COPY silver TO '{SILVER_PATH}/orders.parquet'
        (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    # Also write partitioned version for downstream analytics
    partitioned_path = f"{SILVER_PATH}/partitioned"
    Path(partitioned_path).mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY silver TO '{partitioned_path}'
        (FORMAT PARQUET, PARTITION_BY (order_date, category), OVERWRITE_OR_IGNORE)
    """)

    # ── Summary stats ──
    logger.info("--- Silver Layer Summary ---")
    stats = con.execute("""
        SELECT
            COUNT(*)                                    AS total_orders,
            COUNT(DISTINCT order_id)                    AS unique_orders,
            COUNT(DISTINCT seller_id)                   AS unique_sellers,
            COUNT(DISTINCT category)                    AS categories,
            ROUND(SUM(total_amount), 2)                 AS total_gmv,
            SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) AS cancelled,
            MIN(event_ts) AS earliest,
            MAX(event_ts) AS latest
        FROM silver
    """).fetchone()

    logger.info(f"  Orders:    {stats[0]}")
    logger.info(f"  Sellers:   {stats[1]}")
    logger.info(f"  Categories:{stats[3]}")
    logger.info(f"  Total GMV: Rs.{stats[4]}")
    logger.info(f"  Cancelled: {stats[5]}")
    logger.info(f"  Time range: {stats[6]} to {stats[7]}")
    logger.success("Silver layer complete.")

    con.close()


if __name__ == "__main__":
    run()
