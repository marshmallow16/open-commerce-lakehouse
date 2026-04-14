-- ONDC Lakehouse — Gold Layer Tables in ClickHouse
-- Runs automatically on first container start

CREATE DATABASE IF NOT EXISTS ondc;

-- ──────────────────────────────────────────
-- fact_orders: one row per order event
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ondc.fact_orders (
    order_id        String,
    buyer_id        String,
    seller_id       String,
    product_id      String,
    category        String,
    quantity        UInt32,
    unit_price      Float64,
    total_amount    Float64,
    status          String,         -- CREATED / CONFIRMED / CANCELLED / DELIVERED
    event_ts        DateTime,
    date            Date MATERIALIZED toDate(event_ts)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_ts)
ORDER BY (seller_id, event_ts)
SETTINGS index_granularity = 8192;

-- ──────────────────────────────────────────
-- dim_seller: seller dimension
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ondc.dim_seller (
    seller_id       String,
    seller_name     String,
    city            String,
    category        String,
    registered_at   DateTime
)
ENGINE = ReplacingMergeTree(registered_at)
ORDER BY seller_id;

-- ──────────────────────────────────────────
-- dim_product: product dimension
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ondc.dim_product (
    product_id      String,
    product_name    String,
    category        String,
    seller_id       String,
    base_price      Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY product_id;

-- ──────────────────────────────────────────
-- agg_seller_daily: pre-aggregated GMV
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ondc.agg_seller_daily (
    date            Date,
    seller_id       String,
    category        String,
    total_orders    UInt64,
    total_gmv       Float64,
    cancelled_orders UInt64
)
ENGINE = SummingMergeTree((total_orders, total_gmv, cancelled_orders))
PARTITION BY toYYYYMM(date)
ORDER BY (date, seller_id);
