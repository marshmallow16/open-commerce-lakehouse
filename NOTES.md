# ONDC Lakehouse — Project Notes
> Keep this file open. Updated after every step.

---

## Project Goal
Build a mini ONDC-like data lakehouse to demonstrate skills for the Data Architect JD.
Covers: Kafka streaming, Medallion architecture, ClickHouse OLAP, NL querying via LLM.

---

## Folder Structure
```
ondc-lakehouse/
├── docker-compose.yml       ← all infra (Kafka + ClickHouse)
├── run_pipeline.py          ← one command to run Silver + Gold
├── requirements.txt         ← Python dependencies
├── .env                     ← secrets & config (never commit this)
├── NOTES.md                 ← this file
├── infra/
│   └── clickhouse/init.sql  ← Gold layer tables (auto-runs on first start)
├── pipeline/
│   ├── bronze/
│   │   ├── producer.py      ← Kafka event generator
│   │   └── consumer.py      ← Kafka → raw Parquet
│   ├── silver/
│   │   └── transform.py     ← dedup, clean, type-cast
│   └── gold/
│       └── aggregate.py     ← Star schema + ClickHouse load
├── schema/
│   ├── order_event.json     ← JSON Schema for governance
│   └── validate.py          ← validation logic
├── nl_query/
│   └── engine.py            ← Claude API → SQL → DuckDB
├── dashboard/
│   └── app.py               ← Streamlit dashboard
└── data/
    ├── bronze/              ← raw Parquet from Kafka
    ├── silver/              ← cleaned Parquet
    └── gold/                ← fact + dim + agg Parquet
```

---

## What's Running (Infrastructure)

| Container        | What it does                          | Port           |
|------------------|---------------------------------------|----------------|
| ondc-kafka       | Kafka broker (KRaft, no Zookeeper)    | 9092           |
| ondc-kafka-ui    | Browser UI to inspect Kafka topics    | 8080           |
| ondc-clickhouse  | OLAP database (Gold layer queries)    | 8123, 9000     |

### ClickHouse tables (Gold layer, Star Schema)
- `ondc.fact_orders`      — every order event (500 rows)
- `ondc.dim_seller`       — seller dimension (20 rows)
- `ondc.dim_product`      — product dimension (500 rows)
- `ondc.agg_seller_daily` — pre-aggregated GMV per seller per day

### URLs
- Kafka UI: http://localhost:8080
- ClickHouse HTTP: http://localhost:8123
- Streamlit Dashboard: http://localhost:8501 (after launching)

---

## Steps Completed

- [x] Step 1 — Project folder structure
- [x] Step 2 — Docker Compose (Kafka KRaft + Kafka UI + ClickHouse)
- [x] Step 3 — ClickHouse Gold schema (init.sql)
- [x] Step 4 — requirements.txt and .env
- [x] Step 5 — Kafka producer (pipeline/bronze/producer.py)
  - Generates fake ONDC order events (order_id, seller, product, amount, status)
  - Streams into topic: ondc.orders.raw, partitioned by seller_id
  - Run: `venv/Scripts/python pipeline/bronze/producer.py --rate 10 --total 500`
- [x] Step 6 — Bronze consumer (pipeline/bronze/consumer.py)
  - Reads from Kafka, writes raw Parquet files to data/bronze/date=YYYY-MM-DD/
  - Batch size 100 = 1 Parquet file per batch (500 events = 5 files)
  - Manual offset commit = no data loss if write fails
  - Run: `venv/Scripts/python pipeline/bronze/consumer.py --batch-size 100`
- [x] Step 7 — Silver layer (pipeline/silver/transform.py)
  - Reads Bronze Parquet, deduplicates on order_id (ROW_NUMBER window function)
  - Type casts (event_ts string → timestamp), null handling (COALESCE)
  - Adds derived fields: order_date, order_hour, is_cancelled, dq_flag
  - Writes to data/silver/ as Parquet (flat + partitioned by date/category)
  - Run: `venv/Scripts/python pipeline/silver/transform.py`
- [x] Step 8 — Gold layer (pipeline/gold/aggregate.py)
  - Builds fact_orders, dim_seller, dim_product, agg_seller_daily
  - Writes Parquet locally + loads into ClickHouse
  - Run: `venv/Scripts/python pipeline/gold/aggregate.py`
- [x] Step 9 — Schema governance (schema/order_event.json + schema/validate.py)
  - JSON Schema with required fields, enums, min values
  - Validates events at Bronze boundary, flags bad records
  - Run: `venv/Scripts/python schema/validate.py`
- [x] Step 10 — NL Query layer (nl_query/engine.py)
  - User asks question in English → Claude generates SQL → DuckDB executes → answer
  - Requires ANTHROPIC_API_KEY in .env
  - Run: `venv/Scripts/python nl_query/engine.py`
- [x] Step 11 — Streamlit dashboard (dashboard/app.py)
  - Pipeline status (Bronze/Silver/Gold record counts)
  - Data preview at each layer (tabs)
  - Analytics charts (GMV by category, top sellers, status distribution)
  - NL Query interface (type question → get SQL + answer)
  - Run: `venv/Scripts/streamlit run dashboard/app.py`

---

## HOW TO RUN THE FULL PIPELINE (from scratch or after restart)

### Step 1 — Open terminal in project folder
```
cd "C:/Users/Magical Services Co/project/ondc-lakehouse"
```

### Step 2 — Start Docker containers
```
docker compose up -d
```
Wait ~30 seconds for health checks to pass.

### Step 3 — Verify containers
```
docker ps
```
Should see: ondc-kafka (healthy), ondc-kafka-ui (up), ondc-clickhouse (healthy)

### Step 4 — Activate Python venv
```
venv\Scripts\activate
```

### Step 5 — Generate events & ingest to Bronze
```
python pipeline/bronze/producer.py --rate 10 --total 500
python pipeline/bronze/consumer.py --batch-size 100
```
(producer sends 500 fake orders to Kafka, consumer writes them as Parquet)

### Step 6 — Run Silver + Gold pipeline
```
python run_pipeline.py
```
(This runs schema validation → Silver transform → Gold aggregate + ClickHouse load)

### Step 7 — Launch dashboard
```
streamlit run dashboard/app.py
```
Open http://localhost:8501 in browser.

---

## STOP EVERYTHING (when done for the day)
```
cd "C:/Users/Magical Services Co/project/ondc-lakehouse"
docker compose down
```
Note: Data is preserved in Docker volumes. Use `docker compose down -v` to wipe all data.

---

## Useful Commands (Quick Reference)

| Task                          | Command |
|-------------------------------|---------|
| Start all containers          | `docker compose up -d` |
| Stop all containers           | `docker compose down` |
| Check container status        | `docker ps` |
| View Kafka logs               | `docker logs ondc-kafka` |
| View ClickHouse logs          | `docker logs ondc-clickhouse` |
| Query ClickHouse directly     | `docker exec ondc-clickhouse clickhouse-client --user admin --password ondc1234` |
| Wipe all data (fresh start)   | `docker compose down -v` |
| Run full pipeline             | `python run_pipeline.py` |
| Launch dashboard              | `streamlit run dashboard/app.py` |

---

## Pipeline Results (from latest run)

| Layer   | Records | Details |
|---------|---------|---------|
| Bronze  | 500     | 5 Parquet files, raw from Kafka |
| Silver  | 500     | 0 duplicates removed, 500 good, 0 flagged |
| Gold    | 500 fact + 20 sellers + 500 products + 136 aggregations |

### GMV by Category (from ClickHouse)
| Category       | Orders | GMV (Rs.)    |
|---------------|--------|-------------|
| Electronics    | 75     | 565,909     |
| Sports         | 71     | 335,114     |
| Fashion        | 66     | 310,604     |
| Health         | 68     | 246,927     |
| Home & Kitchen | 78     | 184,946     |
| Books          | 82     | 81,106      |
| Grocery        | 60     | 29,044      |
| **Total**      | **500**| **Rs.17,53,650** |

---

## Interview One-Liner
> "I built a containerized data lakehouse simulating ONDC's open-commerce platform. Kafka streams order events into a Medallion pipeline (Bronze/Silver/Gold) with schema governance via JSON Schema. The Gold layer uses Star Schema modeling loaded into ClickHouse for OLAP queries, and I added an LLM-powered natural language query interface using Claude API that converts plain English questions to SQL."

---

## What each component maps to in the JD

| JD Requirement                    | What we built                          |
|-----------------------------------|----------------------------------------|
| Streaming architecture (Kafka)    | Kafka producer + consumer              |
| Medallion / Lakehouse design      | Bronze → Silver → Gold pipeline        |
| Columnar formats (Parquet)        | Parquet + Snappy at every layer        |
| Schema governance                 | JSON Schema + validation script        |
| OLAP (ClickHouse)                 | Gold tables in ClickHouse              |
| Data modeling (Star Schema)       | fact_orders + dim_seller + dim_product |
| NL Querying / LLM                 | Claude API → SQL → DuckDB             |
| Docker / Kubernetes               | Docker Compose (3 containers)          |
| Orchestration                     | run_pipeline.py (simple DAG runner)    |
