"""
Natural Language Query Engine
-------------------------------
User asks a question in plain English.
Claude generates SQL → runs on DuckDB (Gold Parquet) → returns answer.

This addresses the JD requirement:
  "Contribute to frameworks enabling natural-language analytics,
   integrating LLM-powered question-answer systems over structured data."
"""

import os
from pathlib import Path

import anthropic
import duckdb
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

GOLD_PATH = os.getenv("GOLD_PATH", "./data/gold")

TABLE_CONTEXT = """
You are a SQL analyst for ONDC (Open Network for Digital Commerce).
You have access to these tables in DuckDB:

TABLE: fact_orders
  - order_id (VARCHAR) — unique order ID
  - buyer_id (VARCHAR)
  - seller_id (VARCHAR)
  - product_id (VARCHAR)
  - category (VARCHAR) — Electronics, Fashion, Grocery, Home & Kitchen, Books, Health, Sports
  - quantity (INTEGER)
  - unit_price (DOUBLE)
  - total_amount (DOUBLE)
  - status (VARCHAR) — CREATED, CONFIRMED, CANCELLED, DELIVERED
  - event_ts (TIMESTAMP)
  - order_date (DATE)
  - order_hour (INTEGER)
  - is_cancelled (BOOLEAN)
  - buyer_city (VARCHAR)
  - seller_city (VARCHAR)

TABLE: dim_seller
  - seller_id (VARCHAR)
  - seller_name (VARCHAR)
  - city (VARCHAR)
  - category (VARCHAR)
  - registered_at (TIMESTAMP)

TABLE: dim_product
  - product_id (VARCHAR)
  - product_name (VARCHAR)
  - category (VARCHAR)
  - seller_id (VARCHAR)
  - base_price (DOUBLE)

TABLE: agg_seller_daily
  - date (DATE)
  - seller_id (VARCHAR)
  - category (VARCHAR)
  - total_orders (BIGINT)
  - total_gmv (DOUBLE)
  - cancelled_orders (BIGINT)

Respond ONLY with a valid DuckDB SQL query. No explanation, no markdown, just pure SQL.
"""


def get_duckdb_connection():
    """Create DuckDB connection with Gold layer tables loaded."""
    con = duckdb.connect()
    gold = Path(GOLD_PATH)

    for parquet_file in gold.glob("*.parquet"):
        table_name = parquet_file.stem
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_file}')")
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.debug(f"Loaded {table_name}: {count} rows")

    return con


def generate_sql(question: str) -> str:
    """Use Claude to convert natural language to SQL."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key or api_key == "your-key-here":
        raise ValueError("Set ANTHROPIC_API_KEY in .env file")

    client = anthropic.Anthropic(api_key=api_key)

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        messages=[
            {"role": "user", "content": f"{TABLE_CONTEXT}\n\nQuestion: {question}"}
        ],
    )

    sql = message.content[0].text.strip()
    # Remove markdown code block if present
    if sql.startswith("```"):
        sql = sql.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return sql


def ask(question: str) -> dict:
    """Full pipeline: question → SQL → execute → answer."""
    logger.info(f"Question: {question}")

    sql = generate_sql(question)
    logger.info(f"Generated SQL: {sql}")

    con = get_duckdb_connection()
    try:
        result = con.execute(sql).fetchdf()
        logger.info(f"Result: {len(result)} rows")
        return {
            "question": question,
            "sql": sql,
            "result": result,
            "error": None,
        }
    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        return {
            "question": question,
            "sql": sql,
            "result": None,
            "error": str(e),
        }
    finally:
        con.close()


def demo():
    """Interactive demo — ask questions from terminal."""
    print("\n=== ONDC Natural Language Query Engine ===")
    print("Ask questions about ONDC order data in plain English.")
    print("Type 'quit' to exit.\n")

    while True:
        question = input("Your question: ").strip()
        if question.lower() in ("quit", "exit", "q"):
            break
        if not question:
            continue

        result = ask(question)
        print(f"\nSQL: {result['sql']}")
        if result["error"]:
            print(f"Error: {result['error']}")
        else:
            print(f"\n{result['result'].to_string()}\n")


if __name__ == "__main__":
    demo()
