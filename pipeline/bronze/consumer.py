"""
ONDC Bronze Layer — Kafka Consumer
------------------------------------
Reads raw order events from Kafka topic (ondc.orders.raw)
and writes them as Parquet files to data/bronze/.

Rules of Bronze:
- NO transformations — data lands exactly as received
- Append-only — never update or delete
- Partitioned by date — data/bronze/date=YYYY-MM-DD/
- Schema is inferred from the incoming JSON

This layer ensures replayability: if Silver has a bug,
we reprocess from Bronze without touching the source system.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# ─────────────────────────────────────────
# Bronze Parquet Schema
# Explicit schema = schema governance starts here
# ─────────────────────────────────────────

BRONZE_SCHEMA = pa.schema([
    pa.field("order_id",     pa.string()),
    pa.field("buyer_id",     pa.string()),
    pa.field("seller_id",    pa.string()),
    pa.field("seller_name",  pa.string()),
    pa.field("seller_city",  pa.string()),
    pa.field("product_id",   pa.string()),
    pa.field("product_name", pa.string()),
    pa.field("category",     pa.string()),
    pa.field("quantity",     pa.int32()),
    pa.field("unit_price",   pa.float64()),
    pa.field("total_amount", pa.float64()),
    pa.field("status",       pa.string()),
    pa.field("buyer_city",   pa.string()),
    pa.field("event_ts",     pa.string()),
    pa.field("ingested_at",  pa.string()),   # added by Bronze — audit field
])


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

def get_date_partition(event_ts: str) -> str:
    """Extract date string from ISO timestamp for folder partitioning."""
    try:
        return event_ts[:10]   # 'YYYY-MM-DD'
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def write_batch_to_parquet(batch: list[dict], bronze_path: str):
    """Write a batch of events as a Parquet file, partitioned by date."""
    if not batch:
        return

    # Group records by date partition
    by_date: dict[str, list] = {}
    for record in batch:
        date_key = get_date_partition(record.get("event_ts", ""))
        by_date.setdefault(date_key, []).append(record)

    for date_key, records in by_date.items():
        partition_dir = Path(bronze_path) / f"date={date_key}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Use timestamp in filename to avoid overwriting previous batches
        ts = datetime.now(timezone.utc).strftime("%H%M%S%f")
        file_path = partition_dir / f"orders_{ts}.parquet"

        table = pa.Table.from_pylist(records, schema=BRONZE_SCHEMA)
        pq.write_table(table, file_path, compression="snappy")

        logger.info(f"Written {len(records)} records → {file_path}")


# ─────────────────────────────────────────
# Main consumer loop
# ─────────────────────────────────────────

def run(batch_size: int = 100, timeout_seconds: int = 10):
    topic       = os.getenv("KAFKA_TOPIC_ORDERS", "ondc.orders.raw")
    bootstrap   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    bronze_path = os.getenv("BRONZE_PATH", "./data/bronze")

    consumer = Consumer({
        "bootstrap.servers":  bootstrap,
        "group.id":           "bronze-consumer-group",   # consumer group — Kafka tracks offset per group
        "auto.offset.reset":  "earliest",                # start from the very first message in topic
        "enable.auto.commit": False,                     # we commit manually after writing to disk
    })

    consumer.subscribe([topic])
    logger.info(f"Subscribed to topic: {topic} | batch size: {batch_size}")

    batch   = []
    empty_polls = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)   # wait up to 1s for a message

            if msg is None:
                empty_polls += 1
                if batch:
                    logger.debug(f"No new messages. Flushing current batch of {len(batch)}...")
                    write_batch_to_parquet(batch, bronze_path)
                    consumer.commit()
                    batch = []
                    empty_polls = 0
                if empty_polls >= timeout_seconds:
                    logger.info("No messages for a while. Consumer shutting down.")
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition.")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            # Decode and enrich message
            record = json.loads(msg.value().decode("utf-8"))
            record["ingested_at"] = datetime.now(timezone.utc).isoformat()   # audit timestamp
            batch.append(record)

            # Write batch when it reaches batch_size
            if len(batch) >= batch_size:
                write_batch_to_parquet(batch, bronze_path)
                consumer.commit()   # tell Kafka: "we've processed up to this offset"
                batch = []

    except KeyboardInterrupt:
        logger.warning("Interrupted. Flushing remaining batch...")
        if batch:
            write_batch_to_parquet(batch, bronze_path)
            consumer.commit()
    finally:
        consumer.close()
        logger.success("Bronze consumer closed.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ONDC Bronze Layer Kafka Consumer")
    parser.add_argument("--batch-size", type=int, default=100, help="Records per Parquet file (default: 100)")
    args = parser.parse_args()

    run(batch_size=args.batch_size)
