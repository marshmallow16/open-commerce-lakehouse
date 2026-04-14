"""
ONDC Order Event Producer
--------------------------
Generates fake ONDC-like e-commerce order events and streams them
into a Kafka topic (ondc.orders.raw) in real time.

Each event represents one order placed on the ONDC network.
"""

import json
import random
import time
import uuid
from datetime import datetime

from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker
from loguru import logger
import os

load_dotenv()

fake = Faker("en_IN")  # Indian locale for realistic names/cities

# ─────────────────────────────────────────
# Reference data (mimics ONDC catalogue)
# ─────────────────────────────────────────

CATEGORIES = ["Electronics", "Fashion", "Grocery", "Home & Kitchen", "Books", "Health", "Sports"]

PRODUCTS = {
    "Electronics":    [("Boat Earphones", 999),  ("Mi Power Bank", 1299), ("Fire TV Stick", 3999)],
    "Fashion":        [("Cotton Kurta", 499),     ("Denim Jeans", 1299),  ("Saree", 2499)],
    "Grocery":        [("Tata Salt 1kg", 25),     ("Amul Butter", 55),    ("Basmati Rice 5kg", 350)],
    "Home & Kitchen": [("Prestige Cooker", 1799), ("Milton Bottle", 399), ("Broom Set", 199)],
    "Books":          [("Wings of Fire", 195),    ("Atomic Habits", 499), ("Rich Dad Poor Dad", 350)],
    "Health":         [("Whey Protein", 2499),    ("Vitamin C 60tab", 299),("BP Monitor", 1599)],
    "Sports":         [("Yoga Mat", 799),         ("Cricket Bat", 2499),  ("Badminton Set", 1299)],
}

STATUSES = ["CREATED", "CONFIRMED", "CONFIRMED", "CONFIRMED", "DELIVERED", "DELIVERED", "CANCELLED"]
# Weighted: more CONFIRMED and DELIVERED than CANCELLED (realistic distribution)

CITIES = ["Delhi", "Mumbai", "Bengaluru", "Chennai", "Hyderabad", "Pune", "Kolkata", "Ahmedabad"]

# Pre-generate a pool of sellers (simulates registered ONDC sellers)
SELLERS = [
    {"seller_id": str(uuid.uuid4())[:8], "name": fake.company(), "city": random.choice(CITIES)}
    for _ in range(20)
]


# ─────────────────────────────────────────
# Event generator
# ─────────────────────────────────────────

def generate_order_event() -> dict:
    category = random.choice(CATEGORIES)
    product_name, base_price = random.choice(PRODUCTS[category])
    quantity = random.randint(1, 5)
    # Add slight price variation (+/- 10%) to simulate dynamic pricing
    unit_price = round(base_price * random.uniform(0.9, 1.1), 2)
    seller = random.choice(SELLERS)

    return {
        "order_id":     str(uuid.uuid4()),
        "buyer_id":     str(uuid.uuid4())[:8],
        "seller_id":    seller["seller_id"],
        "seller_name":  seller["name"],
        "seller_city":  seller["city"],
        "product_id":   str(uuid.uuid4())[:8],
        "product_name": product_name,
        "category":     category,
        "quantity":     quantity,
        "unit_price":   unit_price,
        "total_amount": round(unit_price * quantity, 2),
        "status":       random.choice(STATUSES),
        "buyer_city":   random.choice(CITIES),
        "event_ts":     datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────
# Kafka delivery callback
# ─────────────────────────────────────────

def on_delivery(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Delivered → topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


# ─────────────────────────────────────────
# Main producer loop
# ─────────────────────────────────────────

def run(events_per_second: int = 5, total_events: int = 1000):
    topic = os.getenv("KAFKA_TOPIC_ORDERS", "ondc.orders.raw")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    producer = Producer({"bootstrap.servers": bootstrap})

    logger.info(f"Starting producer → topic: {topic} | rate: {events_per_second} events/sec | total: {total_events}")

    interval = 1.0 / events_per_second
    sent = 0

    try:
        while sent < total_events:
            event = generate_order_event()
            producer.produce(
                topic=topic,
                key=event["seller_id"],          # partition by seller — same seller's events go to same partition
                value=json.dumps(event).encode("utf-8"),
                callback=on_delivery,
            )
            producer.poll(0)                     # trigger callbacks without blocking

            sent += 1
            if sent % 100 == 0:
                logger.info(f"Sent {sent}/{total_events} events...")

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
    finally:
        logger.info("Flushing remaining messages...")
        producer.flush()
        logger.success(f"Done. Total events sent: {sent}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ONDC Kafka Order Event Producer")
    parser.add_argument("--rate",   type=int, default=5,    help="Events per second (default: 5)")
    parser.add_argument("--total",  type=int, default=1000, help="Total events to send (default: 1000)")
    args = parser.parse_args()

    run(events_per_second=args.rate, total_events=args.total)
