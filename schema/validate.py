"""
Schema Governance — Validate events against JSON Schema
---------------------------------------------------------
Validates order events at the Bronze ingestion boundary.
Any event failing schema validation gets flagged and routed to a dead-letter queue.

This mimics a central schema registry (like AWS Glue Schema Registry
or Confluent Schema Registry) for governance and data quality.
"""

import json
from pathlib import Path

from jsonschema import Draft7Validator, ValidationError
from loguru import logger


SCHEMA_PATH = Path(__file__).parent / "order_event.json"


def load_schema() -> dict:
    with open(SCHEMA_PATH) as f:
        return json.load(f)


def validate_event(event: dict, validator: Draft7Validator) -> tuple[bool, list[str]]:
    """Validate a single event. Returns (is_valid, list_of_errors)."""
    errors = []
    for error in validator.iter_errors(event):
        errors.append(f"{error.json_path}: {error.message}")
    return len(errors) == 0, errors


def validate_batch(events: list[dict]) -> dict:
    """Validate a batch of events. Returns summary with valid/invalid counts."""
    schema = load_schema()
    validator = Draft7Validator(schema)

    valid = []
    invalid = []

    for event in events:
        is_ok, errors = validate_event(event, validator)
        if is_ok:
            valid.append(event)
        else:
            invalid.append({"event": event, "errors": errors})

    return {
        "total": len(events),
        "valid": len(valid),
        "invalid": len(invalid),
        "valid_events": valid,
        "invalid_events": invalid,
    }


def demo():
    """Demo validation with a good and bad event."""
    good_event = {
        "order_id": "abc-123",
        "buyer_id": "b001",
        "seller_id": "s001",
        "product_id": "p001",
        "product_name": "Boat Earphones",
        "category": "Electronics",
        "quantity": 2,
        "unit_price": 999.0,
        "total_amount": 1998.0,
        "status": "CONFIRMED",
        "event_ts": "2026-04-13T10:00:00",
    }

    bad_event = {
        "order_id": "",
        "buyer_id": "b002",
        "quantity": -1,
        "status": "INVALID_STATUS",
    }

    result = validate_batch([good_event, bad_event])

    logger.info(f"Validated {result['total']} events: {result['valid']} valid, {result['invalid']} invalid")

    for item in result["invalid_events"]:
        logger.warning(f"Invalid event {item['event'].get('order_id', 'N/A')}:")
        for err in item["errors"]:
            logger.warning(f"  - {err}")


if __name__ == "__main__":
    demo()
