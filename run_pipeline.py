"""
ONDC Lakehouse — Full Pipeline Runner
----------------------------------------
Runs the entire pipeline end-to-end:
  1. Schema validation demo
  2. Silver transform (Bronze → Silver)
  3. Gold aggregation (Silver → Gold + ClickHouse)

Prerequisites:
  - Docker containers running (docker compose up -d)
  - Bronze data already ingested (producer.py + consumer.py)
"""

from loguru import logger


def main():
    logger.info("=" * 60)
    logger.info("  ONDC Lakehouse — Full Pipeline Run")
    logger.info("=" * 60)

    # Step 1: Schema validation demo
    logger.info("\n[1/3] Schema Governance — Validation Demo")
    logger.info("-" * 40)
    from schema.validate import demo as schema_demo
    schema_demo()

    # Step 2: Silver
    logger.info("\n[2/3] Silver Layer — Clean & Deduplicate")
    logger.info("-" * 40)
    from pipeline.silver.transform import run as silver_run
    silver_run()

    # Step 3: Gold
    logger.info("\n[3/3] Gold Layer — Aggregate & Load to ClickHouse")
    logger.info("-" * 40)
    from pipeline.gold.aggregate import run as gold_run
    gold_run()

    logger.success("\n" + "=" * 60)
    logger.success("  Pipeline complete!")
    logger.success("  - Bronze: data/bronze/")
    logger.success("  - Silver: data/silver/")
    logger.success("  - Gold:   data/gold/ + ClickHouse")
    logger.success("  Run dashboard: streamlit run dashboard/app.py")
    logger.success("=" * 60)


if __name__ == "__main__":
    main()
