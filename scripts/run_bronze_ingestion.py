"""CLI Script to execute Bronze Layer Ingestion."""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.data_platform.config import settings  # noqa: E402
from src.data_platform.ingestion.bronze import BronzeIngestion  # noqa: E402
from src.data_platform.storage.session import get_spark_session  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run_bronze_ingestion")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw Olist datasets into Bronze Delta tables.")
    parser.add_argument(
        "--entity",
        type=str,
        default="customers",
        help="Entity to ingest (e.g., customers, orders, etc.)",
    )
    parser.add_argument(
        "--use-sample",
        action="store_true",
        help="Use sample dataset instead of full raw dataset.",
    )
    parser.add_argument(
        "--write-mode",
        type=str,
        default="append",
        choices=["append", "overwrite"],
        help="Delta write mode (append or overwrite)",
    )
    args = parser.parse_args()

    spark = get_spark_session(app_name="BronzeIngestionExecution")
    try:
        ingestion = BronzeIngestion(spark)

        if args.use_sample:
            source_file = settings.sample_path / f"olist_{args.entity}_sample.csv"
        else:
            source_file = settings.raw_path / f"olist_{args.entity}_dataset.csv"

        logger.info("Starting Bronze Ingestion for entity: %s", args.entity)
        logger.info("Source file: %s", source_file)

        result = ingestion.ingest_csv(
            entity=args.entity,
            input_file=source_file,
            write_mode=args.write_mode,
        )

        logger.info(
            "Ingestion completed successfully: Entity=%s, Records=%d, BatchId=%s, Target=%s",
            result.entity,
            result.records_count,
            result.batch_id,
            result.target_path,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
