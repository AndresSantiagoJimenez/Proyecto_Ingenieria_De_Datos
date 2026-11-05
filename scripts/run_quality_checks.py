"""CLI Script to execute Data Quality Assurance (DQA) and Quarantine isolation."""

import argparse
import logging
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.data_platform.config import settings  # noqa: E402
from src.data_platform.ingestion.bronze import BronzeIngestion  # noqa: E402
from src.data_platform.quality.dqa import DataQualityAssurance  # noqa: E402
from src.data_platform.quality.quarantine import QuarantineManager  # noqa: E402
from src.data_platform.storage.session import get_spark_session  # noqa: E402
from src.data_platform.transformations.silver import SilverTransformer  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run_quality_checks")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Execute Data Quality rules and isolate non-compliant records in Quarantine."
    )
    parser.add_argument(
        "--entity",
        type=str,
        default="customers",
        help="Entity to validate (e.g. customers, orders)",
    )
    parser.add_argument(
        "--upsert-silver",
        action="store_true",
        help="Whether to upsert validated clean records into Silver layer.",
    )
    args = parser.parse_args()

    spark = get_spark_session(app_name="DQAValidationExecution")
    try:
        bronze_engine = BronzeIngestion(spark)
        dqa_engine = DataQualityAssurance(spark)
        quarantine_mgr = QuarantineManager(spark)

        logger.info("Reading Bronze Delta table for entity: %s", args.entity)
        bronze_df = bronze_engine.read_bronze(entity=args.entity, base_dir=settings.bronze_path)

        logger.info("Validating entity '%s' against Data Quality rules...", args.entity)
        valid_df, quarantine_df, result = dqa_engine.validate(df=bronze_df, entity=args.entity)

        logger.info(
            "Validation result: Total=%d, Valid=%d, Quarantined=%d",
            result.total_evaluated,
            result.valid_count,
            result.quarantined_count,
        )

        if result.quarantined_count > 0:
            logger.warning("Rejection reasons breakdown:")
            for reason, count in result.reasons_summary.items():
                logger.warning("  - %s: %d records", reason, count)

            saved = quarantine_mgr.save_to_quarantine(
                quarantine_df=quarantine_df,
                entity=args.entity,
                base_dir=settings.quarantine_path,
            )
            logger.info("Saved %d records to Delta Quarantine at %s", saved, settings.quarantine_path / args.entity)
        else:
            logger.info("No data quality violations found. Quarantine was not updated.")

        if args.upsert_silver and result.valid_count > 0:
            logger.info("Upserting %d valid records into Silver layer...", result.valid_count)
            silver_engine = SilverTransformer(spark)
            if args.entity == "customers":
                cleansed_valid_df = silver_engine.transform_customers(valid_df)
                silver_result = silver_engine.upsert_silver(
                    df=cleansed_valid_df,
                    entity=args.entity,
                    primary_keys=["customer_id"],
                    output_dir=settings.silver_path,
                )
                logger.info(
                    "Silver upsert completed: Entity=%s, Records=%d",
                    silver_result.entity,
                    silver_result.records_count,
                )

        # Print quarantine metrics
        metrics = quarantine_mgr.get_quarantine_metrics(entity=args.entity)
        logger.info("Current Quarantine Metrics for '%s': %s", args.entity, metrics)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
