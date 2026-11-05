"""CLI Script to execute Silver Layer Transformation and Upsert."""

import argparse
import logging
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.data_platform.config import settings  # noqa: E402
from src.data_platform.ingestion.bronze import BronzeIngestion  # noqa: E402
from src.data_platform.storage.session import get_spark_session  # noqa: E402
from src.data_platform.transformations.silver import SilverTransformer  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run_silver_transformation")


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform Bronze Delta tables into cleansed Silver Delta tables.")
    parser.add_argument(
        "--entity",
        type=str,
        default="customers",
        help="Entity to transform (e.g. customers)",
    )
    args = parser.parse_args()

    spark = get_spark_session(app_name="SilverTransformationExecution")
    try:
        bronze_engine = BronzeIngestion(spark)
        silver_engine = SilverTransformer(spark)

        logger.info("Reading Bronze Delta table for entity: %s", args.entity)
        bronze_df = bronze_engine.read_bronze(entity=args.entity, base_dir=settings.bronze_path)

        if args.entity == "customers":
            cleansed_df = silver_engine.transform_customers(bronze_df)
            primary_keys = ["customer_id"]
        else:
            raise NotImplementedError(f"Silver transformation not yet implemented for entity: {args.entity}")

        logger.info("Executing Silver upsert with MERGE INTO on keys: %s", primary_keys)
        result = silver_engine.upsert_silver(
            df=cleansed_df,
            entity=args.entity,
            primary_keys=primary_keys,
            output_dir=settings.silver_path,
        )

        logger.info(
            "Silver transformation completed successfully: Entity=%s, TotalRecords=%d, Target=%s",
            result.entity,
            result.records_count,
            result.target_path,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
