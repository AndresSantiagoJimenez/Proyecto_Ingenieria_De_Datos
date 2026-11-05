"""End-to-End Medallion Lakehouse Pipeline Runner (Bronze -> DQA/Quarantine -> Silver -> Gold)."""

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.data_platform.config import settings  # noqa: E402
from src.data_platform.ingestion.bronze import BronzeIngestion  # noqa: E402
from src.data_platform.modeling.gold import GoldModeler  # noqa: E402
from src.data_platform.quality.dqa import DataQualityAssurance  # noqa: E402
from src.data_platform.quality.quarantine import QuarantineManager  # noqa: E402
from src.data_platform.storage.session import get_spark_session  # noqa: E402
from src.data_platform.transformations.silver import SilverTransformer  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("medallion_pipeline")


@dataclass
class PipelineExecutionSummary:
    """Consolidated summary of end-to-end pipeline run."""

    entity: str
    started_at: datetime
    completed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    bronze_count: int = 0
    quarantine_count: int = 0
    silver_count: int = 0
    gold_dim_count: int = 0
    gold_fact_count: int = 0
    status: str = "SUCCESS"


def run_pipeline(
    entity: str = "customers",
    use_sample: bool = False,
    spark=None,
    bronze_dir: Path | str | None = None,
    silver_dir: Path | str | None = None,
    gold_dir: Path | str | None = None,
    quarantine_dir: Path | str | None = None,
) -> PipelineExecutionSummary:
    """Execute complete Medallion Lakehouse pipeline for a given entity."""
    started_at = datetime.now(timezone.utc)
    spark_provided = spark is not None
    active_spark = spark or get_spark_session(app_name=f"MedallionPipeline_{entity}")

    b_dir = Path(bronze_dir) if bronze_dir else settings.bronze_path
    s_dir = Path(silver_dir) if silver_dir else settings.silver_path
    g_dir = Path(gold_dir) if gold_dir else settings.gold_path
    q_dir = Path(quarantine_dir) if quarantine_dir else settings.quarantine_path

    try:
        logger.info("=" * 70)
        logger.info("STARTING MEDALLION LAKEHOUSE PIPELINE: %s", entity.upper())
        logger.info("=" * 70)

        # ----------------------------------------------------------------------
        # 1. BRONZE LAYER INGESTION
        # ----------------------------------------------------------------------
        logger.info("[Step 1/4] Ingesting Raw data into Bronze Delta Lake...")
        bronze_engine = BronzeIngestion(active_spark)
        if use_sample:
            source_file = settings.sample_path / f"olist_{entity}_sample.csv"
        else:
            source_file = settings.raw_path / f"olist_{entity}_dataset.csv"
            if not source_file.exists():
                logger.warning("Raw file not found at %s. Falling back to sample file.", source_file)
                source_file = settings.sample_path / f"olist_{entity}_sample.csv"

        bronze_result = bronze_engine.ingest_csv(
            entity=entity,
            input_file=source_file,
            output_dir=b_dir,
            write_mode="overwrite",
        )
        logger.info(
            "Bronze Ingestion Done: %d records ingested into Delta Bronze [batch=%s]",
            bronze_result.records_count,
            bronze_result.batch_id,
        )

        # ----------------------------------------------------------------------
        # 2. DATA QUALITY ASSURANCE & QUARANTINE
        # ----------------------------------------------------------------------
        logger.info("[Step 2/4] Evaluating Data Quality Rules & Quarantine Isolation...")
        bronze_df = bronze_engine.read_bronze(entity=entity, base_dir=b_dir)
        dqa_engine = DataQualityAssurance(active_spark)
        quarantine_mgr = QuarantineManager(active_spark)

        valid_df, quarantine_df, dqa_result = dqa_engine.validate(bronze_df, entity=entity)
        logger.info(
            "DQA Check: Total=%d | Valid=%d | Quarantined=%d",
            dqa_result.total_evaluated,
            dqa_result.valid_count,
            dqa_result.quarantined_count,
        )

        if dqa_result.quarantined_count > 0:
            quarantine_mgr.save_to_quarantine(quarantine_df, entity=entity, base_dir=q_dir)
            logger.warning("Quarantined %d non-compliant records.", dqa_result.quarantined_count)

        # ----------------------------------------------------------------------
        # 3. SILVER LAYER TRANSFORMATION & MERGE INTO
        # ----------------------------------------------------------------------
        logger.info("[Step 3/4] Transforming and Upserting into Silver Delta Lake...")
        silver_engine = SilverTransformer(active_spark)
        if entity == "customers":
            cleansed_df = silver_engine.transform_customers(valid_df)
            primary_keys = ["customer_id"]
        else:
            cleansed_df = valid_df
            primary_keys = ["id"]

        silver_result = silver_engine.upsert_silver(
            df=cleansed_df,
            entity=entity,
            primary_keys=primary_keys,
            output_dir=s_dir,
        )
        logger.info(
            "Silver Upsert Done: Total records in Silver Delta: %d",
            silver_result.records_count,
        )

        # ----------------------------------------------------------------------
        # 4. GOLD LAYER DIMENSIONAL & ANALYTICAL MODELING
        # ----------------------------------------------------------------------
        logger.info("[Step 4/4] Building Gold Star Schema & Analytics...")
        gold_engine = GoldModeler(active_spark)
        silver_customers = silver_engine.read_silver("customers", base_dir=s_dir)

        # 4a. dim_customers
        dim_cust_df = gold_engine.build_dim_customers(silver_customers)
        res_dim_cust = gold_engine.save_gold_table(dim_cust_df, "dim_customers", base_dir=g_dir)

        # 4b. If sample orders/items/payments exist, build fact_sales, RFM, logistics
        gold_fact_count = 0
        sample_dir = settings.sample_path
        orders_file = sample_dir / "olist_orders_sample.csv"
        items_file = sample_dir / "olist_order_items_sample.csv"
        payments_file = sample_dir / "olist_order_payments_sample.csv"

        if orders_file.exists() and items_file.exists() and payments_file.exists():
            orders_df = active_spark.read.format("csv").option("header", "true").load(str(orders_file))
            items_df = active_spark.read.format("csv").option("header", "true").load(str(items_file))
            payments_df = active_spark.read.format("csv").option("header", "true").load(str(payments_file))

            res_sales = gold_engine.save_gold_table(
                gold_engine.build_fact_sales(orders_df, items_df, payments_df),
                "fact_sales",
                base_dir=g_dir,
            )
            gold_engine.save_gold_table(
                gold_engine.build_dim_customer_rfm(silver_customers, orders_df, payments_df),
                "dim_customer_rfm",
                base_dir=g_dir,
            )
            gold_engine.save_gold_table(
                gold_engine.build_dim_logistics(orders_df, silver_customers),
                "dim_logistics",
                base_dir=g_dir,
            )
            gold_fact_count = res_sales.records_count

        completed_at = datetime.now(timezone.utc)
        summary = PipelineExecutionSummary(
            entity=entity,
            started_at=started_at,
            completed_at=completed_at,
            bronze_count=bronze_result.records_count,
            quarantine_count=dqa_result.quarantined_count,
            silver_count=silver_result.records_count,
            gold_dim_count=res_dim_cust.records_count,
            gold_fact_count=gold_fact_count,
            status="SUCCESS",
        )

        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY: %s", summary)
        logger.info("=" * 70)
        return summary

    finally:
        if not spark_provided:
            active_spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run End-to-End Medallion Lakehouse Pipeline.")
    parser.add_argument(
        "--entity",
        type=str,
        default="customers",
        help="Primary entity to run pipeline for.",
    )
    parser.add_argument(
        "--use-sample",
        action="store_true",
        help="Force using sample CSV data rather than full raw dataset.",
    )
    args = parser.parse_args()
    run_pipeline(entity=args.entity, use_sample=args.use_sample)


if __name__ == "__main__":
    main()
