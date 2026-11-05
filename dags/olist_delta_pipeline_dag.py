"""Apache Airflow DAG — Olist E-Commerce Medallion Lakehouse Pipeline (Delta Lake)."""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to sys.path so Airflow workers can import src modules
AIRFLOW_HOME = Path(__file__).resolve().parent.parent
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

logger = logging.getLogger("airflow.task")

# Safe Airflow import pattern allowing local inspection and standalone testing
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = None  # type: ignore
    PythonOperator = None  # type: ignore


# ------------------------------------------------------------------------------
# Task Callables
# ------------------------------------------------------------------------------

def task_validate_sources(**kwargs) -> dict[str, str]:
    """Validate lakehouse storage paths and presence of input datasets."""
    from src.data_platform.config import settings

    logger.info("Validating Lakehouse storage directories...")
    for p in [settings.bronze_path, settings.silver_path, settings.gold_path, settings.quarantine_path]:
        p.mkdir(parents=True, exist_ok=True)

    raw_file = settings.raw_path / "olist_customers_dataset.csv"
    sample_file = settings.sample_path / "olist_customers_sample.csv"

    source = str(raw_file) if raw_file.exists() else str(sample_file)
    logger.info("Source dataset identified: %s", source)
    return {"source_file": source}


def task_ingest_bronze(**kwargs) -> dict[str, int]:
    """Ingest raw datasets into Bronze layer preserving audit metadata."""
    from src.data_platform.config import settings
    from src.data_platform.ingestion.bronze import BronzeIngestion
    from src.data_platform.storage.session import get_spark_session

    spark = get_spark_session(app_name="Airflow_BronzeIngestion")
    try:
        raw_file = settings.raw_path / "olist_customers_dataset.csv"
        input_file = raw_file if raw_file.exists() else settings.sample_path / "olist_customers_sample.csv"

        ingestion = BronzeIngestion(spark)
        result = ingestion.ingest_csv(
            entity="customers",
            input_file=input_file,
            write_mode="overwrite",
        )
        logger.info("Bronze task finished: %d records ingested.", result.records_count)
        return {"records_ingested": result.records_count}
    finally:
        spark.stop()


def task_dqa_and_quarantine(**kwargs) -> dict[str, int]:
    """Evaluate quality rules on Bronze data and isolate rejected records in Delta Quarantine."""
    from src.data_platform.ingestion.bronze import BronzeIngestion
    from src.data_platform.quality.dqa import DataQualityAssurance
    from src.data_platform.quality.quarantine import QuarantineManager
    from src.data_platform.storage.session import get_spark_session

    spark = get_spark_session(app_name="Airflow_DQAQuarantine")
    try:
        bronze_engine = BronzeIngestion(spark)
        dqa_engine = DataQualityAssurance(spark)
        quarantine_mgr = QuarantineManager(spark)

        bronze_df = bronze_engine.read_bronze("customers")
        valid_df, quarantine_df, result = dqa_engine.validate(bronze_df, entity="customers")

        if result.quarantined_count > 0:
            quarantine_mgr.save_to_quarantine(quarantine_df, entity="customers")

        logger.info(
            "DQA task completed: Valid=%d, Quarantined=%d",
            result.valid_count,
            result.quarantined_count,
        )
        return {
            "valid_count": result.valid_count,
            "quarantined_count": result.quarantined_count,
        }
    finally:
        spark.stop()


def task_transform_silver(**kwargs) -> dict[str, int]:
    """Cleanse, deduplicate and upsert valid records into Silver Delta using MERGE INTO."""
    from src.data_platform.ingestion.bronze import BronzeIngestion
    from src.data_platform.quality.dqa import DataQualityAssurance
    from src.data_platform.storage.session import get_spark_session
    from src.data_platform.transformations.silver import SilverTransformer

    spark = get_spark_session(app_name="Airflow_SilverTransformation")
    try:
        bronze_engine = BronzeIngestion(spark)
        dqa_engine = DataQualityAssurance(spark)
        silver_engine = SilverTransformer(spark)

        bronze_df = bronze_engine.read_bronze("customers")
        valid_df, _, _ = dqa_engine.validate(bronze_df, entity="customers")

        cleansed_df = silver_engine.transform_customers(valid_df)
        result = silver_engine.upsert_silver(
            df=cleansed_df,
            entity="customers",
            primary_keys=["customer_id"],
        )
        logger.info("Silver task finished: %d records in Silver Delta.", result.records_count)
        return {"silver_records": result.records_count}
    finally:
        spark.stop()


def task_build_gold_analytics(**kwargs) -> dict[str, int]:
    """Construct star schema fact/dimension tables and customer RFM analytics in Gold Delta."""
    from src.data_platform.config import settings
    from src.data_platform.modeling.gold import GoldModeler
    from src.data_platform.storage.session import get_spark_session
    from src.data_platform.transformations.silver import SilverTransformer

    spark = get_spark_session(app_name="Airflow_GoldAnalytics")
    try:
        silver_engine = SilverTransformer(spark)
        gold_engine = GoldModeler(spark)

        silver_customers = silver_engine.read_silver("customers")
        res_dim_cust = gold_engine.save_gold_table(
            gold_engine.build_dim_customers(silver_customers),
            "dim_customers",
        )

        sample_dir = settings.sample_path
        orders_f = sample_dir / "olist_orders_sample.csv"
        items_f = sample_dir / "olist_order_items_sample.csv"
        pay_f = sample_dir / "olist_order_payments_sample.csv"

        fact_count = 0
        if orders_f.exists() and items_f.exists() and pay_f.exists():
            orders_df = spark.read.format("csv").option("header", "true").load(str(orders_f))
            items_df = spark.read.format("csv").option("header", "true").load(str(items_f))
            pay_df = spark.read.format("csv").option("header", "true").load(str(pay_f))

            res_sales = gold_engine.save_gold_table(
                gold_engine.build_fact_sales(orders_df, items_df, pay_df),
                "fact_sales",
            )
            gold_engine.save_gold_table(
                gold_engine.build_dim_customer_rfm(silver_customers, orders_df, pay_df),
                "dim_customer_rfm",
            )
            gold_engine.save_gold_table(
                gold_engine.build_dim_logistics(orders_df, silver_customers),
                "dim_logistics",
            )
            fact_count = res_sales.records_count

        logger.info(
            "Gold task finished: dim_customers=%d, fact_sales=%d",
            res_dim_cust.records_count,
            fact_count,
        )
        return {
            "dim_customers": res_dim_cust.records_count,
            "fact_sales": fact_count,
        }
    finally:
        spark.stop()


def task_verify_pipeline(**kwargs) -> dict[str, str]:
    """Execute validation query across Gold layer ensuring non-empty state."""
    from src.data_platform.modeling.gold import GoldModeler
    from src.data_platform.storage.session import get_spark_session

    spark = get_spark_session(app_name="Airflow_VerifyPipeline")
    try:
        gold_engine = GoldModeler(spark)
        df = gold_engine.read_gold("dim_customers")
        count = df.count()
        assert count > 0, "dim_customers is unexpectedly empty!"
        logger.info("Verification passed: dim_customers has %d verified profiles.", count)
        return {"status": "PASSED", "count": str(count)}
    finally:
        spark.stop()


# ------------------------------------------------------------------------------
# DAG Definition
# ------------------------------------------------------------------------------

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="olist_delta_pipeline_dag",
        default_args=default_args,
        description="End-to-End Medallion Lakehouse with Delta Lake on Olist E-Commerce",
        schedule_interval="0 2 * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["lakehouse", "delta", "olist", "medallion", "production"],
    ) as dag:

        t_validate = PythonOperator(
            task_id="check_environment_and_sources",
            python_callable=task_validate_sources,
        )

        t_bronze = PythonOperator(
            task_id="ingest_raw_to_bronze",
            python_callable=task_ingest_bronze,
        )

        t_dqa = PythonOperator(
            task_id="evaluate_data_quality_and_quarantine",
            python_callable=task_dqa_and_quarantine,
        )

        t_silver = PythonOperator(
            task_id="transform_and_upsert_silver",
            python_callable=task_transform_silver,
        )

        t_gold = PythonOperator(
            task_id="build_gold_analytical_models",
            python_callable=task_build_gold_analytics,
        )

        t_verify = PythonOperator(
            task_id="verify_gold_quality_and_metrics",
            python_callable=task_verify_pipeline,
        )

        # Linear Medallion dependency graph with ACID guarantees
        t_validate >> t_bronze >> t_dqa >> t_silver >> t_gold >> t_verify
else:
    dag = None
