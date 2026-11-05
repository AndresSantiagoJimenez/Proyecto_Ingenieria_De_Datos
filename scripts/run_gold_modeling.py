"""CLI Script to execute Gold Layer Modeling (Star Schema & RFM) and Spark SQL queries."""

import argparse
import logging
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.data_platform.config import settings  # noqa: E402
from src.data_platform.modeling.gold import GoldModeler  # noqa: E402
from src.data_platform.storage.session import get_spark_session  # noqa: E402
from src.data_platform.transformations.silver import SilverTransformer  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run_gold_modeling")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build analytical star schema and RFM models in Gold Delta Lake."
    )
    parser.add_argument(
        "--use-sample",
        action="store_true",
        help="Use sample datasets for orders and payments if full raw tables are not present.",
    )
    _ = parser.parse_args()

    spark = get_spark_session(app_name="GoldModelingExecution")
    try:
        silver_engine = SilverTransformer(spark)
        gold_engine = GoldModeler(spark)

        # 1. Load Silver customers
        logger.info("Loading Silver customers Delta table...")
        silver_customers = silver_engine.read_silver("customers", base_dir=settings.silver_path)

        # 2. Build and save dim_customers
        dim_cust_result = gold_engine.save_gold_table(
            df=gold_engine.build_dim_customers(silver_customers),
            model_name="dim_customers",
            base_dir=settings.gold_path,
        )
        logger.info("dim_customers created: %d unique customer profiles", dim_cust_result.records_count)

        # 3. Load orders, items, and payments
        sample_dir = settings.sample_path
        orders_path = sample_dir / "olist_orders_sample.csv"
        items_path = sample_dir / "olist_order_items_sample.csv"
        payments_path = sample_dir / "olist_order_payments_sample.csv"

        if orders_path.exists() and items_path.exists() and payments_path.exists():
            logger.info("Loading orders, items, and payments for star schema modeling...")
            orders_df = spark.read.format("csv").option("header", "true").load(str(orders_path))
            items_df = spark.read.format("csv").option("header", "true").load(str(items_path))
            payments_df = spark.read.format("csv").option("header", "true").load(str(payments_path))

            # 4. Build fact_sales
            fact_sales_df = gold_engine.build_fact_sales(orders_df, items_df, payments_df)
            sales_result = gold_engine.save_gold_table(
                df=fact_sales_df,
                model_name="fact_sales",
                base_dir=settings.gold_path,
            )
            logger.info("fact_sales created: %d rows", sales_result.records_count)

            # 5. Build dim_customer_rfm
            rfm_df = gold_engine.build_dim_customer_rfm(silver_customers, orders_df, payments_df)
            rfm_result = gold_engine.save_gold_table(
                df=rfm_df,
                model_name="dim_customer_rfm",
                base_dir=settings.gold_path,
            )
            logger.info("dim_customer_rfm created: %d profiles", rfm_result.records_count)

            # 6. Build dim_logistics
            logistics_df = gold_engine.build_dim_logistics(orders_df, silver_customers)
            logistics_result = gold_engine.save_gold_table(
                df=logistics_df,
                model_name="dim_logistics",
                base_dir=settings.gold_path,
            )
            logger.info("dim_logistics created: %d deliveries", logistics_result.records_count)

            # 7. Execute Spark SQL Analytics
            print("\n" + "=" * 70)
            print("SPARK SQL ANALYTICS: Top 5 States by Total Customers")
            print("=" * 70)
            top_states_df = gold_engine.query_gold("""
                SELECT customer_state, COUNT(*) as total_customers
                FROM dim_customers
                GROUP BY customer_state
                ORDER BY total_customers DESC
                LIMIT 5
            """)
            top_states_df.show(truncate=False)

            print("\n" + "=" * 70)
            print("SPARK SQL ANALYTICS: Customer RFM Segmentation Distribution")
            print("=" * 70)
            rfm_summary_df = gold_engine.query_gold("""
                SELECT rfm_segment, COUNT(*) as count,
                       ROUND(AVG(monetary_value), 2) as avg_monetary,
                       ROUND(AVG(frequency), 2) as avg_frequency,
                       ROUND(AVG(recency_days), 1) as avg_recency_days
                FROM dim_customer_rfm
                GROUP BY rfm_segment
                ORDER BY count DESC
            """)
            rfm_summary_df.show(truncate=False)

            print("\n" + "=" * 70)
            print("SPARK SQL ANALYTICS: Fact Sales Summary")
            print("=" * 70)
            sales_summary_df = gold_engine.query_gold("""
                SELECT COUNT(DISTINCT order_id) as total_orders,
                       ROUND(SUM(price), 2) as total_merchandise_value,
                       ROUND(SUM(freight_value), 2) as total_freight_value,
                       ROUND(SUM(total_item_value), 2) as grand_total_revenue
                FROM fact_sales
            """)
            sales_summary_df.show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
