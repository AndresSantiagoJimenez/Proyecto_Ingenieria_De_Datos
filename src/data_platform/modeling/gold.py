"""Gold Layer Dimensional and Analytical Modeling Engine using Delta Lake and Spark SQL."""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.data_platform.config import settings

logger = logging.getLogger(__name__)


@dataclass
class GoldResult:
    """Summary of a Gold layer model build operation."""

    model_name: str
    target_path: str
    records_count: int
    modeled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "SUCCESS"


class GoldModeler:
    """Builds star schema dimensional models, fact tables, and RFM analytics on Delta Lake."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def build_dim_customers(self, silver_customers_df: DataFrame) -> DataFrame:
        """Build enriched customer dimension aggregating unique customer profiles."""
        logger.info("Building Gold dim_customers table...")
        return (
            silver_customers_df.groupBy("customer_unique_id")
            .agg(
                F.first("customer_id").alias("sample_customer_id"),
                F.first("customer_zip_code_prefix").alias("customer_zip_code_prefix"),
                F.first("customer_city").alias("customer_city"),
                F.first("customer_state").alias("customer_state"),
                F.count("customer_id").alias("total_registered_accounts"),
            )
            .withColumn("_gold_processed_at", F.current_timestamp())
        )

    def build_dim_customer_rfm(
        self,
        customers_df: DataFrame,
        orders_df: DataFrame,
        payments_df: DataFrame,
        reference_date: datetime | str | None = None,
    ) -> DataFrame:
        """Compute Recency, Frequency, and Monetary (RFM) customer segmentation."""
        logger.info("Building Gold dim_customer_rfm segmentation...")

        # 1. Aggregate payments per order
        order_payments = payments_df.groupBy("order_id").agg(
            F.round(F.sum(F.col("payment_value").cast("double")), 2).alias("order_payment_total")
        )

        # 2. Join customers with orders and payments
        joined = (
            orders_df.join(customers_df, on="customer_id", how="inner")
            .join(order_payments, on="order_id", how="left")
            .withColumn(
                "order_payment_total",
                F.coalesce(F.col("order_payment_total"), F.lit(0.0)),
            )
        )

        # 3. Reference date for Recency calculation
        if reference_date is not None:
            ref_col = F.to_date(F.lit(reference_date))
        else:
            max_ts = joined.select(F.max("order_purchase_timestamp")).first()[0]
            ref_col = F.to_date(F.lit(max_ts) if max_ts else F.current_timestamp())

        # 4. Aggregate RFM metrics per customer_unique_id
        rfm_base = joined.groupBy("customer_unique_id").agg(
            F.max("order_purchase_timestamp").alias("last_order_timestamp"),
            F.countDistinct("order_id").alias("frequency"),
            F.round(F.sum("order_payment_total"), 2).alias("monetary_value"),
            F.first("customer_city").alias("customer_city"),
            F.first("customer_state").alias("customer_state"),
        )

        rfm_metrics = rfm_base.withColumn(
            "recency_days",
            F.datediff(ref_col, F.to_date("last_order_timestamp")),
        )

        # 5. Customer Segmentation Logic
        segmented = rfm_metrics.withColumn(
            "rfm_segment",
            F.when(
                (F.col("frequency") >= 2)
                & (F.col("monetary_value") >= 100.0)
                & (F.col("recency_days") <= 90),
                F.lit("Champions"),
            )
            .when(F.col("frequency") >= 2, F.lit("Loyal Customers"))
            .when(
                (F.col("frequency") == 1) & (F.col("recency_days") <= 90),
                F.lit("Recent Customers"),
            )
            .when(
                (F.col("frequency") == 1)
                & (F.col("recency_days") <= 180)
                & (F.col("monetary_value") >= 80.0),
                F.lit("Promising"),
            )
            .when(
                (F.col("recency_days") > 180) & (F.col("monetary_value") >= 100.0),
                F.lit("At Risk"),
            )
            .otherwise(F.lit("Hibernating")),
        ).withColumn("_gold_processed_at", F.current_timestamp())

        return segmented

    def build_fact_sales(
        self,
        orders_df: DataFrame,
        order_items_df: DataFrame,
        payments_df: DataFrame | None = None,
    ) -> DataFrame:
        """Construct the core analytical Fact Sales table."""
        logger.info("Building Gold fact_sales table...")

        # Base item-level sales join
        sales = orders_df.join(order_items_df, on="order_id", how="inner").select(
            "order_id",
            "order_item_id",
            "customer_id",
            "product_id",
            "seller_id",
            "order_status",
            F.to_timestamp("order_purchase_timestamp").alias("order_purchase_timestamp"),
            F.to_date("order_purchase_timestamp").alias("order_purchase_date"),
            F.col("price").cast("double").alias("price"),
            F.col("freight_value").cast("double").alias("freight_value"),
            F.round(
                F.col("price").cast("double") + F.col("freight_value").cast("double"),
                2,
            ).alias("total_item_value"),
        )

        if payments_df is not None:
            order_pay = payments_df.groupBy("order_id").agg(
                F.first("payment_type").alias("primary_payment_type"),
                F.max("payment_installments").cast("int").alias("max_installments"),
                F.round(F.sum(F.col("payment_value").cast("double")), 2).alias(
                    "order_total_payment"
                ),
            )
            sales = sales.join(order_pay, on="order_id", how="left")

        return sales.withColumn("_gold_processed_at", F.current_timestamp())

    def build_dim_logistics(
        self,
        orders_df: DataFrame,
        customers_df: DataFrame | None = None,
    ) -> DataFrame:
        """Build logistics delivery performance dimension."""
        logger.info("Building Gold dim_logistics table...")

        base = (
            orders_df.filter(F.col("order_status") == "delivered")
            .filter(F.col("order_delivered_customer_date").isNotNull())
            .select(
                "order_id",
                "customer_id",
                "order_status",
                F.to_date("order_purchase_timestamp").alias("purchase_date"),
                F.to_date("order_delivered_customer_date").alias("delivery_date"),
                F.to_date("order_estimated_delivery_date").alias("estimated_delivery_date"),
                F.datediff(
                    F.to_date("order_delivered_customer_date"),
                    F.to_date("order_purchase_timestamp"),
                ).alias("actual_delivery_days"),
                F.datediff(
                    F.to_date("order_estimated_delivery_date"),
                    F.to_date("order_purchase_timestamp"),
                ).alias("estimated_delivery_days"),
                F.datediff(
                    F.to_date("order_delivered_customer_date"),
                    F.to_date("order_estimated_delivery_date"),
                ).alias("delivery_delay_days"),
            )
            .withColumn(
                "is_delayed",
                F.when(F.col("delivery_delay_days") > 0, F.lit(1)).otherwise(F.lit(0)),
            )
        )

        if customers_df is not None:
            cust_geo = customers_df.select(
                "customer_id",
                "customer_city",
                "customer_state",
            )
            base = base.join(cust_geo, on="customer_id", how="left")

        return base.withColumn("_gold_processed_at", F.current_timestamp())

    def save_gold_table(
        self,
        df: DataFrame,
        model_name: str,
        base_dir: Path | str | None = None,
        mode: str = "overwrite",
    ) -> GoldResult:
        """Persist a Gold table into Delta Lake format and register a Spark SQL temp view."""
        target_base = Path(base_dir) if base_dir else settings.gold_path
        target_path = (target_base / model_name).resolve()
        target_path_str = str(target_path)

        logger.info("Saving Gold model '%s' to Delta at %s", model_name, target_path_str)

        (
            df.write.format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .save(target_path_str)
        )

        # Register or update Spark SQL view
        df.createOrReplaceTempView(model_name)
        count = df.count()

        logger.info("Gold model '%s' saved successfully with %d records.", model_name, count)

        return GoldResult(
            model_name=model_name,
            target_path=target_path_str,
            records_count=count,
            status="SUCCESS",
        )

    def read_gold(
        self,
        model_name: str,
        base_dir: Path | str | None = None,
        register_view: bool = True,
    ) -> DataFrame:
        """Read a Gold Delta table and optionally register as Spark SQL temp view."""
        target_base = Path(base_dir) if base_dir else settings.gold_path
        target_path = (target_base / model_name).resolve()

        if not target_path.exists():
            raise FileNotFoundError(f"Gold Delta table not found: {target_path}")

        df = self.spark.read.format("delta").load(str(target_path))
        if register_view:
            df.createOrReplaceTempView(model_name)
        return df

    def query_gold(self, sql_query: str) -> DataFrame:
        """Execute an analytical query via Spark SQL."""
        logger.info("Executing Spark SQL Gold query: %s", sql_query)
        return self.spark.sql(sql_query)
