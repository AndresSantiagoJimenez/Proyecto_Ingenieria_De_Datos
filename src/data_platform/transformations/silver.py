"""Silver Layer Transformation and Cleansing Engine using Delta Lake MERGE INTO."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.data_platform.config import settings

logger = logging.getLogger(__name__)


@dataclass
class SilverResult:
    """Summary of a Silver layer transformation and upsert operation."""

    entity: str
    target_path: str
    records_count: int
    processed_at: datetime
    status: str


class SilverTransformer:
    """Handles deterministic deduplication, type casting, cleansing and Delta Lake MERGE INTO."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @staticmethod
    def deduplicate(
        df: DataFrame,
        primary_keys: list[str],
        order_by_col: str = "_ingested_at",
    ) -> DataFrame:
        """Deterministically deduplicate a DataFrame keeping the most recently ingested record."""
        window_spec = Window.partitionBy(*[F.col(k) for k in primary_keys]).orderBy(
            F.col(order_by_col).desc()
        )
        return (
            df.withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    def transform_customers(self, bronze_df: DataFrame) -> DataFrame:
        """Cleanse and standardize the Bronze customers dataset."""
        logger.info("Transforming customers dataset for Silver layer...")

        # 1. Deterministic deduplication by primary key
        deduped_df = self.deduplicate(bronze_df, primary_keys=["customer_id"])

        # 2. Type casting, string normalization, and zipcode padding
        cleansed_df = (
            deduped_df.withColumn("customer_id", F.trim(F.col("customer_id")))
            .withColumn("customer_unique_id", F.trim(F.col("customer_unique_id")))
            .withColumn(
                "customer_zip_code_prefix",
                F.lpad(F.trim(F.col("customer_zip_code_prefix")), 5, "0"),
            )
            .withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
            .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
            .withColumn("_silver_processed_at", F.current_timestamp())
        )

        return cleansed_df

    def upsert_silver(
        self,
        df: DataFrame,
        entity: str,
        primary_keys: list[str],
        output_dir: Path | str | None = None,
    ) -> SilverResult:
        """Upsert a cleansed DataFrame into the Silver Delta table using MERGE INTO."""
        target_base = Path(output_dir) if output_dir else settings.silver_path
        target_path = (target_base / entity).resolve()
        target_path_str = str(target_path)
        processed_at = datetime.now(timezone.utc)

        logger.info(
            "Performing Delta Lake upsert for entity '%s' at '%s' on keys: %s",
            entity,
            target_path_str,
            primary_keys,
        )

        table_exists = (
            target_path.exists() and DeltaTable.isDeltaTable(self.spark, target_path_str)
        )

        if table_exists:
            logger.info("Existing Delta table found at %s. Executing MERGE INTO...", target_path_str)
            delta_target = DeltaTable.forPath(self.spark, target_path_str)

            # Build merge condition from primary keys
            merge_condition = " AND ".join(
                [f"target.{k} = source.{k}" for k in primary_keys]
            )

            (
                delta_target.alias("target")
                .merge(source=df.alias("source"), condition=merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            logger.info("Initializing new Delta table at %s...", target_path_str)
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .save(target_path_str)
            )

        # Read back table to obtain authoritative record count
        result_df = self.spark.read.format("delta").load(target_path_str)
        count = result_df.count()

        logger.info(
            "Silver upsert completed for entity '%s'. Total records in Silver: %d",
            entity,
            count,
        )

        return SilverResult(
            entity=entity,
            target_path=target_path_str,
            records_count=count,
            processed_at=processed_at,
            status="SUCCESS",
        )

    def read_silver(self, entity: str, base_dir: Path | str | None = None) -> DataFrame:
        """Read a Silver Delta table as a PySpark DataFrame."""
        target_base = Path(base_dir) if base_dir else settings.silver_path
        target_path = (target_base / entity).resolve()

        if not target_path.exists():
            raise FileNotFoundError(f"Silver Delta table not found: {target_path}")

        return self.spark.read.format("delta").load(str(target_path))
