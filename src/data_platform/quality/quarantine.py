"""Quarantine isolation engine for Delta Lake."""

import logging
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.data_platform.config import settings

logger = logging.getLogger(__name__)


class QuarantineManager:
    """Manages rejected and non-compliant records storage in Delta Lake Quarantine tables."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def save_to_quarantine(
        self,
        quarantine_df: DataFrame,
        entity: str,
        base_dir: Path | str | None = None,
    ) -> int:
        """Persist quarantined records into Delta Lake format.

        Returns:
            Number of records persisted to quarantine.
        """
        count = quarantine_df.count()
        if count == 0:
            logger.info("No records to quarantine for entity '%s'.", entity)
            return 0

        target_base = Path(base_dir) if base_dir else settings.quarantine_path
        target_path = (target_base / entity).resolve()
        target_path_str = str(target_path)

        logger.warning(
            "Isolating %d non-compliant records for entity '%s' in Delta Quarantine at %s",
            count,
            entity,
            target_path_str,
        )

        (
            quarantine_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(target_path_str)
        )

        return count

    def read_quarantine(
        self,
        entity: str,
        base_dir: Path | str | None = None,
    ) -> DataFrame:
        """Read quarantined Delta table as a PySpark DataFrame."""
        target_base = Path(base_dir) if base_dir else settings.quarantine_path
        target_path = (target_base / entity).resolve()

        if not target_path.exists():
            raise FileNotFoundError(f"Quarantine Delta table not found: {target_path}")

        return self.spark.read.format("delta").load(str(target_path))

    def get_quarantine_metrics(
        self,
        entity: str,
        base_dir: Path | str | None = None,
    ) -> dict[str, Any]:
        """Aggregate summary metrics of quarantined records for an entity."""
        try:
            df = self.read_quarantine(entity=entity, base_dir=base_dir)
        except FileNotFoundError:
            return {
                "entity": entity,
                "total_quarantined": 0,
                "distinct_batches": 0,
                "reasons_breakdown": {},
            }

        total = df.count()
        distinct_batches = (
            df.select("_quarantine_batch_id").distinct().count()
            if "_quarantine_batch_id" in df.columns
            else 0
        )

        reasons_breakdown: dict[str, int] = {}
        if "_quarantine_reasons" in df.columns and total > 0:
            exploded = (
                df.select(F.explode(F.col("_quarantine_reasons")).alias("reason"))
                .groupBy("reason")
                .count()
                .collect()
            )
            reasons_breakdown = {row["reason"]: row["count"] for row in exploded}

        return {
            "entity": entity,
            "total_quarantined": total,
            "distinct_batches": distinct_batches,
            "reasons_breakdown": reasons_breakdown,
        }
