"""Bronze Layer Ingestion Engine for Olist Lakehouse."""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.data_platform.config import settings

logger = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    """Summary of a Bronze layer ingestion operation."""

    entity: str
    batch_id: str
    source_path: str
    target_path: str
    records_count: int
    ingested_at: datetime
    status: str


class BronzeIngestion:
    """Manages raw dataset loading and persistence into Delta Lake Bronze layer."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def add_audit_metadata(
        self,
        df: DataFrame,
        batch_id: str,
        source_filename: str,
    ) -> DataFrame:
        """Append standard Bronze audit lineage columns."""
        return (
            df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_source_filename", F.lit(source_filename))
        )

    def ingest_csv(
        self,
        entity: str,
        input_file: Path | str,
        output_dir: Path | str | None = None,
        write_mode: str = "append",
    ) -> IngestionResult:
        """Ingest a raw CSV file into a Bronze Delta Lake table with audit metadata."""
        input_path = Path(input_file).resolve()
        if not input_path.exists():
            raise FileNotFoundError(f"Source file not found: {input_path}")

        target_base = Path(output_dir) if output_dir else settings.bronze_path
        target_path = (target_base / entity).resolve()
        batch_id = str(uuid.uuid4())
        source_filename = input_path.name
        ingestion_time = datetime.now(timezone.utc)

        logger.info(
            "Ingesting %s from %s into Delta Bronze at %s [batch=%s]",
            entity,
            input_path,
            target_path,
            batch_id,
        )

        # Read raw CSV with quoted strings and multiline handling
        raw_df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")  # Preserve raw string fidelity in Bronze
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", "true")
            .load(str(input_path))
        )

        # Add audit metadata columns
        bronze_df = self.add_audit_metadata(
            raw_df, batch_id=batch_id, source_filename=source_filename
        )

        # Write to Delta table
        (
            bronze_df.write.format("delta")
            .mode(write_mode)
            .option("mergeSchema", "true")
            .save(str(target_path))
        )

        count = bronze_df.count()
        logger.info(
            "Successfully ingested %d records for entity '%s' [batch=%s]",
            count,
            entity,
            batch_id,
        )

        return IngestionResult(
            entity=entity,
            batch_id=batch_id,
            source_path=str(input_path),
            target_path=str(target_path),
            records_count=count,
            ingested_at=ingestion_time,
            status="SUCCESS",
        )

    def read_bronze(self, entity: str, base_dir: Path | str | None = None) -> DataFrame:
        """Read a Bronze Delta table as a PySpark DataFrame."""
        target_base = Path(base_dir) if base_dir else settings.bronze_path
        target_path = (target_base / entity).resolve()

        if not target_path.exists():
            raise FileNotFoundError(f"Bronze Delta table not found: {target_path}")

        return self.spark.read.format("delta").load(str(target_path))
