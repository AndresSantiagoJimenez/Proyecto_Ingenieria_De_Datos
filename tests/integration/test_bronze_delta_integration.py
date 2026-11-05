"""Integration test for Bronze Ingestion into Delta Lake format."""

from pathlib import Path

import pytest

from src.data_platform.config import settings
from src.data_platform.ingestion.bronze import BronzeIngestion
from src.data_platform.storage.session import get_spark_session


@pytest.fixture(scope="module")
def spark():
    """Module-level SparkSession for Delta integration testing."""
    session = get_spark_session(app_name="BronzeDeltaIntegrationTest", master="local[2]")
    yield session
    session.stop()


def test_bronze_ingestion_delta_table_end_to_end(spark, tmp_path: Path):
    """Test ingestion of sample customers CSV into a Delta Lake Bronze table."""
    sample_file = settings.sample_path / "olist_customers_sample.csv"
    assert sample_file.exists(), f"Sample file not found at {sample_file}"

    bronze_output_dir = tmp_path / "bronze"
    ingestion = BronzeIngestion(spark)

    # Ingest CSV into Delta format
    result = ingestion.ingest_csv(
        entity="customers",
        input_file=sample_file,
        output_dir=bronze_output_dir,
        write_mode="overwrite",
    )

    assert result.status == "SUCCESS"
    assert result.entity == "customers"
    assert result.records_count == 100
    assert result.batch_id is not None
    assert Path(result.target_path).exists()

    # Read back the Delta table
    delta_df = ingestion.read_bronze(entity="customers", base_dir=bronze_output_dir)

    # Assert schema & audit columns
    columns = delta_df.columns
    assert "_ingested_at" in columns
    assert "_batch_id" in columns
    assert "_source_filename" in columns
    assert "customer_id" in columns
    assert "customer_city" in columns

    # Assert row count in Delta table
    assert delta_df.count() == 100

    # Assert source filename value
    first_row = delta_df.select("_source_filename", "_batch_id").first()
    assert first_row["_source_filename"] == "olist_customers_sample.csv"
    assert first_row["_batch_id"] == result.batch_id
