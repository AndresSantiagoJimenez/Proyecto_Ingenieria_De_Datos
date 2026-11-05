"""Unit tests for Silver layer transformations and cleansing logic."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.data_platform.storage.session import get_spark_session
from src.data_platform.transformations.silver import SilverTransformer


@pytest.fixture(scope="module")
def spark():
    """Module-level SparkSession for unit transformation testing."""
    session = get_spark_session(app_name="SilverUnitTransformationTest", master="local[2]")
    yield session
    session.stop()


def test_read_silver_raises_file_not_found():
    """Test that read_silver raises FileNotFoundError when target path is missing."""
    mock_spark = MagicMock()
    transformer = SilverTransformer(mock_spark)

    with pytest.raises(FileNotFoundError, match="Silver Delta table not found"):
        transformer.read_silver("customers", base_dir=Path("/non/existent/silver"))


def test_deduplicate_deterministic(spark):
    """Test that deduplicate keeps the newest record based on _ingested_at."""
    transformer = SilverTransformer(spark)

    data = [
        ("CUST1", "Sao Paulo", datetime(2023, 1, 1, 10, 0, 0)),
        ("CUST1", "Sao Paulo Updated", datetime(2023, 1, 2, 10, 0, 0)),  # Newer
        ("CUST2", "Curitiba", datetime(2023, 1, 1, 10, 0, 0)),
    ]
    df = spark.createDataFrame(data, ["customer_id", "customer_city", "_ingested_at"])

    deduped_df = transformer.deduplicate(df, primary_keys=["customer_id"])

    assert deduped_df.count() == 2
    cust1_row = deduped_df.filter(deduped_df.customer_id == "CUST1").first()
    assert cust1_row["customer_city"] == "Sao Paulo Updated"


def test_transform_customers_cleansing(spark):
    """Test customer field cleansing: zip padding, case normalization, whitespace trimming."""
    transformer = SilverTransformer(spark)

    data = [
        (
            " CUST_001 ",
            " UNIQ_001 ",
            " 151 ",       # Unpadded 3-digit zip code
            "  SAO PAULO  ",
            " sp ",
            datetime(2023, 1, 1, 10, 0, 0),
            "batch-1",
            "olist_customers.csv",
        )
    ]
    schema = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "_ingested_at",
        "_batch_id",
        "_source_filename",
    ]
    bronze_df = spark.createDataFrame(data, schema)

    cleansed_df = transformer.transform_customers(bronze_df)
    row = cleansed_df.first()

    assert row["customer_id"] == "CUST_001"
    assert row["customer_unique_id"] == "UNIQ_001"
    assert row["customer_zip_code_prefix"] == "00151"  # Zero-padded to 5 digits
    assert row["customer_city"] == "sao paulo"         # Lowercase
    assert row["customer_state"] == "SP"               # Uppercase
    assert "_silver_processed_at" in cleansed_df.columns
