"""Integration test for Silver layer Delta Lake MERGE INTO upserts."""

from datetime import datetime
from pathlib import Path

import pytest
from delta.tables import DeltaTable

from src.data_platform.storage.session import get_spark_session
from src.data_platform.transformations.silver import SilverTransformer


@pytest.fixture(scope="module")
def spark():
    """Module-level SparkSession for Delta MERGE integration testing."""
    session = get_spark_session(app_name="SilverDeltaMergeIntegrationTest", master="local[2]")
    yield session
    session.stop()


def test_silver_upsert_merge_into_end_to_end(spark, tmp_path: Path):
    """Test full cycle: initial Delta write followed by idempotent MERGE INTO update and insert."""
    transformer = SilverTransformer(spark)
    silver_output_dir = tmp_path / "silver"

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

    # Batch 1: Initial dataset with 2 customers
    batch1_data = [
        ("CUST_1", "UNIQ_1", "14409", "franca", "SP", datetime(2023, 1, 1, 10, 0), "b1", "f1.csv"),
        ("CUST_2", "UNIQ_2", "09790", "sao bernardo", "SP", datetime(2023, 1, 1, 10, 0), "b1", "f1.csv"),
    ]
    df_batch1 = spark.createDataFrame(batch1_data, schema)
    cleansed_b1 = transformer.transform_customers(df_batch1)

    # 1. First Upsert: Table initialization
    result1 = transformer.upsert_silver(
        df=cleansed_b1,
        entity="customers",
        primary_keys=["customer_id"],
        output_dir=silver_output_dir,
    )

    assert result1.status == "SUCCESS"
    assert result1.records_count == 2

    # Verify table content
    silver_df = transformer.read_silver(entity="customers", base_dir=silver_output_dir)
    assert silver_df.count() == 2

    # Batch 2: 1 Updated customer (CUST_1 new city) + 1 New customer (CUST_3)
    batch2_data = [
        ("CUST_1", "UNIQ_1", "14409", "campinas", "SP", datetime(2023, 1, 2, 10, 0), "b2", "f2.csv"),
        ("CUST_3", "UNIQ_3", "01151", "sao paulo", "SP", datetime(2023, 1, 2, 10, 0), "b2", "f2.csv"),
    ]
    df_batch2 = spark.createDataFrame(batch2_data, schema)
    cleansed_b2 = transformer.transform_customers(df_batch2)

    # 2. Second Upsert: MERGE INTO
    result2 = transformer.upsert_silver(
        df=cleansed_b2,
        entity="customers",
        primary_keys=["customer_id"],
        output_dir=silver_output_dir,
    )

    assert result2.status == "SUCCESS"
    assert result2.records_count == 3  # 2 original - 1 updated + 1 new = 3 unique

    # Read back table and assert MERGE INTO effects
    updated_silver_df = transformer.read_silver(entity="customers", base_dir=silver_output_dir)
    assert updated_silver_df.count() == 3

    # Assert that CUST_1 was updated in-place
    cust1 = updated_silver_df.filter(updated_silver_df.customer_id == "CUST_1").first()
    assert cust1["customer_city"] == "campinas"

    # Assert that CUST_3 was inserted
    cust3 = updated_silver_df.filter(updated_silver_df.customer_id == "CUST_3").first()
    assert cust3 is not None
    assert cust3["customer_city"] == "sao paulo"

    # Assert Delta Lake transaction history has 2 versions
    delta_table = DeltaTable.forPath(spark, str(tmp_path / "silver" / "customers"))
    history_df = delta_table.history()
    assert history_df.count() >= 2
