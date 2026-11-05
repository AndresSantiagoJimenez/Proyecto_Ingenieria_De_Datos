"""Integration tests for Quarantine Delta Lake storage and isolation."""

from pathlib import Path

import pytest

from src.data_platform.quality.dqa import DataQualityAssurance
from src.data_platform.quality.quarantine import QuarantineManager
from src.data_platform.storage.session import get_spark_session


@pytest.fixture(scope="module")
def spark():
    """Module-level SparkSession for Delta Quarantine integration testing."""
    session = get_spark_session(app_name="QuarantineDeltaIntegrationTest", master="local[2]")
    yield session
    session.stop()


def test_quarantine_read_missing_table_raises_error(spark, tmp_path: Path):
    """Test read_quarantine raises FileNotFoundError when quarantine path does not exist."""
    quarantine_mgr = QuarantineManager(spark)
    with pytest.raises(FileNotFoundError, match="Quarantine Delta table not found"):
        quarantine_mgr.read_quarantine("non_existent", base_dir=tmp_path / "quarantine")


def test_quarantine_metrics_empty_when_table_missing(spark, tmp_path: Path):
    """Test metrics returns zeros when quarantine table does not exist."""
    quarantine_mgr = QuarantineManager(spark)
    metrics = quarantine_mgr.get_quarantine_metrics("non_existent", base_dir=tmp_path / "quarantine")
    assert metrics["total_quarantined"] == 0
    assert metrics["distinct_batches"] == 0
    assert metrics["reasons_breakdown"] == {}


def test_quarantine_delta_table_end_to_end(spark, tmp_path: Path):
    """Test full cycle: DQA validation, Delta Quarantine table creation, and metrics query."""
    quarantine_dir = tmp_path / "quarantine"
    dqa = DataQualityAssurance(spark)
    quarantine_mgr = QuarantineManager(spark)

    rules = {
        "not_null": ["customer_id", "customer_state"],
        "string_length": {"customer_state": 2},
    }

    # 4 records: 2 valid, 2 invalid
    test_data = [
        ("C1", "SP"),          # Valid
        ("C2", "RJ"),          # Valid
        (None, "MG"),          # Null PK
        ("C4", "MINAS_GERAIS") # Invalid state length
    ]
    df = spark.createDataFrame(test_data, ["customer_id", "customer_state"])

    # 1. Validate
    valid_df, quarantine_df, result = dqa.validate(df, entity="customers", rules_override=rules)

    assert result.total_evaluated == 4
    assert result.valid_count == 2
    assert result.quarantined_count == 2

    # 2. Save quarantined rows to Delta Lake
    saved_count = quarantine_mgr.save_to_quarantine(
        quarantine_df=quarantine_df,
        entity="customers",
        base_dir=quarantine_dir,
    )
    assert saved_count == 2

    # 3. Read back from Delta Lake
    quarantined_delta_df = quarantine_mgr.read_quarantine(
        entity="customers",
        base_dir=quarantine_dir,
    )
    assert quarantined_delta_df.count() == 2

    columns = quarantined_delta_df.columns
    assert "_quarantined_at" in columns
    assert "_quarantine_batch_id" in columns
    assert "_quarantine_reasons" in columns
    assert "_quarantine_reasons_str" in columns
    assert "_quarantine_entity" in columns

    # 4. Verify Quarantine Metrics
    metrics = quarantine_mgr.get_quarantine_metrics(
        entity="customers",
        base_dir=quarantine_dir,
    )
    assert metrics["total_quarantined"] == 2
    assert metrics["distinct_batches"] == 1
    assert "NULL_OR_EMPTY: customer_id" in metrics["reasons_breakdown"]
    assert "INVALID_STRING_LENGTH: customer_state (expected 2)" in metrics["reasons_breakdown"]

    # 5. Saving empty quarantine DataFrame returns 0 safely
    empty_df = df.filter("1 = 0")
    assert quarantine_mgr.save_to_quarantine(empty_df, "customers", base_dir=quarantine_dir) == 0
