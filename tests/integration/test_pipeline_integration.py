"""Integration test for full Medallion Pipeline execution."""

import pytest

from scripts.run_pipeline import run_pipeline
from src.data_platform.storage.session import get_spark_session


@pytest.fixture(scope="module")
def spark():
    """Module-level SparkSession for Pipeline integration testing."""
    session = get_spark_session(app_name="PipelineIntegrationTest", master="local[2]")
    yield session
    session.stop()


def test_full_medallion_pipeline_end_to_end(spark, tmp_path):
    """Test full execution: Bronze -> DQA/Quarantine -> Silver MERGE -> Gold Models."""
    test_lakehouse = tmp_path / "lakehouse"
    b_dir = test_lakehouse / "bronze"
    s_dir = test_lakehouse / "silver"
    g_dir = test_lakehouse / "gold"
    q_dir = test_lakehouse / "quarantine"

    summary = run_pipeline(
        entity="customers",
        use_sample=True,
        spark=spark,
        bronze_dir=b_dir,
        silver_dir=s_dir,
        gold_dir=g_dir,
        quarantine_dir=q_dir,
    )

    assert summary.status == "SUCCESS"
    assert summary.entity == "customers"
    assert summary.bronze_count == 100
    assert summary.quarantine_count == 0
    assert summary.silver_count == 100
    assert summary.gold_dim_count > 0
    assert summary.gold_fact_count == 5
    assert summary.completed_at >= summary.started_at
