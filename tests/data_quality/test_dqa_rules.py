"""Unit tests for Data Quality Assurance (DQA) validation engine."""

from unittest.mock import MagicMock

import pytest

from src.data_platform.quality.dqa import DataQualityAssurance
from src.data_platform.storage.session import get_spark_session


@pytest.fixture(scope="module")
def spark():
    """Module-level SparkSession for DQA rule unit tests."""
    session = get_spark_session(app_name="DQAUnitTest", master="local[2]")
    yield session
    session.stop()


def test_dqa_not_null_rule(spark):
    """Test that rows with null or empty values in not_null columns are flagged."""
    dqa = DataQualityAssurance(spark)
    rules = {
        "not_null": ["customer_id", "customer_city"],
    }

    data = [
        ("C1", "sao paulo"),
        (None, "curitiba"),         # Null customer_id
        ("C3", "   "),              # Empty string customer_city
        ("C4", "rio de janeiro"),
    ]
    df = spark.createDataFrame(data, ["customer_id", "customer_city"])

    valid_df, quarantine_df, result = dqa.validate(df, entity="customers", rules_override=rules)

    assert result.total_evaluated == 4
    assert result.valid_count == 2
    assert result.quarantined_count == 2
    assert "NULL_OR_EMPTY: customer_id" in result.reasons_summary
    assert "NULL_OR_EMPTY: customer_city" in result.reasons_summary


def test_dqa_string_length_rule(spark):
    """Test that rows violating exact string length constraints are quarantined."""
    dqa = DataQualityAssurance(spark)
    rules = {
        "string_length": {
            "customer_state": 2,
            "customer_zip_code_prefix": 5,
        }
    }

    data = [
        ("SP", "01151"),   # Valid
        ("SPA", "01151"),  # State too long (3 chars)
        ("S", "01151"),    # State too short (1 char)
        ("RJ", "123"),     # Zip too short (3 chars)
    ]
    df = spark.createDataFrame(data, ["customer_state", "customer_zip_code_prefix"])

    valid_df, quarantine_df, result = dqa.validate(df, entity="customers", rules_override=rules)

    assert result.total_evaluated == 4
    assert result.valid_count == 1
    assert result.quarantined_count == 3


def test_dqa_positive_numeric_rule(spark):
    """Test that negative, zero, or null numerical values are quarantined."""
    dqa = DataQualityAssurance(spark)
    rules = {
        "positive_numeric": ["price", "freight_value"],
    }

    data = [
        (100.0, 15.5),   # Valid
        (-10.0, 5.0),    # Negative price
        (0.0, 10.0),     # Zero price
        (50.0, -1.0),    # Negative freight
    ]
    df = spark.createDataFrame(data, ["price", "freight_value"])

    valid_df, quarantine_df, result = dqa.validate(df, entity="order_items", rules_override=rules)

    assert result.total_evaluated == 4
    assert result.valid_count == 1
    assert result.quarantined_count == 3


def test_dqa_valid_statuses_rule(spark):
    """Test that unexpected categorical status values are quarantined."""
    dqa = DataQualityAssurance(spark)
    rules = {
        "status_column": "order_status",
        "valid_statuses": ["delivered", "shipped", "canceled"],
    }

    data = [
        ("O1", "delivered"),
        ("O2", "unknown_status"),
        ("O3", "shipped"),
    ]
    df = spark.createDataFrame(data, ["order_id", "order_status"])

    valid_df, quarantine_df, result = dqa.validate(df, entity="orders", rules_override=rules)

    assert result.total_evaluated == 3
    assert result.valid_count == 2
    assert result.quarantined_count == 1
    assert "INVALID_STATUS: order_status" in result.reasons_summary


def test_dqa_missing_column_flagged(spark):
    """Test that if a required not_null column is missing entirely from the schema, it flags MISSING_COLUMN."""
    dqa = DataQualityAssurance(spark)
    rules = {
        "not_null": ["missing_column_abc"],
    }

    data = [("value1",), ("value2",)]
    df = spark.createDataFrame(data, ["existing_col"])

    valid_df, quarantine_df, result = dqa.validate(df, entity="test_entity", rules_override=rules)

    assert result.total_evaluated == 2
    assert result.valid_count == 0
    assert result.quarantined_count == 2
    assert "MISSING_COLUMN: missing_column_abc" in result.reasons_summary


def test_dqa_get_entity_rules():
    """Test retrieving configured rules from rules_config dictionary."""
    mock_spark = MagicMock()
    custom_rules = {"orders": {"primary_key": "order_id"}}
    dqa = DataQualityAssurance(mock_spark, rules_config=custom_rules)

    assert dqa.get_entity_rules("orders") == {"primary_key": "order_id"}
    assert dqa.get_entity_rules("unknown_entity") == {}
