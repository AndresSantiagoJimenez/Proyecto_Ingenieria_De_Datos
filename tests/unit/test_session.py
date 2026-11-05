"""Unit tests for Spark session builder."""

import sys
from unittest.mock import MagicMock, patch

import pytest

from src.data_platform.storage.session import get_spark_session


def test_get_spark_session_missing_pyspark():
    """Test that informative ImportError is raised when pyspark is missing."""
    with patch.dict(sys.modules, {"pyspark": None, "pyspark.sql": None}):
        with pytest.raises(ImportError, match="PySpark is not installed"):
            get_spark_session()


def test_get_spark_session_builder_configuration():
    """Test that SparkSession builder is invoked with Delta Lake extensions."""
    mock_spark = MagicMock()
    mock_builder = MagicMock()
    mock_spark.builder = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = "mock_session"

    mock_pyspark_sql = MagicMock()
    mock_pyspark_sql.SparkSession = mock_spark

    with patch.dict(
        sys.modules,
        {
            "pyspark": MagicMock(),
            "pyspark.sql": mock_pyspark_sql,
            "delta": None,
        },
    ):
        session = get_spark_session(app_name="TestApp", master="local[2]")
        assert session == "mock_session"
        mock_builder.appName.assert_called_with("TestApp")
        mock_builder.master.assert_called_with("local[2]")
        mock_builder.config.assert_any_call(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        mock_builder.config.assert_any_call(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
