"""Unit tests for Bronze layer ingestion logic."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.data_platform.ingestion.bronze import BronzeIngestion


def test_ingest_csv_raises_file_not_found():
    """Test that ingest_csv raises FileNotFoundError if input does not exist."""
    mock_spark = MagicMock()
    ingestion = BronzeIngestion(mock_spark)

    with pytest.raises(FileNotFoundError, match="Source file not found"):
        ingestion.ingest_csv(
            entity="customers",
            input_file=Path("/non/existent/file.csv"),
        )


def test_read_bronze_raises_file_not_found():
    """Test that read_bronze raises FileNotFoundError if table path does not exist."""
    mock_spark = MagicMock()
    ingestion = BronzeIngestion(mock_spark)

    with pytest.raises(FileNotFoundError, match="Bronze Delta table not found"):
        ingestion.read_bronze(
            entity="non_existent_entity",
            base_dir=Path("/non/existent/bronze"),
        )
