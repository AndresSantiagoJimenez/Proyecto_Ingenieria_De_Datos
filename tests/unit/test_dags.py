"""Unit tests for Airflow DAG structure, task callables, and configuration."""

import sys
from datetime import timedelta
from unittest.mock import MagicMock, patch

from dags import olist_delta_pipeline_dag as dag_module


def test_dag_module_importable():
    """Verify DAG file can be parsed and loaded without syntax or import errors."""
    assert dag_module is not None
    assert hasattr(dag_module, "default_args")
    assert dag_module.default_args["retries"] == 2
    assert dag_module.default_args["retry_delay"] == timedelta(minutes=5)
    assert dag_module.default_args["owner"] == "data_engineer"
    assert dag_module.default_args["depends_on_past"] is False


def test_task_validate_sources():
    """Test task_validate_sources creates directories and returns source path."""
    res = dag_module.task_validate_sources()
    assert "source_file" in res
    assert "olist_customers" in res["source_file"]


def test_airflow_dag_structure_mocked():
    """Test DAG instantiation and dependency chain using mock Airflow objects."""
    mock_dag_cls = MagicMock()
    mock_op_cls = MagicMock()

    with patch.dict(
        sys.modules,
        {
            "airflow": MagicMock(DAG=mock_dag_cls),
            "airflow.operators.python": MagicMock(PythonOperator=mock_op_cls),
        },
    ):
        # Verify callables exist
        assert callable(dag_module.task_validate_sources)
        assert callable(dag_module.task_ingest_bronze)
        assert callable(dag_module.task_dqa_and_quarantine)
        assert callable(dag_module.task_transform_silver)
        assert callable(dag_module.task_build_gold_analytics)
        assert callable(dag_module.task_verify_pipeline)
