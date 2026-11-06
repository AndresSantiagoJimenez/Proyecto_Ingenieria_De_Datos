"""Unit tests for the observability, logging and metrics modules."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from src.data_platform.observability.logging import JSONFormatter, get_structured_logger
from src.data_platform.observability.metrics import BatchMetricsTracker


def test_json_formatter_outputs_valid_json() -> None:
    formatter = JSONFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Processing stage batch",
        args=(),
        exc_info=None,
    )
    record.stage = "bronze_ingestion"
    record.batch_id = "batch-123"

    formatted = formatter.format(record)
    parsed = json.loads(formatted)

    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "test_logger"
    assert parsed["message"] == "Processing stage batch"
    assert parsed["stage"] == "bronze_ingestion"
    assert parsed["batch_id"] == "batch-123"
    assert "timestamp" in parsed


def test_get_structured_logger_factory() -> None:
    logger = get_structured_logger(name="test_custom_logger", level=logging.DEBUG, as_json=True)
    assert logger.name == "test_custom_logger"
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) >= 1


def test_batch_metrics_tracker_flow(tmp_path: Path) -> None:
    tracker = BatchMetricsTracker(batch_id="batch-test-001", output_dir=tmp_path)

    # 1. Start stage
    tracker.start_stage("bronze_ingestion", records_in=100)
    assert "bronze_ingestion" in tracker.stages
    assert tracker.stages["bronze_ingestion"].status == "RUNNING"

    # 2. End stage
    metric_stage = tracker.end_stage("bronze_ingestion", records_out=100, status="SUCCESS")
    assert metric_stage.status == "SUCCESS"
    assert metric_stage.records_out == 100
    assert metric_stage.duration_seconds >= 0.0

    # 3. Build summary
    summary = tracker.build_summary(
        status="SUCCESS",
        total_bronze=100,
        total_quarantined=5,
        total_silver=95,
        total_gold=95,
    )
    assert summary.batch_id == "batch-test-001"
    assert summary.total_bronze_records == 100
    assert summary.total_quarantined_records == 5
    assert summary.quarantine_rejection_rate_pct == 5.0
    assert summary.status == "SUCCESS"

    # 4. Persist
    metrics_file = tracker.persist(summary)
    assert metrics_file.exists()

    # 5. Read back
    latest = tracker.get_latest_metric()
    assert latest is not None
    assert latest["batch_id"] == "batch-test-001"
    assert latest["total_silver_records"] == 95
