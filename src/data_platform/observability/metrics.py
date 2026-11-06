"""Batch metrics collection, tracking and persistence for Lakehouse Medallion pipeline."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class StageMetric:
    """Represents performance and volume metrics for a specific pipeline stage."""

    stage_name: str
    status: str = "PENDING"
    start_time: str = ""
    end_time: str = ""
    duration_seconds: float = 0.0
    records_in: int = 0
    records_out: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineExecutionMetric:
    """Consolidated execution metric for an entire end-to-end pipeline run."""

    batch_id: str
    status: str
    start_time: str
    end_time: str = ""
    total_duration_seconds: float = 0.0
    total_bronze_records: int = 0
    total_quarantined_records: int = 0
    total_silver_records: int = 0
    total_gold_records: int = 0
    quarantine_rejection_rate_pct: float = 0.0
    stages: dict[str, StageMetric] = field(default_factory=dict)


class BatchMetricsTracker:
    """Collects, computes, and persists batch execution metrics for Lakehouse operations."""

    def __init__(self, batch_id: str, output_dir: str | Path | None = None) -> None:
        self.batch_id = batch_id
        self.start_dt = datetime.now(timezone.utc)
        self.start_time_iso = self.start_dt.isoformat()

        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            self.output_dir = Path("data") / "lakehouse" / "metrics"

        self.stages: dict[str, StageMetric] = {}
        self._stage_start_times: dict[str, datetime] = {}

    def start_stage(self, stage_name: str, records_in: int = 0) -> None:
        """Marks the start of a pipeline stage."""
        now = datetime.now(timezone.utc)
        self._stage_start_times[stage_name] = now
        self.stages[stage_name] = StageMetric(
            stage_name=stage_name,
            status="RUNNING",
            start_time=now.isoformat(),
            records_in=records_in,
        )

    def end_stage(
        self,
        stage_name: str,
        records_out: int = 0,
        status: str = "SUCCESS",
        metadata: dict[str, Any] | None = None,
    ) -> StageMetric:
        """Marks the completion of a pipeline stage and records metrics."""
        now = datetime.now(timezone.utc)
        start_time = self._stage_start_times.get(stage_name, now)
        duration = round((now - start_time).total_seconds(), 3)

        metric = self.stages.get(stage_name)
        if not metric:
            metric = StageMetric(stage_name=stage_name)
            self.stages[stage_name] = metric

        metric.status = status
        metric.end_time = now.isoformat()
        metric.duration_seconds = duration
        metric.records_out = records_out
        if metadata:
            metric.metadata.update(metadata)

        return metric

    def build_summary(
        self,
        status: str = "SUCCESS",
        total_bronze: int = 0,
        total_quarantined: int = 0,
        total_silver: int = 0,
        total_gold: int = 0,
    ) -> PipelineExecutionMetric:
        """Builds the final consolidated pipeline execution summary."""
        end_dt = datetime.now(timezone.utc)
        total_duration = round((end_dt - self.start_dt).total_seconds(), 3)

        rejection_rate = 0.0
        if total_bronze > 0:
            rejection_rate = round((total_quarantined / total_bronze) * 100, 2)

        return PipelineExecutionMetric(
            batch_id=self.batch_id,
            status=status,
            start_time=self.start_time_iso,
            end_time=end_dt.isoformat(),
            total_duration_seconds=total_duration,
            total_bronze_records=total_bronze,
            total_quarantined_records=total_quarantined,
            total_silver_records=total_silver,
            total_gold_records=total_gold,
            quarantine_rejection_rate_pct=rejection_rate,
            stages=self.stages,
        )

    def persist(self, metric: PipelineExecutionMetric) -> Path:
        """Appends the metric as a JSON line into the metrics log file.

        Args:
            metric: The consolidated metric to persist.

        Returns:
            The Path of the metrics file.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        metrics_file = self.output_dir / "pipeline_metrics.jsonl"

        payload = asdict(metric)
        with open(metrics_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")

        return metrics_file

    def get_latest_metric(self) -> dict[str, Any] | None:
        """Reads and returns the last persisted metric record, or None if empty."""
        metrics_file = self.output_dir / "pipeline_metrics.jsonl"
        if not metrics_file.exists():
            return None

        lines = metrics_file.read_text(encoding="utf-8").strip().splitlines()
        if not lines:
            return None

        return json.loads(lines[-1])
