"""Data Quality Assurance (DQA) validation engine."""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.data_platform.config import settings

logger = logging.getLogger(__name__)


@dataclass
class DQAResult:
    """Summary of DQA validation results for an entity."""

    entity: str
    batch_id: str
    total_evaluated: int
    valid_count: int
    quarantined_count: int
    reasons_summary: dict[str, int] = field(default_factory=dict)
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "SUCCESS"


class DataQualityAssurance:
    """Evaluates business and data integrity rules, isolating failing records."""

    def __init__(
        self,
        spark: SparkSession,
        rules_config: dict[str, Any] | None = None,
    ) -> None:
        self.spark = spark
        self.rules_config = rules_config or settings.load_quality_rules().get("rules", {})

    def get_entity_rules(self, entity: str) -> dict[str, Any]:
        """Retrieve quality rules configured for a given entity."""
        return self.rules_config.get(entity, {})

    def _build_rule_expressions(
        self,
        df_columns: list[str],
        entity_rules: dict[str, Any],
    ) -> list[tuple[Column, str]]:
        """Construct a list of (violation_condition, reason_message) pairs."""
        rule_specs: list[tuple[Column, str]] = []

        # 1. Not Null / Not Empty checks
        not_null_cols = entity_rules.get("not_null", [])
        for col_name in not_null_cols:
            if col_name in df_columns:
                cond = F.col(col_name).isNull() | (
                    F.trim(F.col(col_name).cast("string")) == ""
                )
                reason = f"NULL_OR_EMPTY: {col_name}"
                rule_specs.append((cond, reason))
            else:
                rule_specs.append((F.lit(True), f"MISSING_COLUMN: {col_name}"))

        # 2. String Length checks (exact length)
        string_length_rules = entity_rules.get("string_length", {})
        for col_name, expected_len in string_length_rules.items():
            if col_name in df_columns:
                cond = F.col(col_name).isNull() | (
                    F.length(F.trim(F.col(col_name).cast("string"))) != expected_len
                )
                reason = f"INVALID_STRING_LENGTH: {col_name} (expected {expected_len})"
                rule_specs.append((cond, reason))

        # 3. Positive numeric checks (val > 0)
        positive_numeric_cols = entity_rules.get("positive_numeric", [])
        for col_name in positive_numeric_cols:
            if col_name in df_columns:
                numeric_val = F.col(col_name).cast("double")
                cond = numeric_val.isNull() | (numeric_val <= 0)
                reason = f"NON_POSITIVE_NUMERIC: {col_name}"
                rule_specs.append((cond, reason))

        # 4. Valid statuses / allowed categorical values
        valid_statuses = entity_rules.get("valid_statuses")
        status_col = entity_rules.get("status_column", "order_status")
        if valid_statuses and status_col in df_columns:
            cond = F.col(status_col).isNull() | (~F.col(status_col).isin(valid_statuses))
            reason = f"INVALID_STATUS: {status_col}"
            rule_specs.append((cond, reason))

        return rule_specs

    def validate(
        self,
        df: DataFrame,
        entity: str,
        rules_override: dict[str, Any] | None = None,
    ) -> tuple[DataFrame, DataFrame, DQAResult]:
        """Validate DataFrame against quality rules, splitting into valid and quarantined sets.

        Returns:
            (valid_df, quarantine_df, result_summary)
        """
        entity_rules = (
            rules_override
            if rules_override is not None
            else self.get_entity_rules(entity)
        )
        batch_id = str(uuid.uuid4())
        total_count = df.count()

        logger.info(
            "Evaluating DQA rules for entity '%s' (batch=%s, total=%d)",
            entity,
            batch_id,
            total_count,
        )

        rule_specs = self._build_rule_expressions(df.columns, entity_rules)

        # Accumulate violation reasons
        reasons_col = F.array().cast("array<string>")
        for cond, reason in rule_specs:
            reasons_col = F.when(
                cond,
                F.concat(reasons_col, F.array(F.lit(reason))),
            ).otherwise(reasons_col)

        df_evaluated = df.withColumn("_quarantine_reasons", reasons_col)
        is_quarantined = F.size(F.col("_quarantine_reasons")) > 0

        # Valid DataFrame: keep clean data and drop internal reasons
        valid_df = df_evaluated.filter(~is_quarantined).drop("_quarantine_reasons")

        # Quarantine DataFrame: keep failed rows with audit columns
        quarantine_df = (
            df_evaluated.filter(is_quarantined)
            .withColumn("_quarantined_at", F.current_timestamp())
            .withColumn("_quarantine_batch_id", F.lit(batch_id))
            .withColumn("_quarantine_entity", F.lit(entity))
            .withColumn(
                "_quarantine_reasons_str",
                F.concat_ws("; ", F.col("_quarantine_reasons")),
            )
        )

        valid_count = valid_df.count()
        quarantined_count = quarantine_df.count()

        # Aggregate summary of reasons
        reasons_summary: dict[str, int] = {}
        if quarantined_count > 0:
            exploded = (
                quarantine_df.select(F.explode(F.col("_quarantine_reasons")).alias("reason"))
                .groupBy("reason")
                .count()
                .collect()
            )
            reasons_summary = {row["reason"]: row["count"] for row in exploded}

        logger.info(
            "DQA completed for '%s': total=%d, valid=%d, quarantined=%d",
            entity,
            total_count,
            valid_count,
            quarantined_count,
        )

        result = DQAResult(
            entity=entity,
            batch_id=batch_id,
            total_evaluated=total_count,
            valid_count=valid_count,
            quarantined_count=quarantined_count,
            reasons_summary=reasons_summary,
            status="SUCCESS",
        )

        return valid_df, quarantine_df, result
