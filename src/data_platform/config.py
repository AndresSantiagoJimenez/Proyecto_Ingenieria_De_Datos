"""Configuration management for Data Engineering Platform."""

import os
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore


class Settings:
    """Application settings loaded from environment and YAML configuration."""

    def __init__(self, config_path: Path | None = None) -> None:
        self.root_dir = Path(__file__).resolve().parent.parent.parent
        self.config_path = config_path or (self.root_dir / "configs" / "app.yaml")
        self.env = os.getenv("ENVIRONMENT", "development")
        self.config_data = self._load_yaml()

    def _load_yaml(self) -> dict[str, Any]:
        """Load YAML configuration safely."""
        if yaml is None:
            return {}
        if not self.config_path.exists():
            return {}
        with open(self.config_path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    @property
    def quality_rules_path(self) -> Path:
        return self.root_dir / "configs" / "quality_rules.yaml"

    def load_quality_rules(self) -> dict[str, Any]:
        """Load Data Quality rules from YAML."""
        if yaml is None or not self.quality_rules_path.exists():
            return {}
        with open(self.quality_rules_path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    @property
    def data_dir(self) -> Path:
        """Base directory for lakehouse data."""
        configured_path = os.getenv("DATA_DIR")
        if configured_path:
            return Path(configured_path)
        return self.root_dir / "data"

    @property
    def raw_path(self) -> Path:
        return Path(os.getenv("RAW_DATA_PATH", self.data_dir / "raw"))

    @property
    def sample_path(self) -> Path:
        return Path(os.getenv("SAMPLE_DATA_PATH", self.data_dir / "sample"))

    @property
    def bronze_path(self) -> Path:
        return Path(os.getenv("BRONZE_PATH", self.data_dir / "bronze"))

    @property
    def silver_path(self) -> Path:
        return Path(os.getenv("SILVER_PATH", self.data_dir / "silver"))

    @property
    def gold_path(self) -> Path:
        return Path(os.getenv("GOLD_PATH", self.data_dir / "gold"))

    @property
    def quarantine_path(self) -> Path:
        return Path(os.getenv("QUARANTINE_PATH", self.data_dir / "quarantine"))


# Global singleton settings instance
settings = Settings()
