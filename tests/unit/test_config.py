"""Unit tests for configuration management."""

from pathlib import Path

from src.data_platform.config import Settings


def test_settings_initialization(project_root: Path):
    """Test settings load default paths and environment values."""
    settings = Settings()
    assert settings.root_dir.resolve() == project_root.resolve()
    assert settings.env in ["development", "production", "test"]
    assert isinstance(settings.raw_path, Path)
    assert isinstance(settings.sample_path, Path)
    assert isinstance(settings.bronze_path, Path)
    assert isinstance(settings.silver_path, Path)
    assert isinstance(settings.gold_path, Path)
    assert isinstance(settings.quarantine_path, Path)
    assert isinstance(settings.quality_rules_path, Path)


def test_settings_load_yaml_config():
    """Test YAML config loading and fallback to empty dict for non-existent path."""
    non_existent = Path("/non/existent/config.yaml")
    custom_settings = Settings(config_path=non_existent)
    assert custom_settings._load_yaml() == {}


def test_settings_load_quality_rules():
    """Test loading quality rules from existing YAML specification."""
    settings = Settings()
    rules = settings.load_quality_rules()
    assert isinstance(rules, dict)
    assert "rules" in rules
    assert "customers" in rules["rules"]
    assert "orders" in rules["rules"]
