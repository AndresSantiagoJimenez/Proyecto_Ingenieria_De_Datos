"""Unit tests for the Quality Gate validation script."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

from scripts.validate_quality_gate import (
    validate_coverage,
    validate_sonar_properties,
    validate_test_results,
)


def test_validate_sonar_properties_success(tmp_path: Path) -> None:
    sonar_file = tmp_path / "sonar-project.properties"
    sonar_file.write_text(
        "sonar.projectKey=test\nsonar.sources=src\nsonar.tests=tests\nsonar.python.coverage.reportPaths=cov.xml\n"
    )
    assert validate_sonar_properties(sonar_file) is True


def test_validate_sonar_properties_missing_keys(tmp_path: Path) -> None:
    sonar_file = tmp_path / "sonar-project.properties"
    sonar_file.write_text("sonar.projectKey=test\n")
    assert validate_sonar_properties(sonar_file) is False


def test_validate_sonar_properties_not_found(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing.properties"
    assert validate_sonar_properties(missing_file) is False


def test_validate_test_results_success(tmp_path: Path) -> None:
    results_file = tmp_path / "test-results.xml"
    root = ET.Element("testsuites", tests="10", failures="0", errors="0")
    ET.ElementTree(root).write(str(results_file))
    assert validate_test_results(results_file) is True


def test_validate_test_results_with_failure(tmp_path: Path) -> None:
    results_file = tmp_path / "test-results.xml"
    root = ET.Element("testsuites", tests="10", failures="1", errors="0")
    ET.ElementTree(root).write(str(results_file))
    assert validate_test_results(results_file) is False


def test_validate_coverage_success(tmp_path: Path) -> None:
    cov_file = tmp_path / "coverage.xml"
    root = ET.Element("coverage", {"line-rate": "0.95"})
    ET.ElementTree(root).write(str(cov_file))
    assert validate_coverage(cov_file, min_coverage=80.0) is True


def test_validate_coverage_below_threshold(tmp_path: Path) -> None:
    cov_file = tmp_path / "coverage.xml"
    root = ET.Element("coverage", {"line-rate": "0.75"})
    ET.ElementTree(root).write(str(cov_file))
    assert validate_coverage(cov_file, min_coverage=80.0) is False


def test_validate_coverage_missing_file(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing_cov.xml"
    assert validate_coverage(missing_file, min_coverage=80.0) is False
