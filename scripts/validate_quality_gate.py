#!/usr/bin/env python3
"""Quality Gate Local Validator.

This script audits the generated test reports (coverage.xml and test-results.xml)
against enterprise Quality Gate thresholds:
- Minimum code coverage percentage (default: 80.0%)
- Zero test failures and zero test errors in test execution report
- Verifies that SonarQube configuration file exists and contains required properties.

Usage:
    python scripts/validate_quality_gate.py [--min-coverage 80.0]
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate local Quality Gate metrics.")
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=80.0,
        help="Minimum required coverage percentage (default: 80.0)",
    )
    parser.add_argument(
        "--coverage-file",
        type=str,
        default="coverage.xml",
        help="Path to coverage.xml report file",
    )
    parser.add_argument(
        "--test-results-file",
        type=str,
        default="test-results.xml",
        help="Path to test-results.xml JUnit report file",
    )
    parser.add_argument(
        "--sonar-properties-file",
        type=str,
        default="sonar-project.properties",
        help="Path to sonar-project.properties file",
    )
    return parser.parse_args()


def validate_sonar_properties(filepath: Path) -> bool:
    """Verifies that sonar-project.properties exists and has essential keys."""
    if not filepath.exists():
        print(f"[FAIL] SonarQube properties file '{filepath}' does not exist.")
        return False

    content = filepath.read_text(encoding="utf-8")
    required_keys = [
        "sonar.projectKey",
        "sonar.sources",
        "sonar.tests",
        "sonar.python.coverage.reportPaths",
    ]
    missing = [key for key in required_keys if key not in content]
    if missing:
        print(f"[FAIL] Missing required Sonar properties: {missing}")
        return False

    print(f"[PASS] SonarQube properties valid in '{filepath}'.")
    return True


def validate_test_results(filepath: Path) -> bool:
    """Verifies test-results.xml has 0 failures and 0 errors."""
    if not filepath.exists():
        print(f"[WARN] Test results file '{filepath}' not found (skipping test count check).")
        return True

    try:
        tree = ET.parse(filepath)
        root = tree.getroot()

        # In JUnit XML, root can be <testsuites> or <testsuite>
        failures = int(root.attrib.get("failures", 0))
        errors = int(root.attrib.get("errors", 0))
        tests = int(root.attrib.get("tests", 0))

        # Check nested suites if present
        suites = list(root.iter("testsuite"))
        if suites:
            failures = sum(int(suite.attrib.get("failures", 0)) for suite in suites)
            errors = sum(int(suite.attrib.get("errors", 0)) for suite in suites)
            tests = sum(int(suite.attrib.get("tests", 0)) for suite in suites)

        if failures > 0 or errors > 0:
            print(f"[FAIL] Quality Gate failed: {failures} test failure(s) and {errors} error(s).")
            return False

        print(f"[PASS] All {tests} tests passed with 0 failures and 0 errors.")
        return True
    except ET.ParseError as exc:
        print(f"[FAIL] Could not parse '{filepath}': {exc}")
        return False


def validate_coverage(filepath: Path, min_coverage: float) -> bool:
    """Verifies coverage.xml meets or exceeds min_coverage threshold."""
    if not filepath.exists():
        print(f"[FAIL] Coverage report '{filepath}' not found. Run pytest with --cov-report=xml first.")
        return False

    try:
        tree = ET.parse(filepath)
        root = tree.getroot()

        # Cobertura XML format has line-rate as a decimal (e.g., 0.985 -> 98.5%)
        line_rate_str = root.attrib.get("line-rate")
        if line_rate_str is None:
            print(f"[FAIL] Attribute 'line-rate' not found in root of '{filepath}'.")
            return False

        coverage_pct = round(float(line_rate_str) * 100, 2)
        print(f"[INFO] Measured Code Coverage: {coverage_pct:.2f}% (Threshold: {min_coverage:.2f}%)")

        if coverage_pct < min_coverage:
            print(
                f"[FAIL] Quality Gate failed: Coverage {coverage_pct:.2f}% is below required {min_coverage:.2f}%."
            )
            return False

        print(f"[PASS] Code Coverage Quality Gate passed: {coverage_pct:.2f}% >= {min_coverage:.2f}%.")
        return True
    except (ET.ParseError, ValueError) as exc:
        print(f"[FAIL] Could not parse coverage file '{filepath}': {exc}")
        return False


def main() -> int:
    # Ensure stdout handles UTF-8 safely across platforms and Windows consoles
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    args = parse_args()
    print("=" * 60)
    print("[AUDIT] QUALITY GATE COMPLIANCE (SonarQube / CI Standards)")
    print("=" * 60)

    checks = [
        validate_sonar_properties(Path(args.sonar_properties_file)),
        validate_test_results(Path(args.test_results_file)),
        validate_coverage(Path(args.coverage_file), args.min_coverage),
    ]

    print("=" * 60)
    if all(checks):
        print("[SUCCESS] QUALITY GATE PASSED: All code quality & coverage criteria are met!")
        print("=" * 60)
        return 0
    else:
        print("[ERROR] QUALITY GATE FAILED: Some criteria did not meet compliance.")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
