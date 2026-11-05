"""Shared fixtures for pytest test suite."""

from pathlib import Path

import pytest


@pytest.fixture
def project_root() -> Path:
    """Fixture returning the root directory of the project."""
    return Path(__file__).resolve().parent.parent
