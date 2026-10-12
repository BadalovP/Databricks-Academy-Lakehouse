"""Offline test fixtures. No test contacts Databricks or Zerobus."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

LAB = Path(__file__).resolve().parents[1]
for path in (LAB / "src", LAB / "scripts"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from lab11.config import load_config  # noqa: E402
from lab11.producer import build_plan, fixed_clock  # noqa: E402

START_MICROS = 1_791_000_000_000_000


@pytest.fixture
def config():
    return load_config()


@pytest.fixture
def target(config):
    return config.target("personal_dev")


@pytest.fixture
def plan(config):
    return build_plan(config.producer, "test-run", clock=fixed_clock(START_MICROS))
