"""
LAB 08 - TravelOps

Package:
travelops

Purpose:
Provides small, unit-testable business rules used by the Lab 8 Databricks
lakehouse implementation.

Responsibilities:
- expose transformation helper modules
- keep local tests independent from a live Spark session

Inputs:
None at import time.

Outputs:
No data is created by importing this package.

Idempotency:
Imports are side-effect free and safe to rerun.

Environment behavior:
Databricks target-specific behavior is handled by bundle variables and notebooks,
not package import side effects.
"""

__all__ = ["config", "quality_rules", "reconciliation", "transformations"]
