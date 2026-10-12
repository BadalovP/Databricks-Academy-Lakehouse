"""Makes `tests` an importable package.

Without this file, `from tests.conftest_spark import ...` resolves locally only by a
sys.path side effect and fails in CI with `ModuleNotFoundError: No module named 'tests'`.
With it, pytest puts the first non-package parent (`Demos/Demo3`) on sys.path, so the
shared Spark helpers import the same way everywhere.
"""
