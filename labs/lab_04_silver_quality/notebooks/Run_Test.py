# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Lab 04 — Unit Test Runner
# MAGIC
# MAGIC This notebook runs the reusable Lab 4 Python unit tests.
# MAGIC
# MAGIC It is **not part of the production data pipeline** and should not be added to the main Lab 4 Job DAG.
# MAGIC
# MAGIC Use it:
# MAGIC - manually during development;
# MAGIC - before deployment;
# MAGIC - or from a separate CI/test workflow.
# MAGIC
# MAGIC Expected repository files:
# MAGIC
# MAGIC ```text
# MAGIC lab_04_silver_quality/
# MAGIC ├── src/
# MAGIC │   ├── quality_rules.py
# MAGIC │   └── merge_utils.py
# MAGIC └── tests/
# MAGIC     ├── test_quality_rules.py
# MAGIC     └── test_merge_idempotency.py
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %pip install -q pytest
# MAGIC

# COMMAND ----------

import os
import sys
from pathlib import Path

import pytest

lab04_root = Path(
    "/Workspace/Users/parvinbadalov@yahoo.com/"
    "Databricks-Academy-Lakehouse/labs/lab_04_silver_quality"
)

required_test_files = [
    lab04_root / "tests" / "test_quality_rules.py",
    lab04_root / "tests" / "test_merge_idempotency.py",
]

missing_test_files = [
    str(path)
    for path in required_test_files
    if not path.exists()
]

if missing_test_files:
    raise FileNotFoundError(
        "Required Lab 4 unit-test files are missing:\n- "
        + "\n- ".join(missing_test_files)
    )

os.chdir(lab04_root)

lab04_root_str = str(lab04_root)
if lab04_root_str not in sys.path:
    sys.path.insert(0, lab04_root_str)

sys.dont_write_bytecode = True

# Databricks can retain imported modules in the Python process.
# Remove cached helper modules so pytest loads the latest saved code.
for module_name in [
    "src",
    "src.quality_rules",
    "src.merge_utils",
]:
    sys.modules.pop(module_name, None)

result = pytest.main(
    [
        "-v",
        "--tb=short",
        "--assert=plain",
        "-p",
        "no:cacheprovider",
        "tests/test_quality_rules.py",
        "tests/test_merge_idempotency.py",
        "tests/test_contracts.py",
    ]
)

if result != pytest.ExitCode.OK:
    raise AssertionError(
        f"Lab 4 tests failed with pytest exit code {result}."
    )

print("✅ All Lab 4 reusable-module unit tests passed.")