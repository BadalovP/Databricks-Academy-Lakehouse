from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))


@pytest.fixture(scope="session")
def spark():
    if os.getenv("DEMO2_REMOTE_SPARK") == "1":
        from databricks.connect import DatabricksSession

        session = (
            DatabricksSession.builder.profile(os.getenv("DATABRICKS_CONFIG_PROFILE", "AZURE_DEV"))
            .clusterId(os.getenv("DEMO2_CLUSTER_ID", "0702-171207-xo9bbc0y"))
            .getOrCreate()
        )
    else:
        from pyspark.sql import SparkSession

        session = (
            SparkSession.builder.master("local[2]")
            .appName("demo2-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    yield session
    if os.getenv("DEMO2_REMOTE_SPARK") != "1":
        session.stop()
