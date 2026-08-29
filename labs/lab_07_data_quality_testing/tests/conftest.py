import os

import pytest


@pytest.fixture(scope="session")
def spark():
    if os.getenv("LAB07_REMOTE_SPARK") == "1":
        from databricks.connect import DatabricksSession

        yield DatabricksSession.builder.getOrCreate()
        return
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("lab07-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()
