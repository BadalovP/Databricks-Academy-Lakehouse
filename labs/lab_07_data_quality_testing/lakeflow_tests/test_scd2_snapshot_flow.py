import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark
test_pipeline=TestPipeline.active()
def test_dim_license_scd2(test_spark):
    # Populate mocked named source tables in the Editor before running this test.
    # This file is intentionally a runnable scaffold to customize with the supplied fixtures.
    test_pipeline.run(test_spark, set(["dbr_dev.parvinbadalov.dim_license_scd2"]))
    assert test_spark.table("dbr_dev.parvinbadalov.dim_license_scd2") is not None
