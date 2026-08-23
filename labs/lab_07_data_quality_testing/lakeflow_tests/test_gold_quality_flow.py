import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark
test_pipeline=TestPipeline.active()
def test_license_quality_daily(test_spark):
    # Populate mocked named source tables in the Editor before running this test.
    # This file is intentionally a runnable scaffold to customize with the supplied fixtures.
    test_pipeline.run(test_spark, set(["dbr_dev.parvinbadalov.license_quality_daily"]))
    assert test_spark.table("dbr_dev.parvinbadalov.license_quality_daily") is not None
