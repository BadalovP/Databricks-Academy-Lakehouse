import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark
test_pipeline=TestPipeline.active()
def test_business_license_validated(test_spark):
    # Populate mocked named source tables in the Editor before running this test.
    # This file is intentionally a runnable scaffold to customize with the supplied fixtures.
    test_pipeline.run(test_spark, set(["dbr_dev.parvinbadalov.business_license_validated"]))
    assert test_spark.table("dbr_dev.parvinbadalov.business_license_validated") is not None
