from lab07.transformations import prepare_business_licenses
from tests.fixtures.valid_licenses import VALID


def test_normalization(spark):
    r = prepare_business_licenses(spark.createDataFrame(VALID)).orderBy("id").first()
    assert r["application_type"] == "ISSUE"
    assert r["license_status"] == "AAI"
    assert r["state"] == "IL"
    assert r["license_number"] == 700001
