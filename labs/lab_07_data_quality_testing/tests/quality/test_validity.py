from copy import deepcopy

from lab07.quality_rules import classify_license_records
from lab07.transformations import prepare_business_licenses
from tests.fixtures.valid_licenses import VALID


def test_invalid_status(spark):
    x = deepcopy(VALID[0])
    x["id"] = "bad"
    x["license_status"] = "INVALID"
    r = (
        classify_license_records(prepare_business_licenses(spark.createDataFrame([x, VALID[1]])))
        .filter("id='bad'")
        .first()
    )
    assert "INVALID_LICENSE_STATUS" in r["_dq_quarantine_reasons"]
