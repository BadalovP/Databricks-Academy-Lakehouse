from copy import deepcopy

from lab07.quality_rules import classify_license_records
from lab07.transformations import prepare_business_licenses
from tests.fixtures.valid_licenses import VALID


def test_duplicate_source_id(spark):
    a = deepcopy(VALID[0])
    b = deepcopy(VALID[1])
    a["id"] = b["id"] = "dup"
    x = classify_license_records(prepare_business_licenses(spark.createDataFrame([a, b])))
    assert x.filter("array_contains(_dq_quarantine_reasons,'DUPLICATE_SOURCE_ID')").count() == 2
