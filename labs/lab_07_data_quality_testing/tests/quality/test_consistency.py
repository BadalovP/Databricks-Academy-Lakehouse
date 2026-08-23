from copy import deepcopy
from lab07.transformations import prepare_business_licenses
from lab07.quality_rules import classify_license_records
from tests.fixtures.valid_licenses import VALID
def test_bad_date_order(spark):
    x=deepcopy(VALID[0]); x['id']='bad'; x['license_start_date']='2026-12-31'; x['expiration_date']='2026-01-01'; r=classify_license_records(prepare_business_licenses(spark.createDataFrame([x,VALID[1]]))).filter("id='bad'").first(); assert 'EXPIRATION_BEFORE_START' in r['_dq_quarantine_reasons']
