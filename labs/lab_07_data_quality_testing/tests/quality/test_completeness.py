from copy import deepcopy
from lab07.transformations import prepare_business_licenses
from lab07.quality_rules import classify_license_records
from tests.fixtures.valid_licenses import VALID
def test_missing_license_number_quarantined(spark):
    x=deepcopy(VALID[0]); x['id']='bad'; x['license_number']=None; r=classify_license_records(prepare_business_licenses(spark.createDataFrame([x,VALID[1]]))).filter("id='bad'").first(); assert r['_dq_status']=='QUARANTINE'; assert 'LICENSE_NUMBER_MISSING' in r['_dq_quarantine_reasons']
