import os,pytest
from pyspark.sql import functions as F
pytestmark=pytest.mark.remote
@pytest.mark.skipif(os.getenv('LAB07_REMOTE_INTEGRATION')!='1',reason='requires deployed Lab 07')
def test_deployed_reconciliation(spark):
    b=spark.table('dbr_dev.parvinbadalov.business_license_bronze').count(); v=spark.table('dbr_dev.parvinbadalov.business_license_validated').count(); q=spark.table('dbr_dev.parvinbadalov.business_license_quarantine').count(); assert b==v+q
