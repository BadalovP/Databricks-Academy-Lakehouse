from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT/"src") not in sys.path: sys.path.insert(0,str(ROOT/"src"))

from pyspark import pipelines as dp
from lab07.transformations import prepare_business_licenses
from lab07.quality_rules import classify_license_records
@dp.materialized_view(name="business_license_classified")
def classified(): return classify_license_records(prepare_business_licenses(spark.read.table("business_license_bronze")))
@dp.materialized_view(name="business_license_quarantine")
def quarantine(): return spark.read.table("business_license_classified").filter("size(_dq_quarantine_reasons)>0")
@dp.materialized_view(name="business_license_validated")
@dp.expect("dba_name_present","doing_business_as_name IS NOT NULL")
@dp.expect_or_fail("trusted_metadata_present","_source_batch_id IS NOT NULL AND _ingested_at IS NOT NULL")
@dp.expect_or_drop("trusted_only","size(_dq_quarantine_reasons)=0")
def validated(): return spark.read.table("business_license_classified")
