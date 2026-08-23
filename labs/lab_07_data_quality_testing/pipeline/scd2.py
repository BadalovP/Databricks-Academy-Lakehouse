from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT/"src") not in sys.path: sys.path.insert(0,str(ROOT/"src"))

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from lab07.config import SCD_TRACKED_COLUMNS
catalog=spark.conf.get("lab07.catalog","dbr_dev"); schema=spark.conf.get("lab07.schema","parvinbadalov"); n=int(spark.conf.get("lab07.snapshot_count","3")); feed=f"{catalog}.{schema}.business_license_snapshot_feed"
def next_snapshot_and_version(latest_snapshot_version):
    v=1 if latest_snapshot_version is None else int(latest_snapshot_version)+1
    if v>n: return None
    return spark.read.table(feed).filter(F.col("snapshot_version")==v).select(*SCD_TRACKED_COLUMNS), v
dp.create_streaming_table("dim_license_scd2")
dp.create_auto_cdc_from_snapshot_flow(target="dim_license_scd2",source=next_snapshot_and_version,keys=["license_number"],stored_as_scd_type=2)
