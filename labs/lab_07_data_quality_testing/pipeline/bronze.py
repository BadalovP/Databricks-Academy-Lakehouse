from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("lab07.catalog", "dbr_dev")
schema = spark.conf.get("lab07.schema", "parvinbadalov")


@dp.materialized_view(name="business_license_bronze")
@dp.expect("source_id_populated", "id IS NOT NULL")
@dp.expect_or_fail(
    "landing_metadata_present",
    "_source_batch_id IS NOT NULL AND _ingested_at IS NOT NULL",
)
def business_license_bronze():
    return spark.read.table(f"{catalog}.{schema}.business_license_landing").withColumn(
        "_bronze_loaded_at",
        F.current_timestamp(),
    )
