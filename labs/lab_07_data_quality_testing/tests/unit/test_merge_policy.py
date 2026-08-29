from pyspark.sql import functions as F

from lab07.merge_policy import prepare_merge_source
from tests.fixtures.duplicate_licenses import DUPLICATES


def test_duplicate_merge_source_is_deterministic(spark):
    df = spark.createDataFrame(
        DUPLICATES,
        "license_number long,seq int,value string,_ingested_at string",
    ).withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
    r = prepare_merge_source(df, ["license_number"], "seq").first()
    assert r["value"] == "new"
