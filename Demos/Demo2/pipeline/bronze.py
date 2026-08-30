"""Bronze ingestion for deterministic reference data and incremental orders."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from demo2.transformations import with_canonical_row_hash

catalog = spark.conf.get("demo2.catalog", "dbr_dev")
schema = spark.conf.get("demo2.schema", "parvinbadalov")
volume_name = spark.conf.get("demo2.volume_name", "demo2_ecommerce")
runtime_root = f"/Volumes/{catalog}/{schema}/{volume_name}/runtime"


@dp.materialized_view(name="demo2_customers_bronze")
@dp.expect_or_fail("customer_id_present", "customer_id IS NOT NULL")
def customers_bronze():
    source = f"{runtime_root}/source/customers/*.csv"
    return (
        spark.read.option("header", True)
        .csv(source)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn(
            "snapshot_version",
            F.regexp_extract(F.col("_source_file"), r"snapshot_(\d{4})_(\d{2})_(\d{2})", 1).cast(
                "int"
            )
            * F.lit(10000)
            + F.regexp_extract(F.col("_source_file"), r"snapshot_(\d{4})_(\d{2})_(\d{2})", 2).cast(
                "int"
            )
            * F.lit(100)
            + F.regexp_extract(F.col("_source_file"), r"snapshot_(\d{4})_(\d{2})_(\d{2})", 3).cast(
                "int"
            ),
        )
        .withColumn(
            "_source_batch_id",
            F.concat(F.lit("DEMO2_CUSTOMERS_"), F.col("snapshot_version")),
        )
        .withColumn(
            "_batch_loaded_at",
            F.when(
                F.col("snapshot_version") == 20260801, F.to_timestamp(F.lit("2026-09-01T08:00:00Z"))
            ).otherwise(F.to_timestamp(F.lit("2026-09-01T08:30:00Z"))),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.materialized_view(name="demo2_products_bronze")
@dp.expect_or_fail("product_id_present", "product_id IS NOT NULL")
def products_bronze():
    source = f"{runtime_root}/source/products/products.csv"
    return (
        spark.read.option("header", True)
        .csv(source)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_batch_id", F.lit("DEMO2_PRODUCTS_INITIAL"))
        .withColumn("_batch_loaded_at", F.to_timestamp(F.lit("2026-09-01T08:45:00Z")))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(name="demo2_orders_bronze")
@dp.expect("order_line_id_observed", "order_line_id IS NOT NULL")
@dp.expect_or_fail(
    "source_metadata_present",
    "_source_batch_id IS NOT NULL AND _batch_loaded_at IS NOT NULL AND _source_generated_at IS NOT NULL",
)
def orders_bronze():
    source = f"{runtime_root}/landing/orders"
    schema_location = f"{runtime_root}/system/schemas/orders"
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(source)
    )
    return with_canonical_row_hash(raw).withColumn("_ingested_at", F.current_timestamp())
