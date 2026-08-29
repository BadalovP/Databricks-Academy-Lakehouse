from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TIMESTAMPS = (
    "payment_date",
    "license_start_date",
    "expiration_date",
    "date_issued",
    "license_status_change_date",
    "_source_dataset_updated_at",
    "_ingested_at",
)
BIGINTS = ("license_id", "account_number", "site_number", "license_number", "_source_offset")
INTS = ("ward", "precinct", "police_district", "community_area", "license_code")
DOUBLES = ("latitude", "longitude")


def normalize_business_licenses(df: DataFrame) -> DataFrame:
    out = df
    for c in [
        "id",
        "legal_name",
        "doing_business_as_name",
        "address",
        "city",
        "state",
        "zip_code",
        "license_description",
        "application_type",
        "license_status",
        "_source_batch_id",
        "_source_api",
        "_source_dataset_id",
        "_fixture_kind",
    ]:
        if c in out.columns:
            out = out.withColumn(
                c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))
            )
    for c in ["application_type", "license_status", "state"]:
        if c in out.columns:
            out = out.withColumn(c, F.upper(F.col(c)))
    for c in TIMESTAMPS:
        if c in out.columns:
            out = out.withColumn(c, F.to_timestamp(c))
    for c in BIGINTS:
        if c in out.columns:
            out = out.withColumn(c, F.col(c).cast("bigint"))
    for c in INTS:
        if c in out.columns:
            out = out.withColumn(c, F.col(c).cast("int"))
    for c in DOUBLES:
        if c in out.columns:
            out = out.withColumn(c, F.col(c).cast("double"))
    return out


def prepare_business_licenses(df: DataFrame) -> DataFrame:
    out = normalize_business_licenses(df)
    return (
        out.withColumn(
            "license_term_days",
            F.when(
                F.col("license_start_date").isNotNull() & F.col("expiration_date").isNotNull(),
                F.datediff("expiration_date", "license_start_date"),
            ).cast("int"),
        )
        .withColumn("is_location_change", F.col("application_type") == "C_LOC")
        .withColumn("is_renewal", F.col("application_type") == "RENEW")
        .withColumn("_ingestion_date", F.to_date("_ingested_at"))
    )
