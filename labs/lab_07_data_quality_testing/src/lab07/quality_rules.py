from pyspark.sql import DataFrame, Window, functions as F
from lab07.config import APPLICATION_TYPES, LICENSE_STATUSES

def classify_license_records(df: DataFrame) -> DataFrame:
    w=Window.partitionBy("id")
    x=df.withColumn("_source_id_occurrences",F.count("*").over(w))
    warn=F.array_compact(F.array(F.when(F.col("doing_business_as_name").isNull(),F.lit("DBA_NAME_MISSING"))))
    quarantine=F.array_compact(F.array(
      F.when(F.col("id").isNull(),F.lit("SOURCE_ID_MISSING")),
      F.when(F.col("license_number").isNull(),F.lit("LICENSE_NUMBER_MISSING")),
      F.when(F.col("legal_name").isNull(),F.lit("LEGAL_NAME_MISSING")),
      F.when(F.col("_source_id_occurrences")>1,F.lit("DUPLICATE_SOURCE_ID")),
      F.when(F.col("application_type").isNull() | ~F.col("application_type").isin(*APPLICATION_TYPES),F.lit("INVALID_APPLICATION_TYPE")),
      F.when(F.col("license_status").isNull() | ~F.col("license_status").isin(*LICENSE_STATUSES),F.lit("INVALID_LICENSE_STATUS")),
      F.when(F.col("zip_code").isNotNull() & ~F.col("zip_code").rlike(r"^[0-9]{5}(-[0-9]{4})?$"),F.lit("INVALID_ZIP")),
      F.when(F.col("latitude").isNotNull() & ~F.col("latitude").between(-90.0,90.0),F.lit("INVALID_LATITUDE")),
      F.when(F.col("longitude").isNotNull() & ~F.col("longitude").between(-180.0,180.0),F.lit("INVALID_LONGITUDE")),
      F.when(F.col("expiration_date").isNotNull() & F.col("license_start_date").isNotNull() & (F.col("expiration_date")<F.col("license_start_date")),F.lit("EXPIRATION_BEFORE_START")),
      F.when(F.col("license_status").isin("AAC","REV","REA") & F.col("license_status_change_date").isNull(),F.lit("STATUS_CHANGE_DATE_MISSING"))
    ))
    dims=F.array_distinct(F.array_compact(F.array(
      F.when(F.size(warn)>0,F.lit("completeness")),
      F.when(F.array_contains(quarantine,"DUPLICATE_SOURCE_ID"),F.lit("uniqueness")),
      F.when(F.exists(quarantine,lambda z: z.isin("INVALID_APPLICATION_TYPE","INVALID_LICENSE_STATUS","INVALID_ZIP","INVALID_LATITUDE","INVALID_LONGITUDE")),F.lit("validity")),
      F.when(F.exists(quarantine,lambda z: z.isin("EXPIRATION_BEFORE_START","STATUS_CHANGE_DATE_MISSING")),F.lit("consistency")),
      F.when(F.exists(quarantine,lambda z: z.isin("SOURCE_ID_MISSING","LICENSE_NUMBER_MISSING","LEGAL_NAME_MISSING")),F.lit("completeness"))
    )))
    return (x.withColumn("_dq_warn_reasons",warn).withColumn("_dq_quarantine_reasons",quarantine).withColumn("_dq_dimensions",dims)
      .withColumn("_dq_status",F.when(F.size("_dq_quarantine_reasons")>0,"QUARANTINE").when(F.size("_dq_warn_reasons")>0,"WARN").otherwise("VALID"))
      .drop("_source_id_occurrences"))
