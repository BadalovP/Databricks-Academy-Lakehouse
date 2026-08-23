from pyspark.sql import DataFrame, Window, functions as F

def prepare_merge_source(source: DataFrame,key_columns:list[str],sequence_column:str,ingestion_column:str="_ingested_at") -> DataFrame:
    missing=[c for c in [*key_columns,sequence_column,ingestion_column] if c not in source.columns]
    if missing: raise ValueError("MERGE source missing: "+", ".join(missing))
    w=Window.partitionBy(*key_columns).orderBy(F.col(sequence_column).desc_nulls_last(),F.col(ingestion_column).desc_nulls_last())
    return source.withColumn("_rn",F.row_number().over(w)).filter("_rn=1").drop("_rn")
