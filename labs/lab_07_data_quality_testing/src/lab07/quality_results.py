from datetime import datetime, timezone
from pyspark.sql import Row

def make_result(run_id,check_name,dimension,status,severity,observed_value=None,threshold=None,details=""):
    return Row(run_id=str(run_id),check_name=check_name,dimension=dimension,status=status,severity=severity,observed_value=None if observed_value is None else str(observed_value),threshold=None if threshold is None else str(threshold),details=details,checked_at=datetime.now(timezone.utc))
def append_results(spark,table_name,rows):
    rows=list(rows)
    if rows: spark.createDataFrame(rows).write.mode("append").option("mergeSchema","true").saveAsTable(table_name)
