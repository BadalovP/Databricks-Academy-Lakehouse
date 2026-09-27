# Databricks notebook source
# MAGIC %md
# MAGIC # LAB 09 - Reconciliation notebook
# MAGIC
# MAGIC Runs after the Lakeflow pipeline update completes. Queries
# MAGIC `lab09_taxi_bronze`, `lab09_taxi_silver`, `lab09_taxi_quarantine` and
# MAGIC `lab09_taxi_daily_summary`, asserts the reconciliation invariant
# MAGIC `bronze_rows == silver_valid_rows + rejected_rows`, and returns a
# MAGIC machine-readable JSON payload via `dbutils.notebook.exit()` so the
# MAGIC automation (src/lab09/jobs.py's get_run_output_json) can read it back
# MAGIC through the Jobs API without a SQL Warehouse / Statement Execution API
# MAGIC dependency.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbr_dev")
dbutils.widgets.text("schema", "parvinbadalov")
dbutils.widgets.text("bronze_table", "lab09_taxi_bronze")
dbutils.widgets.text("silver_table", "lab09_taxi_silver")
dbutils.widgets.text("quarantine_table", "lab09_taxi_quarantine")
dbutils.widgets.text("gold_table", "lab09_taxi_daily_summary")

# COMMAND ----------

import json

from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
bronze_table = f"{catalog}.{schema}.{dbutils.widgets.get('bronze_table')}"
silver_table = f"{catalog}.{schema}.{dbutils.widgets.get('silver_table')}"
quarantine_table = f"{catalog}.{schema}.{dbutils.widgets.get('quarantine_table')}"
gold_table = f"{catalog}.{schema}.{dbutils.widgets.get('gold_table')}"

# COMMAND ----------

bronze_rows = spark.table(bronze_table).count()
silver_valid_rows = spark.table(silver_table).count()
rejected_rows = spark.table(quarantine_table).count()
gold_rows = spark.table(gold_table).count()

# COMMAND ----------

quarantine_df = spark.table(quarantine_table)
failed_rule_rows = (
    quarantine_df.select(F.explode("failed_rules").alias("rule")).groupBy("rule").count().collect()
)
failed_rules = {row["rule"]: int(row["count"]) for row in failed_rule_rows}

# COMMAND ----------

reconciliation_passed = bronze_rows == (silver_valid_rows + rejected_rows)

assert reconciliation_passed, (
    f"Reconciliation invariant failed: bronze_rows={bronze_rows} != "
    f"silver_valid_rows({silver_valid_rows}) + rejected_rows({rejected_rows}) "
    f"= {silver_valid_rows + rejected_rows}"
)

# COMMAND ----------

result = {
    "bronze_rows": bronze_rows,
    "silver_valid_rows": silver_valid_rows,
    "rejected_rows": rejected_rows,
    "gold_rows": gold_rows,
    "failed_rules": failed_rules,
    "reconciliation_passed": reconciliation_passed,
}

print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
