# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 11 - validate a Zerobus run inside Databricks
# MAGIC
# MAGIC Optional in-workspace alternative to `scripts/run_validation.py`. It runs the same SQL assets
# MAGIC from this repository: the idempotent Silver `MERGE` twice, then the reconciliation query.
# MAGIC It never writes to Bronze, which only Zerobus Ingest fills.

# COMMAND ----------

import re
from pathlib import Path

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lab_11_zerobus")
dbutils.widgets.text("run_id", "")

values = {
    "catalog": dbutils.widgets.get("catalog").strip(),
    "schema": dbutils.widgets.get("schema").strip(),
    "bronze": "bronze_iot_events",
    "silver": "silver_iot_events",
    "run_id": dbutils.widgets.get("run_id").strip(),
}
for name, value in values.items():
    pattern = r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}" if name == "run_id" else r"[A-Za-z_][A-Za-z0-9_]*"
    assert re.fullmatch(pattern, value), f"Invalid {name}: {value!r}"

SQL_DIR = Path.cwd().parent / "sql"


def sql_file(name: str) -> str:
    text = (SQL_DIR / name).read_text(encoding="utf-8")
    text = re.sub(r"\$\{(\w+)\}", lambda m: values[m.group(1)], text)
    body = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    return body.strip().rstrip(";")


# COMMAND ----------

# The second MERGE must report 0 affected rows: Silver is not rewritten when nothing changed.
for attempt in (1, 2):
    display(
        spark.sql(sql_file("01_deduplication.sql")).withColumnRenamed(
            "num_affected_rows", f"run_{attempt}_affected_rows"
        )
    )

# COMMAND ----------

display(spark.sql(sql_file("02_validation.sql")))
