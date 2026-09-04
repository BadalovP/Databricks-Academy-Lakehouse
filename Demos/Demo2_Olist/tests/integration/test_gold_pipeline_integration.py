"""Integration tests for the deployed Olist Gold pipeline."""

import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CATALOG = "dbr_dev"
SCHEMA = "parvinbadalov"


@pytest.fixture(scope="module")
def spark_session():
    """Use the active Databricks Spark session."""

    session = SparkSession.getActiveSession()

    if session is None:
        session = SparkSession.builder.getOrCreate()

    return session


def test_gold_fact_matches_latest_reconciliation_audit(
    spark_session,
):
    """Gold fact count must match the latest successful audit."""

    fact_table = (
        f"{CATALOG}.{SCHEMA}.gold_fact_order_items"
    )

    audit_table = (
        f"{CATALOG}.{SCHEMA}.gold_reconciliation_audit"
    )

    assert spark_session.catalog.tableExists(fact_table)
    assert spark_session.catalog.tableExists(audit_table)

    actual_gold_rows = (
        spark_session.table(fact_table).count()
    )

    latest_audit = (
        spark_session.table(audit_table)
        .orderBy(F.col("_validated_at").desc())
        .select(
            "gold_rows",
            "integrity_status",
            "overall_status",
        )
        .first()
    )

    assert latest_audit is not None
    assert actual_gold_rows == latest_audit["gold_rows"]
    assert latest_audit["integrity_status"] == "PASS"
    assert latest_audit["overall_status"] == "PASS"