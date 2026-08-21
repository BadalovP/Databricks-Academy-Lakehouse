"""Tests for the Serverless-safe external Delta helper module."""

from unittest.mock import MagicMock

import pytest

from src.external_tables import (
    normalize_location,
    registered_table_location,
    register_external_delta_table,
    validate_registered_location,
)


def _detail_row(location: str):
    row = MagicMock()
    row.asDict.return_value = {"location": location}
    return row


def test_normalize_location_ignores_case_and_trailing_slash():
    assert (
        normalize_location("ABFSS://Container@Account/path/")
        == "abfss://container@account/path"
    )


def test_registered_table_location_returns_none_when_table_missing():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False

    assert registered_table_location(spark, "cat.sch.table") is None
    spark.sql.assert_not_called()


def test_registered_table_location_reads_describe_detail():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    spark.sql.return_value.first.return_value = _detail_row(
        "abfss://container@account/path/table"
    )

    result = registered_table_location(spark, "cat.sch.table")

    assert result == "abfss://container@account/path/table"
    spark.sql.assert_called_once_with("DESCRIBE DETAIL cat.sch.table")


def test_register_existing_table_at_same_location_is_idempotent():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    spark.sql.return_value.first.return_value = _detail_row(
        "abfss://container@account/path/table"
    )

    register_external_delta_table(
        spark,
        "cat.sch.table",
        "abfss://container@account/path/table/",
    )

    # Only DESCRIBE DETAIL is needed. No CREATE TABLE should be issued.
    calls = [str(call) for call in spark.sql.call_args_list]
    assert not any("CREATE TABLE" in call for call in calls)


def test_register_rejects_existing_table_at_different_location():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    spark.sql.return_value.first.return_value = _detail_row(
        "abfss://container@account/old/table"
    )

    with pytest.raises(RuntimeError, match="already registered"):
        register_external_delta_table(
            spark,
            "cat.sch.table",
            "abfss://container@account/new/table",
        )


def test_validate_registered_location_rejects_mismatch():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    spark.sql.return_value.first.return_value = _detail_row(
        "abfss://container@account/actual"
    )

    with pytest.raises(RuntimeError, match="location mismatch"):
        validate_registered_location(
            spark,
            "cat.sch.table",
            "abfss://container@account/expected",
        )
