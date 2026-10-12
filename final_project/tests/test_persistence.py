"""Static and fake-client tests for the shared Delta MERGE helper."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from urbanflow.persistence import (
    build_merge_sql,
    merge_delta_table,
    quote_qualified_identifier,
)


def test_qualified_table_names_are_validated_and_quoted() -> None:
    assert quote_qualified_identifier("dbr_dev.parvinbadalov_urbanflow.silver") == (
        "`dbr_dev`.`parvinbadalov_urbanflow`.`silver`"
    )
    with pytest.raises(ValueError, match="plain identifier"):
        quote_qualified_identifier("dbr_dev.schema.silver; DROP TABLE bronze")


def test_merge_sql_uses_null_safe_composite_keys_and_quotes_columns() -> None:
    sql = build_merge_sql(
        table_name="dbr_dev.schema.quarantine",
        source_view="merge_source",
        columns=("topic", "partition", "offset", "reason"),
        key_columns=("topic", "partition", "offset"),
    )

    assert "t.`topic` <=> s.`topic`" in sql
    assert "t.`partition` <=> s.`partition`" in sql
    assert "WHEN MATCHED THEN UPDATE SET t.`reason` = s.`reason`" in sql
    assert "INSERT (`topic`, `partition`, `offset`, `reason`)" in sql


class _FakeWriter:
    def __init__(self) -> None:
        self.format_name = None
        self.saved_table = None

    def format(self, name: str):
        self.format_name = name
        return self

    def saveAsTable(self, name: str) -> None:  # noqa: N802 - Spark API spelling
        self.saved_table = name


class _FakeFrame:
    columns = ["event_id", "value"]

    def __init__(self, *, rows: int = 1, unique: int = 1) -> None:
        self.rows = rows
        self.unique = unique
        self.write = _FakeWriter()
        self.view_name = None
        self._selected = False

    def count(self) -> int:
        return self.unique if self._selected else self.rows

    def select(self, *_columns: str):
        selected = _FakeFrame(rows=self.rows, unique=self.unique)
        selected._selected = True
        return selected

    def distinct(self):
        return self

    def limit(self, _count: int):
        return self

    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802 - Spark API spelling
        self.view_name = name


def test_merge_creates_once_and_reports_idempotent_counts() -> None:
    spark = Mock()
    spark.catalog.tableExists.return_value = False
    spark.table.side_effect = [SimpleNamespace(count=lambda: 0), SimpleNamespace(count=lambda: 1)]
    frame = _FakeFrame()

    report = merge_delta_table(
        spark,
        frame,
        "dbr_dev.schema.silver",
        key_columns=("event_id",),
    )

    assert frame.write.format_name == "delta"
    assert frame.write.saved_table == "dbr_dev.schema.silver"
    assert report["inserted_rows"] == 1
    assert report["table_existed"] is False
    assert spark.sql.call_count == 1


def test_merge_refuses_duplicate_source_keys_before_writing() -> None:
    spark = Mock()
    frame = _FakeFrame(rows=2, unique=1)

    with pytest.raises(ValueError, match="duplicate source keys"):
        merge_delta_table(
            spark,
            frame,
            "dbr_dev.schema.silver",
            key_columns=("event_id",),
        )

    spark.sql.assert_not_called()


def test_merge_on_an_existing_table_reports_zero_inserts_when_rows_are_unchanged() -> None:
    """Rerun safety: re-merging the same keys must update in place, not append.

    This drives the real merge_delta_table code path for the second-run case and pins the
    contract that a repeat merge inserts nothing. It does NOT prove Delta's own MERGE
    semantics, because delta-spark's jars cannot be resolved in this environment (local
    Spark raises ClassNotFoundException for DeltaSparkSessionExtension), so the Delta
    engine itself is mocked here. True end-to-end idempotency is proven by running the
    Silver-to-Gold Job twice against the real Bronze table and observing the row counts
    stay equal; see docs/SILVER_GOLD_RUNBOOK.md.
    """
    spark = Mock()
    spark.catalog.tableExists.return_value = True
    # Same count before and after: every source key already existed, so MERGE updated.
    spark.table.side_effect = [
        SimpleNamespace(count=lambda: 2520),
        SimpleNamespace(count=lambda: 2520),
    ]
    frame = _FakeFrame(rows=2520, unique=2520)

    report = merge_delta_table(spark, frame, "dbr_dev.schema.silver", key_columns=("event_id",))

    assert report["table_existed"] is True
    assert report["rows_before"] == 2520
    assert report["rows_after"] == 2520
    assert report["inserted_rows"] == 0
    # An existing table must not be recreated, which would silently discard history.
    assert frame.write.saved_table is None
    assert spark.sql.call_count == 1


def test_merge_rejects_a_table_name_that_could_touch_another_schema() -> None:
    """Guards the 'cannot accidentally modify unrelated tables' requirement."""
    spark = Mock()
    for hostile in (
        "dbr_dev.other_student_schema.silver; DROP TABLE bronze_station_status",
        "dbr_dev..silver",
        "dbr_dev.schema.silver`",
        "a.b.c.d",
    ):
        with pytest.raises(ValueError):
            merge_delta_table(spark, _FakeFrame(), hostile, key_columns=("event_id",))
    spark.sql.assert_not_called()
