"""Tests for the Phase 2 correction: schema migration and stale derived-row removal.

Two live defects motivate this file, both found by inspecting the real Azure tables
rather than by reading code:

1. `silver_station_status` was created with 27 columns, before `is_operational` joined the
   Silver contract. Delta MERGE does not evolve the target schema on its own, so the
   corrected run would fail on an unresolvable target column.
2. `gold_station_shortage` and `gold_rebalancing_priority` are derived filters whose row
   set shrinks when a station stops qualifying. UPDATE and INSERT alone leave the old rows
   behind forever, so roughly 89 out-of-service stations would stay listed as actionable.

What is real here and what is not: schema migration and stale-row DETECTION run against a
genuine local Spark session using plain saved tables, so the schema comparison, the
generated `ALTER TABLE` and the anti-join are all really executed. The scoped MERGE itself
cannot run locally because delta-spark's jars do not resolve in this environment, so those
tests assert the generated statement instead. The deletion actually taking effect is proven
by the live corrected run.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest

from urbanflow.persistence import (
    _stale_scope_count,
    evolve_delta_schema,
    replace_execution_scope,
)


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-migration-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _view(spark: Any, name: str, sql: str) -> None:
    """A temp view satisfies tableExists() and table(), needs no warehouse and leaks nothing."""
    spark.sql(sql).createOrReplaceTempView(name)


def _table(spark: Any, tmp_path, name: str, sql: str) -> None:
    """A real table at an explicit temp location.

    ALTER TABLE ADD COLUMNS needs a real table, not a view. The location is explicit and
    per-test because DROP TABLE leaves the warehouse directory behind, so reusing a managed
    name fails the next run with LOCATION_ALREADY_EXISTS -- and a repo-relative warehouse
    would litter the working tree.
    """
    location = (tmp_path / name).as_posix()
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.sql(sql).write.mode("overwrite").option("path", location).saveAsTable(name)


# --- 1 and 2: existing table missing is_operational, and controlled migration ---


@pytest.mark.spark
def test_existing_table_missing_is_operational_is_detected_and_migrated(
    spark_session, tmp_path
) -> None:
    """The live shape: a 3-column table meeting a 4-column contract."""
    _table(
        spark_session,
        tmp_path,
        "mig_silver",
        "SELECT * FROM VALUES ('e1','run-1','AVAILABLE') AS t(event_id, execution_id, availability_status)",
    )
    before = {f.name for f in spark_session.table("mig_silver").schema.fields}
    assert "is_operational" not in before  # precondition, mirroring the real table

    corrected = spark_session.sql(
        "SELECT * FROM VALUES ('e1','run-1','AVAILABLE',true) "
        "AS t(event_id, execution_id, availability_status, is_operational)"
    )

    report = evolve_delta_schema(spark_session, corrected, "mig_silver")

    assert report == {
        "table_existed": True,
        "added_columns": ["is_operational"],
        "migrated": True,
    }
    after = {f.name for f in spark_session.table("mig_silver").schema.fields}
    assert "is_operational" in after
    # Existing data survives the migration; the new column is simply null.
    rows = spark_session.table("mig_silver").collect()
    assert len(rows) == 1
    assert rows[0]["event_id"] == "e1"
    assert rows[0]["is_operational"] is None


@pytest.mark.spark
def test_migration_is_a_no_op_when_the_schema_already_matches(spark_session, tmp_path) -> None:
    """Rerun safety: a second migration must not re-add or duplicate anything."""
    _table(
        spark_session,
        tmp_path,
        "mig_noop",
        "SELECT * FROM VALUES ('e1',true) AS t(event_id, is_operational)",
    )
    frame = spark_session.sql("SELECT * FROM VALUES ('e1',true) AS t(event_id, is_operational)")

    first = evolve_delta_schema(spark_session, frame, "mig_noop")
    second = evolve_delta_schema(spark_session, frame, "mig_noop")

    assert first["added_columns"] == []
    assert first["migrated"] is False
    assert second == first
    assert len(spark_session.table("mig_noop").schema.fields) == 2


@pytest.mark.spark
def test_migration_only_adds_and_never_drops_a_column_the_source_lost(
    spark_session, tmp_path
) -> None:
    """Narrow by design: a column missing from the source must NOT be removed."""
    _table(
        spark_session,
        tmp_path,
        "mig_keep",
        "SELECT * FROM VALUES ('e1','keep-me') AS t(event_id, legacy_column)",
    )
    narrower = spark_session.sql("SELECT * FROM VALUES ('e1') AS t(event_id)")

    report = evolve_delta_schema(spark_session, narrower, "mig_keep")

    assert report["added_columns"] == []
    assert "legacy_column" in {f.name for f in spark_session.table("mig_keep").schema.fields}
    assert spark_session.table("mig_keep").collect()[0]["legacy_column"] == "keep-me"


def test_migration_on_an_absent_table_reports_nothing_to_do() -> None:
    spark = Mock()
    spark.catalog.tableExists.return_value = False

    report = evolve_delta_schema(spark, Mock(), "dbr_dev.schema.absent")

    assert report == {"table_existed": False, "added_columns": [], "migrated": False}
    spark.sql.assert_not_called()


def test_migration_refuses_a_table_name_outside_the_identifier_rules() -> None:
    spark = Mock()
    with pytest.raises(ValueError, match="plain identifier"):
        evolve_delta_schema(spark, Mock(), "dbr_dev.schema.t; DROP TABLE bronze")
    spark.sql.assert_not_called()


# --- 3, 4, 5, 7: which rows are stale, against real Spark -----------------------


def _shortage_target(spark: Any, name: str) -> None:
    """Mirrors the live table: our execution plus another execution's rows."""
    _view(
        spark,
        name,
        "SELECT * FROM VALUES "
        "('ours-low','run-1','st-1','LOW_BIKES'), "  # still qualifies
        "('ours-oos','run-1','st-2','LOW_BIKES_AND_DOCKS'), "  # was misclassified
        "('ours-oos2','run-1','st-3','LOW_BIKES_AND_DOCKS'), "  # was misclassified
        "('other-run','run-2','st-9','LOW_BIKES') "  # different execution
        "AS t(event_id, execution_id, station_id, availability_status)",
    )


@pytest.mark.spark
def test_out_of_service_rows_are_identified_as_stale_and_valid_ones_are_not(
    spark_session,
) -> None:
    """Scenarios 3 and 4: the misclassified rows go, the genuine shortage stays."""
    _shortage_target(spark_session, "stale_mix")
    # The corrected source keeps only the genuine shortage.
    corrected = spark_session.sql(
        "SELECT * FROM VALUES ('ours-low','run-1','st-1','LOW_BIKES') "
        "AS t(event_id, execution_id, station_id, availability_status)"
    )

    stale = _stale_scope_count(
        spark_session, corrected, "stale_mix", ("event_id",), "execution_id", "run-1"
    )

    assert stale == 2  # ours-oos and ours-oos2, never other-run


@pytest.mark.spark
def test_rows_belonging_to_another_execution_are_never_counted_as_stale(
    spark_session,
) -> None:
    """Scenario 7: another execution's rows are outside our scope entirely."""
    _shortage_target(spark_session, "stale_scope")
    empty = spark_session.sql(
        "SELECT * FROM VALUES ('x','run-1','st-0','LOW_BIKES') "
        "AS t(event_id, execution_id, station_id, availability_status)"
    ).where("1=0")

    stale = _stale_scope_count(
        spark_session, empty, "stale_scope", ("event_id",), "execution_id", "run-1"
    )

    # All three run-1 rows are stale; run-2's single row is untouched by the count.
    assert stale == 3


@pytest.mark.spark
def test_an_empty_corrected_source_marks_every_scoped_row_stale(spark_session) -> None:
    """Scenario 5: if nothing qualifies any more, every stale actionable row must go."""
    _view(
        spark_session,
        "stale_empty",
        "SELECT * FROM VALUES ('a','run-1'), ('b','run-1') AS t(event_id, execution_id)",
    )
    empty = spark_session.sql(
        "SELECT * FROM VALUES ('a','run-1') AS t(event_id, execution_id)"
    ).where("1=0")

    assert (
        _stale_scope_count(
            spark_session, empty, "stale_empty", ("event_id",), "execution_id", "run-1"
        )
        == 2
    )


@pytest.mark.spark
def test_nothing_is_stale_when_the_source_reproduces_the_target(spark_session) -> None:
    """Scenario 6, detection half: a repeat of the corrected run removes nothing."""
    _view(
        spark_session,
        "stale_same",
        "SELECT * FROM VALUES ('a','run-1'), ('b','run-1') AS t(event_id, execution_id)",
    )
    same = spark_session.sql(
        "SELECT * FROM VALUES ('a','run-1'), ('b','run-1') AS t(event_id, execution_id)"
    )

    assert (
        _stale_scope_count(
            spark_session, same, "stale_same", ("event_id",), "execution_id", "run-1"
        )
        == 0
    )


# --- the scoped MERGE statement itself -----------------------------------------


class _ScopeFrame:
    """Minimal source frame; counts are supplied so the guards can be driven."""

    def __init__(self, columns, rows=2, unique=2, foreign=0) -> None:
        self.columns = list(columns)
        self._rows = rows
        self._unique = unique
        self._foreign = foreign
        self._mode = "rows"

    def count(self) -> int:
        return {"rows": self._rows, "unique": self._unique, "foreign": self._foreign}[self._mode]

    def select(self, *_c):
        clone = _ScopeFrame(self.columns, self._rows, self._unique, self._foreign)
        clone._mode = "unique"
        return clone

    def distinct(self):
        return self

    def where(self, _condition: str):
        clone = _ScopeFrame(self.columns, self._rows, self._unique, self._foreign)
        clone._mode = "foreign"
        return clone

    def join(self, *_a, **_k):
        clone = _ScopeFrame(self.columns, 0, 0, 0)
        clone._mode = "rows"
        return clone

    def limit(self, _n):
        return self

    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802 - Spark API spelling
        self.view = name


def _scope_spark(scope_before: int, scope_after: int, total: int = 10):
    spark = Mock()
    spark.catalog.tableExists.return_value = True
    scoped = Mock()
    scoped.count.side_effect = [scope_before, scope_after]
    target = Mock()
    target.count.return_value = total
    target.where.return_value = scoped
    target.join.return_value = Mock(count=Mock(return_value=0))
    spark.table.return_value = target
    return spark


def test_scoped_replacement_emits_a_delete_restricted_to_one_execution() -> None:
    """The DELETE must be scoped twice: not-matched-by-source AND this execution only."""
    spark = _scope_spark(scope_before=3, scope_after=1)
    frame = _ScopeFrame(["event_id", "execution_id", "station_id"], rows=1, unique=1)

    report = replace_execution_scope(
        spark,
        frame,
        "dbr_dev.parvinbadalov_urbanflow.gold_station_shortage",
        key_columns=("event_id",),
        execution_column="execution_id",
        execution_id="urbanflow-20260929T195132Z-r3",
    )

    sql = spark.sql.call_args.args[0]
    assert "WHEN NOT MATCHED BY SOURCE" in sql
    assert "t.`execution_id` = 'urbanflow-20260929T195132Z-r3'" in sql
    assert "THEN DELETE" in sql
    # One atomic statement, so the table is never briefly empty.
    assert spark.sql.call_count == 1
    assert sql.count("MERGE INTO") == 1
    assert report["scope_matches_source"] is True


def test_scoped_replacement_requires_the_execution_column_in_the_source() -> None:
    """gold_rebalancing_priority lacked execution_id, which is why this guard exists."""
    spark = Mock()
    frame = _ScopeFrame(["station_id", "observed_at"])  # no execution_id

    with pytest.raises(ValueError, match="scoped"):
        replace_execution_scope(
            spark,
            frame,
            "dbr_dev.schema.gold_rebalancing_priority",
            key_columns=("station_id",),
            execution_column="execution_id",
            execution_id="run-1",
        )
    spark.sql.assert_not_called()


def test_scoped_replacement_refuses_a_source_containing_another_execution() -> None:
    """A mixed source would delete rows it should not own."""
    spark = _scope_spark(scope_before=1, scope_after=1)
    frame = _ScopeFrame(["event_id", "execution_id"], rows=2, unique=2, foreign=1)

    with pytest.raises(ValueError, match="another execution"):
        replace_execution_scope(
            spark,
            frame,
            "dbr_dev.schema.t",
            key_columns=("event_id",),
            execution_column="execution_id",
            execution_id="run-1",
        )
    spark.sql.assert_not_called()


def test_scoped_replacement_refuses_an_unsafe_execution_id() -> None:
    spark = Mock()
    frame = _ScopeFrame(["event_id", "execution_id"])

    with pytest.raises(ValueError, match="Unsafe execution id"):
        replace_execution_scope(
            spark,
            frame,
            "dbr_dev.schema.t",
            key_columns=("event_id",),
            execution_column="execution_id",
            execution_id="run-1' OR '1'='1",
        )
    spark.sql.assert_not_called()


def test_scoped_replacement_refuses_duplicate_source_keys() -> None:
    spark = _scope_spark(scope_before=1, scope_after=1)
    frame = _ScopeFrame(["event_id", "execution_id"], rows=3, unique=2)

    with pytest.raises(ValueError, match="duplicate source keys"):
        replace_execution_scope(
            spark,
            frame,
            "dbr_dev.schema.t",
            key_columns=("event_id",),
            execution_column="execution_id",
            execution_id="run-1",
        )
    spark.sql.assert_not_called()
