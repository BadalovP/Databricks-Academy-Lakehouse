"""Tests for the legacy `gold_rebalancing_priority` rows that predate `execution_id`.

The live hazard these cover, in the order it would have unfolded during the correction run:

1. `gold_rebalancing_priority` holds 746 rows and has NO `execution_id` column.
2. `evolve_delta_schema` adds the column, so all 746 legacy rows become NULL.
3. The corrected source produces 657 rows. The MERGE matches 657 legacy rows on
   (station_id, observed_at) and updates them, including filling in their execution id.
4. The other 89 rows are NOT MATCHED BY SOURCE, but the delete is scoped by
   `t.execution_id = '<run>'`, and `NULL = '<run>'` is NULL rather than true - so they are
   never deleted.
5. `scope_matches_source` then compares 657 scoped rows against 657 source rows and
   reports success, while 89 stations that are actually out of service remain listed as
   actionable.

So the failure is silent, which is why the checks here look at the ENTIRE table rather
than the scoped subset.

What is real here and what is not: the 746-row table, the `ALTER TABLE ADD COLUMNS`, every
NULL and anti-join count, all the attribution guards and the whole-table verification run
against a genuine local Spark session. The physical MERGE and DELETE cannot run locally
because delta-spark's jars do not resolve in this environment, so the statements are
asserted at statement level and their effect is proven by the live corrected run. Crucially,
every check that DECIDES which rows are kept, attributed or deleted is executed for real.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest

from urbanflow.persistence import (
    _stale_scope_count,
    backfill_execution_id,
    evolve_delta_schema,
    replace_execution_scope,
    unassigned_execution_rows,
    verify_execution_scope,
)

RUN = "urbanflow-20260929T195132Z-r3"
OTHER_RUN = "urbanflow-20260930T101010Z-r9"

# The live table's shape before the correction: 12 columns, no execution_id.
LEGACY_COLUMNS = (
    "station_id",
    "station_name",
    "observed_at",
    "num_bikes_available",
    "num_docks_available",
    "capacity_estimate",
    "availability_status",
    "severity_points",
    "deficit_points",
    "size_points",
    "priority_score",
    "action",
)

# The real counts, taken from the recorded evidence rather than invented: the first Gold run
# wrote 746 priority rows, and the reference rule applied to the same 2,520 Bronze
# observations yields 657 shortages. 746 - 657 = 89 rows that no longer qualify.
LEGACY_ROWS = 746
CORRECTED_ROWS = 657
STALE_ROWS = LEGACY_ROWS - CORRECTED_ROWS


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-legacy-priority-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _base(spark: Any) -> Any:
    """746 priority rows generated entirely inside the JVM.

    `spark.range` plus Catalyst expressions is deliberate: building the frame from a Python
    list routes every row through the local Python worker, which resets its socket partway
    through on this platform and fails the whole suite. Nothing here needs a Python worker.

    Row ordering carries the meaning. Ids 0..656 are the rows that still qualify under the
    corrected rule, split in the same proportions the recorded medallion preview reported for
    this execution (LOW_BIKES 278, LOW_DOCKS 374, LOW_BIKES_AND_DOCKS 5). Ids 657..745 are the
    89 the old count-only rule wrongly called shortages, because it ignored whether the
    station was installed and renting.
    """
    from pyspark.sql import functions as F

    identifier = F.col("id")
    capacity = F.lit(20) + (identifier % F.lit(15))
    status = (
        F.when(identifier < F.lit(278), F.lit("LOW_BIKES"))
        .when(identifier < F.lit(652), F.lit("LOW_DOCKS"))
        .when(identifier < F.lit(CORRECTED_ROWS), F.lit("LOW_BIKES_AND_DOCKS"))
        .otherwise(F.lit("LOW_BIKES"))
    )
    return (
        spark.range(0, LEGACY_ROWS)
        .withColumn("station_id", F.format_string("st-%04d", identifier))
        .withColumn("station_name", F.format_string("Station %d", identifier))
        # The real snapshot's source timestamp, so the fixture carries a true value.
        .withColumn(
            "observed_at",
            F.to_timestamp(F.from_unixtime(F.lit(1790711443) + (identifier % F.lit(7)))),
        )
        .withColumn("num_bikes_available", F.when(status == F.lit("LOW_DOCKS"), 9).otherwise(0))
        .withColumn("num_docks_available", F.when(status == F.lit("LOW_BIKES"), 9).otherwise(0))
        .withColumn("capacity_estimate", capacity.cast("int"))
        .withColumn("availability_status", status)
        .withColumn(
            "severity_points",
            F.when(status == F.lit("LOW_BIKES_AND_DOCKS"), 2).otherwise(1),
        )
        .withColumn("deficit_points", F.lit(2))
        .withColumn("size_points", F.when(capacity >= F.lit(20), 1).otherwise(0))
        .withColumn("priority_score", F.lit(4))
        .withColumn(
            "action",
            F.when(status == F.lit("LOW_BIKES_AND_DOCKS"), F.lit("INSPECT_STATION")).otherwise(
                F.lit("DELIVER_BIKES")
            ),
        )
    )


def _legacy_frame(spark: Any) -> Any:
    """The live table's pre-correction contents: 746 rows, 12 columns, no execution_id."""
    return _base(spark).select(*LEGACY_COLUMNS)


def _corrected_source(spark: Any, *, execution_id: str = RUN) -> Any:
    """The 657 rows the corrected run produces, with execution_id first as in gold.py."""
    from pyspark.sql import functions as F

    return (
        _base(spark)
        .where(F.col("id") < F.lit(CORRECTED_ROWS))
        .withColumn("execution_id", F.lit(execution_id))
        .select("execution_id", *LEGACY_COLUMNS)
    )


def _stale_only(spark: Any) -> Any:
    """The 89 rows the corrected rule no longer produces."""
    from pyspark.sql import functions as F

    return _base(spark).where(F.col("id") >= F.lit(CORRECTED_ROWS)).select(*LEGACY_COLUMNS)


def _save(spark: Any, tmp_path, name: str, frame: Any) -> None:
    """Persist a real table at an explicit temp location.

    DROP TABLE leaves the warehouse directory behind, so a managed name would fail the next
    run with LOCATION_ALREADY_EXISTS, and a repo-relative warehouse would litter the tree.
    """
    location = (tmp_path / name).as_posix()
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    frame.write.mode("overwrite").option("path", location).saveAsTable(name)


# --- the legacy table, and what adding the column really does -------------------


@pytest.mark.spark
def test_the_legacy_priority_table_has_746_rows_and_no_execution_id(
    spark_session, tmp_path
) -> None:
    """Precondition, mirroring the live table: 746 rows across 12 columns."""
    _save(spark_session, tmp_path, "legacy_shape", _legacy_frame(spark_session))

    table = spark_session.table("legacy_shape")

    assert table.count() == LEGACY_ROWS
    assert len(table.schema.fields) == len(LEGACY_COLUMNS)
    assert "execution_id" not in {field.name for field in table.schema.fields}


@pytest.mark.spark
def test_adding_execution_id_leaves_every_legacy_row_unassigned(spark_session, tmp_path) -> None:
    """Step 2 of the hazard: the migration is necessary, and it creates 746 NULLs."""
    _save(spark_session, tmp_path, "legacy_nulls", _legacy_frame(spark_session))

    report = evolve_delta_schema(spark_session, _corrected_source(spark_session), "legacy_nulls")

    assert report["added_columns"] == ["execution_id"]
    assert len(spark_session.table("legacy_nulls").schema.fields) == len(LEGACY_COLUMNS) + 1
    # Every pre-existing row is now invisible to any execution-scoped predicate.
    assert unassigned_execution_rows(spark_session, "legacy_nulls", "execution_id") == LEGACY_ROWS


@pytest.mark.spark
def test_unassigned_legacy_rows_are_invisible_to_the_stale_row_count(
    spark_session, tmp_path
) -> None:
    """Why the old report looked clean: the anti-join is scoped, so NULL rows never appear."""
    _save(spark_session, tmp_path, "legacy_invisible", _legacy_frame(spark_session))
    evolve_delta_schema(spark_session, _corrected_source(spark_session), "legacy_invisible")

    stale = _stale_scope_count(
        spark_session,
        _corrected_source(spark_session),
        "legacy_invisible",
        ("station_id", "observed_at"),
        "execution_id",
        RUN,
    )

    # 89 rows genuinely need deleting, yet the scoped count reports none of them.
    assert stale == 0


# --- the silent pass, and the check that catches it ----------------------------


@pytest.mark.spark
def test_whole_table_verification_catches_the_pass_that_scope_matching_would_have_missed(
    spark_session, tmp_path
) -> None:
    """The exact end state the unfixed code would have produced, and why it must FAIL.

    657 rows updated and attributed, 89 left behind with a NULL execution id. The scoped
    comparison agrees (657 == 657), so `scope_matches_source` alone reports success.
    """
    from pyspark.sql import functions as F

    corrected = _corrected_source(spark_session)
    stranded = (
        _stale_only(spark_session)
        .withColumn("execution_id", F.lit(None).cast("string"))
        .select("execution_id", *LEGACY_COLUMNS)
    )
    _save(spark_session, tmp_path, "buggy_end_state", corrected.unionByName(stranded))

    report = verify_execution_scope(
        spark_session,
        corrected,
        "buggy_end_state",
        key_columns=("station_id", "observed_at"),
        execution_column="execution_id",
        execution_id=RUN,
    )

    # The misleading signal on its own.
    assert report["scope_matches_source"] is True
    # The whole-table audit refuses to call that a pass.
    assert report["unassigned_rows"] == STALE_ROWS
    assert report["no_unassigned_rows"] is False
    assert report["total_rows"] == LEGACY_ROWS
    assert report["status"] == "FAIL"


@pytest.mark.spark
def test_replacement_refuses_to_run_while_legacy_rows_are_unassigned(
    spark_session, tmp_path
) -> None:
    """Fail closed: the correction stops before writing rather than half-correcting."""
    _save(spark_session, tmp_path, "legacy_guard", _legacy_frame(spark_session))
    evolve_delta_schema(spark_session, _corrected_source(spark_session), "legacy_guard")
    spark = Mock(wraps=spark_session)
    spark.catalog = spark_session.catalog
    spark.table = spark_session.table

    with pytest.raises(ValueError, match="have no execution_id"):
        replace_execution_scope(
            spark,
            _corrected_source(spark_session),
            "legacy_guard",
            key_columns=("station_id", "observed_at"),
            execution_column="execution_id",
            execution_id=RUN,
        )

    spark.sql.assert_not_called()


# --- attributing the legacy rows from verified lineage -------------------------


def _shortage_lineage(spark: Any, *, rows: int = LEGACY_ROWS, execution_id: str = RUN) -> Any:
    """The sibling shortage table, which DOES carry execution_id for all 746 legacy rows."""
    from pyspark.sql import functions as F

    return (
        _base(spark)
        .where(F.col("id") < F.lit(rows))
        .withColumn("execution_id", F.lit(execution_id))
        .select("station_id", "observed_at", "execution_id")
    )


@pytest.mark.spark
def test_every_legacy_row_is_attributable_from_the_shortage_table(spark_session, tmp_path) -> None:
    """Provenance established: all 746 legacy rows match a shortage row of the same run."""
    _save(spark_session, tmp_path, "legacy_attr", _legacy_frame(spark_session))
    evolve_delta_schema(spark_session, _corrected_source(spark_session), "legacy_attr")
    recorded: list[str] = []

    def _capture(statement: str):
        recorded.append(statement)
        return Mock()

    spark = Mock()
    spark.catalog = spark_session.catalog
    spark.table = spark_session.table
    spark.sql = Mock(side_effect=_capture)

    report = backfill_execution_id(
        spark,
        "legacy_attr",
        lineage=_shortage_lineage(spark_session),
        key_columns=("station_id", "observed_at"),
        execution_column="execution_id",
    )

    assert report["null_rows_before"] == LEGACY_ROWS
    assert report["attributable_rows"] == LEGACY_ROWS
    assert report["unattributable_rows"] == 0
    statement = recorded[0]
    # Each row takes its id from the lineage row, never from a hardcoded current run.
    assert "t.`execution_id` = l.`execution_id`" in statement
    # And a row that already has an execution id is untouchable.
    assert "WHEN MATCHED AND t.`execution_id` IS NULL" in statement
    assert "WHEN NOT MATCHED" not in statement
    assert len(recorded) == 1


@pytest.mark.spark
def test_attribution_refuses_when_any_legacy_row_cannot_be_attributed(
    spark_session, tmp_path
) -> None:
    """The stop-and-report case: partial lineage must not be stretched over the gap."""
    _save(spark_session, tmp_path, "legacy_partial", _legacy_frame(spark_session))
    evolve_delta_schema(spark_session, _corrected_source(spark_session), "legacy_partial")
    spark = Mock()
    spark.catalog = spark_session.catalog
    spark.table = spark_session.table

    # Lineage covering only the rows that still qualify - exactly what reading the shortage
    # table AFTER its own correction would give, and the ordering trap gold.py avoids.
    with pytest.raises(ValueError, match="provenance could not be established for 89"):
        backfill_execution_id(
            spark,
            "legacy_partial",
            lineage=_shortage_lineage(spark_session, rows=CORRECTED_ROWS),
            key_columns=("station_id", "observed_at"),
            execution_column="execution_id",
        )

    spark.sql.assert_not_called()


@pytest.mark.spark
def test_attribution_refuses_ambiguous_lineage(spark_session, tmp_path) -> None:
    """A key claimed by two executions is refused, never resolved arbitrarily."""
    _save(spark_session, tmp_path, "legacy_ambiguous", _legacy_frame(spark_session))
    evolve_delta_schema(spark_session, _corrected_source(spark_session), "legacy_ambiguous")
    ours = _shortage_lineage(spark_session)
    theirs = _shortage_lineage(spark_session, rows=3, execution_id=OTHER_RUN)
    spark = Mock()
    spark.catalog = spark_session.catalog
    spark.table = spark_session.table

    with pytest.raises(ValueError, match="ambiguous"):
        backfill_execution_id(
            spark,
            "legacy_ambiguous",
            lineage=ours.unionByName(theirs),
            key_columns=("station_id", "observed_at"),
            execution_column="execution_id",
        )

    spark.sql.assert_not_called()


@pytest.mark.spark
def test_attribution_refuses_lineage_that_is_itself_unassigned(spark_session, tmp_path) -> None:
    """Lineage with a NULL execution id cannot launder one NULL into another."""
    from pyspark.sql import functions as F

    _save(spark_session, tmp_path, "legacy_nulllineage", _legacy_frame(spark_session))
    evolve_delta_schema(spark_session, _corrected_source(spark_session), "legacy_nulllineage")
    spark = Mock()
    spark.catalog = spark_session.catalog
    spark.table = spark_session.table

    with pytest.raises(ValueError, match="NULL execution_id"):
        backfill_execution_id(
            spark,
            "legacy_nulllineage",
            lineage=_shortage_lineage(spark_session).withColumn(
                "execution_id", F.lit(None).cast("string")
            ),
            key_columns=("station_id", "observed_at"),
            execution_column="execution_id",
        )

    spark.sql.assert_not_called()


@pytest.mark.spark
def test_attribution_counts_only_unassigned_rows_and_leaves_other_executions_alone(
    spark_session, tmp_path
) -> None:
    """Another execution's rows are not legacy rows and must not be reassigned."""
    from pyspark.sql import functions as F

    theirs = (
        _base(spark_session)
        .where(F.col("id") < F.lit(5))
        .withColumn("station_id", F.concat(F.lit("other-"), F.col("station_id")))
        .withColumn("execution_id", F.lit(OTHER_RUN))
        .select("execution_id", *LEGACY_COLUMNS)
    )
    ours = (
        _legacy_frame(spark_session)
        .withColumn("execution_id", F.lit(None).cast("string"))
        .select("execution_id", *LEGACY_COLUMNS)
    )
    _save(spark_session, tmp_path, "legacy_mixed", ours.unionByName(theirs))
    spark = Mock()
    spark.catalog = spark_session.catalog
    spark.table = spark_session.table
    spark.sql = Mock(return_value=Mock())

    report = backfill_execution_id(
        spark,
        "legacy_mixed",
        lineage=_shortage_lineage(spark_session),
        key_columns=("station_id", "observed_at"),
        execution_column="execution_id",
    )

    # The 5 rows that already carry an execution id are not in scope for attribution.
    assert report["null_rows_before"] == LEGACY_ROWS
    assert report["attributable_rows"] == LEGACY_ROWS


def test_attribution_requires_the_column_to_exist_first() -> None:
    """Ordering guard: attribute only after the schema migration has added the column."""
    spark = Mock()
    spark.catalog.tableExists.return_value = True
    spark.table.return_value = Mock(schema=Mock(fields=[Mock(**{"name": "station_id"})]))

    with pytest.raises(ValueError, match="evolve_delta_schema"):
        backfill_execution_id(
            spark,
            "dbr_dev.schema.gold_rebalancing_priority",
            lineage=Mock(),
            key_columns=("station_id",),
            execution_column="execution_id",
        )


def test_attribution_is_a_no_op_when_no_row_is_unassigned() -> None:
    """A correctly populated table needs no attribution, and the lineage is never read."""
    spark = Mock()
    spark.catalog.tableExists.return_value = True
    field = Mock()
    field.name = "execution_id"
    spark.table.return_value = Mock(
        schema=Mock(fields=[field]),
        where=Mock(return_value=Mock(count=Mock(return_value=0))),
    )
    lineage = Mock()

    report = backfill_execution_id(
        spark,
        "dbr_dev.schema.t",
        lineage=lineage,
        key_columns=("station_id",),
        execution_column="execution_id",
    )

    assert report["null_rows_before"] == 0
    assert report["backfilled_rows"] == 0
    lineage.select.assert_not_called()
    spark.sql.assert_not_called()


# --- once attributed, the stale rows become deletable and the table is correct ---


@pytest.mark.spark
def test_once_attributed_the_89_stale_rows_become_visible_to_the_delete(
    spark_session, tmp_path
) -> None:
    """The point of the whole migration: the stale rows enter the delete's scope."""
    from pyspark.sql import functions as F

    attributed = (
        _legacy_frame(spark_session)
        .withColumn("execution_id", F.lit(RUN))
        .select("execution_id", *LEGACY_COLUMNS)
    )
    _save(spark_session, tmp_path, "legacy_attributed", attributed)

    stale = _stale_scope_count(
        spark_session,
        _corrected_source(spark_session),
        "legacy_attributed",
        ("station_id", "observed_at"),
        "execution_id",
        RUN,
    )

    assert stale == STALE_ROWS


@pytest.mark.spark
def test_the_corrected_table_exactly_matches_its_source(spark_session, tmp_path) -> None:
    """The end state the correction must reach: 657 rows, nothing extra, nothing missing."""
    corrected = _corrected_source(spark_session)
    _save(spark_session, tmp_path, "corrected_end_state", corrected)

    report = verify_execution_scope(
        spark_session,
        corrected,
        "corrected_end_state",
        key_columns=("station_id", "observed_at"),
        execution_column="execution_id",
        execution_id=RUN,
    )

    assert report["status"] == "PASS"
    assert report["total_rows"] == CORRECTED_ROWS
    assert report["scope_rows"] == CORRECTED_ROWS
    assert report["missing_from_table"] == 0
    assert report["unexpected_in_table"] == 0
    assert report["unassigned_rows"] == 0
    assert report["other_execution_rows"] == 0


@pytest.mark.spark
def test_another_executions_rows_survive_the_correction(spark_session, tmp_path) -> None:
    """Scoping means a second execution's rows are untouched and not counted as failures."""
    from pyspark.sql import functions as F

    corrected = _corrected_source(spark_session)
    theirs = (
        _base(spark_session)
        .where(F.col("id") < F.lit(11))
        .withColumn("station_id", F.concat(F.lit("other-"), F.col("station_id")))
        .withColumn("execution_id", F.lit(OTHER_RUN))
        .select("execution_id", *LEGACY_COLUMNS)
    )
    _save(spark_session, tmp_path, "corrected_multi", corrected.unionByName(theirs))

    report = verify_execution_scope(
        spark_session,
        corrected,
        "corrected_multi",
        key_columns=("station_id", "observed_at"),
        execution_column="execution_id",
        execution_id=RUN,
    )

    assert report["status"] == "PASS"
    assert report["other_execution_rows"] == 11
    assert report["total_rows"] == CORRECTED_ROWS + 11
    assert report["scope_rows"] == CORRECTED_ROWS


# --- how Gold sequences the correction ------------------------------------------


def _gold_frames() -> dict[str, Any]:
    return {name: Mock(name=name) for name in ("dimension", "fact", "daily_summary")}


GOLD_TABLES = {
    "dimension": "cat.sch.dim_station",
    "fact": "cat.sch.fact_station_availability",
    "daily_summary": "cat.sch.gold_daily_station_summary",
    "shortages": "cat.sch.gold_station_shortage",
    "priorities": "cat.sch.gold_rebalancing_priority",
}


def test_gold_attributes_legacy_priorities_before_correcting_the_shortage_table(
    monkeypatch,
) -> None:
    """The ordering trap, guarded.

    The priority table's legacy rows are attributed from the shortage table, which shares
    their grain. But the shortage table is itself about to lose the 89 rows that no longer
    qualify - so if its correction ran first, the evidence for exactly the rows needing
    attribution would be gone and every one of them would look unattributable.
    """
    from urbanflow import gold as gold_module
    from urbanflow import persistence

    order: list[str] = []

    def _fake_backfill(spark, table_name, **_kwargs):
        order.append(f"backfill:{table_name}")
        return {"null_rows_before": LEGACY_ROWS, "backfilled_rows": LEGACY_ROWS}

    def _fake_replace(spark, frame, table_name, **_kwargs):
        order.append(f"replace:{table_name}")
        return {"source_rows": CORRECTED_ROWS}

    monkeypatch.setattr(persistence, "backfill_execution_id", _fake_backfill)
    monkeypatch.setattr(persistence, "replace_execution_scope", _fake_replace)
    monkeypatch.setattr(persistence, "merge_delta_table", lambda *a, **k: {})
    monkeypatch.setattr(
        persistence, "evolve_delta_schema", lambda *a, **k: {"added_columns": ["execution_id"]}
    )
    monkeypatch.setattr(persistence, "unassigned_execution_rows", lambda *a, **k: LEGACY_ROWS)
    spark = Mock()
    spark.catalog.tableExists.return_value = True

    report = gold_module.persist_gold_outputs(
        spark,
        **_gold_frames(),
        shortages=Mock(name="shortages"),
        priorities=Mock(name="priorities"),
        table_names=GOLD_TABLES,
        execution_id=RUN,
    )

    assert order.index(f"backfill:{GOLD_TABLES['priorities']}") < order.index(
        f"replace:{GOLD_TABLES['shortages']}"
    )
    # The lineage comes from the shortage table, read before that table is corrected.
    spark.table.assert_any_call(GOLD_TABLES["shortages"])
    assert report["priorities"]["legacy_backfill"]["backfilled_rows"] == LEGACY_ROWS


def test_gold_refuses_when_legacy_priorities_have_no_lineage_to_attribute_them(
    monkeypatch,
) -> None:
    """Stop and report: unassigned rows with no sibling evidence must not be adopted."""
    from urbanflow import gold as gold_module
    from urbanflow import persistence

    monkeypatch.setattr(persistence, "evolve_delta_schema", lambda *a, **k: {})
    monkeypatch.setattr(persistence, "unassigned_execution_rows", lambda *a, **k: LEGACY_ROWS)
    spark = Mock()
    spark.catalog.tableExists.return_value = False  # the shortage table is absent

    with pytest.raises(ValueError, match="provenance cannot be established"):
        gold_module.persist_gold_outputs(
            spark,
            **_gold_frames(),
            shortages=Mock(),
            priorities=Mock(),
            table_names=GOLD_TABLES,
            execution_id=RUN,
        )


@pytest.mark.spark
def test_a_second_corrected_execution_changes_nothing(spark_session, tmp_path) -> None:
    """Idempotency of the corrected result: nothing stale, nothing missing, nothing added."""
    corrected = _corrected_source(spark_session)
    _save(spark_session, tmp_path, "corrected_repeat", corrected)

    stale = _stale_scope_count(
        spark_session,
        corrected,
        "corrected_repeat",
        ("station_id", "observed_at"),
        "execution_id",
        RUN,
    )
    report = verify_execution_scope(
        spark_session,
        corrected,
        "corrected_repeat",
        key_columns=("station_id", "observed_at"),
        execution_column="execution_id",
        execution_id=RUN,
    )

    assert stale == 0
    assert report["status"] == "PASS"
    assert report["total_rows"] == CORRECTED_ROWS
    assert unassigned_execution_rows(spark_session, "corrected_repeat", "execution_id") == 0
