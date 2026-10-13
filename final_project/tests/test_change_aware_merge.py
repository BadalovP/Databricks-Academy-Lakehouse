"""Change-aware MERGE: unchanged replays write nothing, audit stamps survive, real changes apply.

The first DEV replay (run 896549012431425) updated every matched row although no business value
changed, and refreshed `ingested_at` on historical Silver. Delta itself cannot run in this local
environment (delta-spark's jars do not resolve), so these tests evaluate the exact predicate text
the MERGE uses in real local Spark, and simulate MERGE semantics with that same predicate.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from urbanflow import historical, persistence, weather
from urbanflow.persistence import (
    build_merge_sql,
    changed_row_predicate,
    merge_metrics,
    present_columns,
)

COLUMNS = ("execution_id", "ride_id", "station", "minutes", "ingested_at")
KEYS = ("execution_id", "ride_id")
AUDIT = ("ingested_at",)


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-change-aware-merge")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _sql_literal(value) -> str:
    if value is None:
        return "CAST(NULL AS STRING)"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _frame(spark, rows):
    """Built with Spark SQL VALUES: local createDataFrame is unreliable here (conftest_spark)."""
    names = "execution_id, ride_id, station, minutes, ingested_at"
    if not rows:
        return spark.sql(
            "SELECT CAST(NULL AS STRING) AS execution_id, CAST(NULL AS STRING) AS ride_id, "
            "CAST(NULL AS STRING) AS station, CAST(NULL AS INT) AS minutes, "
            "CAST(NULL AS STRING) AS ingested_at WHERE 1 = 0"
        )
    values = ", ".join("(" + ", ".join(_sql_literal(v) for v in row) + ")" for row in rows)
    return spark.sql(f"SELECT * FROM VALUES {values} AS v({names})")


def _simulate_merge(spark, target, source, *, scope_execution=None):
    """MERGE semantics driven by the production predicate and update/insert rules."""
    from pyspark.sql import functions as F

    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in KEYS)
    predicate = changed_row_predicate(COLUMNS, KEYS, AUDIT)
    t, s = target.alias("t"), source.alias("s")
    joined = t.join(s, F.expr(condition), "inner")
    changed = joined.where(F.expr(predicate))
    unchanged = joined.where(~F.expr(predicate))
    updated = changed.select(
        *[F.col(f"t.{c}" if c in KEYS or c in AUDIT else f"s.{c}").alias(c) for c in COLUMNS]
    )
    kept = unchanged.select(*[F.col(f"t.{c}").alias(c) for c in COLUMNS])
    inserted = s.join(t, F.expr(condition), "left_anti").select(*COLUMNS)
    untouched = t.join(s, F.expr(condition), "left_anti").select(*COLUMNS)
    if scope_execution is not None:  # replace_execution_scope: stale rows of THIS execution go
        untouched = untouched.where(F.col("execution_id") != F.lit(scope_execution))
    result = updated.unionByName(kept).unionByName(inserted).unionByName(untouched)
    return result, changed.count(), inserted.count()


def _rows(frame):
    return sorted(tuple(r) for r in frame.collect())


# --- the predicate and the SQL ------------------------------------------------------------


def test_only_business_columns_are_compared_null_safely() -> None:
    predicate = changed_row_predicate(COLUMNS, KEYS, AUDIT)
    assert predicate == "NOT (t.`station` <=> s.`station` AND t.`minutes` <=> s.`minutes`)"
    assert changed_row_predicate(("k", "audit"), ("k",), ("audit",)) is None


def test_preserved_columns_are_inserted_but_never_compared_or_updated() -> None:
    sql = build_merge_sql(table_name="c.s.t", source_view="v", columns=COLUMNS, key_columns=KEYS,
                          preserve_columns=AUDIT)  # fmt: skip
    matched = sql[sql.index("WHEN MATCHED") : sql.index("WHEN NOT MATCHED")]
    assert "WHEN MATCHED AND NOT (" in matched
    assert "ingested_at" not in matched
    assert "INSERT (`execution_id`, `ride_id`, `station`, `minutes`, `ingested_at`)" in sql


def test_a_table_with_only_keys_and_audit_columns_has_no_update_clause() -> None:
    sql = build_merge_sql(table_name="c.s.t", source_view="v", columns=("k", "audit"),
                          key_columns=("k",), preserve_columns=("audit",))  # fmt: skip
    assert "WHEN MATCHED" not in sql and "WHEN NOT MATCHED THEN INSERT" in sql


@pytest.mark.parametrize(
    "preserve, message",
    [(("ride_id",), "key column cannot be preserved"), (("absent",), "absent from the source"),
     (("bad name",), "Invalid Delta column name")],
)  # fmt: skip
def test_preserved_columns_are_validated(preserve, message) -> None:
    with pytest.raises(ValueError, match=message):
        build_merge_sql(table_name="c.s.t", source_view="v", columns=COLUMNS, key_columns=KEYS,
                        preserve_columns=preserve)  # fmt: skip


def test_scoped_replacement_is_change_aware_and_still_deletes_only_its_execution() -> None:
    from tests.test_persistence_migration import _scope_spark, _ScopeFrame

    spark = _scope_spark(scope_before=2, scope_after=2)
    frame = _ScopeFrame(["trip_date", "execution_id", "trips", "audit"], rows=2, unique=2)
    persistence.replace_execution_scope(
        spark, frame, "c.s.gold", key_columns=("trip_date",), execution_column="execution_id",
        execution_id="exec-1", preserve_columns=("audit",),
    )  # fmt: skip
    sql = spark.sql.call_args.args[0]
    assert "WHEN MATCHED AND NOT (t.`execution_id` <=> s.`execution_id` AND " in sql
    assert "t.`audit` = s.`audit`" not in sql
    assert "WHEN NOT MATCHED BY SOURCE AND t.`execution_id` = 'exec-1' THEN DELETE" in sql


def test_merge_metrics_are_read_from_the_delta_result_and_never_fail_a_write() -> None:
    row = SimpleNamespace(asDict=lambda: {"num_affected_rows": 3, "num_updated_rows": 0,
                                          "num_deleted_rows": 1, "num_inserted_rows": 2})  # fmt: skip
    assert merge_metrics(SimpleNamespace(first=lambda: row)) == {
        "num_affected_rows": 3, "num_updated_rows": 0, "num_deleted_rows": 1,
        "num_inserted_rows": 2,
    }  # fmt: skip
    assert merge_metrics(Mock()) == {}
    assert merge_metrics(SimpleNamespace(first=lambda: None)) == {}


def test_present_columns_tolerates_frames_without_a_column_list() -> None:
    assert present_columns(SimpleNamespace(columns=["a", "ingested_at"]), AUDIT) == AUDIT
    assert present_columns(SimpleNamespace(columns=["a"]), AUDIT) == ()
    assert present_columns(Mock(), AUDIT) == ()


# --- MERGE semantics in real Spark -------------------------------------------------------

STORED = [("e1", "r1", "A", 10, "t0"), ("e1", "r2", None, 5, "t0"), ("e2", "r1", "B", 7, "t0")]


def test_an_unchanged_replay_updates_nothing_and_keeps_ingested_at(spark_session) -> None:
    target = _frame(spark_session, STORED)
    replay = _frame(spark_session, [("e1", "r1", "A", 10, "t9"), ("e1", "r2", None, 5, "t9")])
    result, updated, inserted = _simulate_merge(spark_session, target, replay)
    assert (updated, inserted) == (0, 0)  # NULL station <=> NULL station is "unchanged"
    assert _rows(result) == _rows(target)  # every stored ingested_at is "t0"


def test_a_genuine_change_updates_business_values_but_keeps_the_first_ingestion(
    spark_session,
) -> None:
    target = _frame(spark_session, STORED)
    source = _frame(spark_session, [("e1", "r1", "A", 11, "t9"), ("e1", "r2", "C", 5, "t9")])
    result, updated, inserted = _simulate_merge(spark_session, target, source)
    assert (updated, inserted) == (2, 0)  # a value change and NULL -> value both count
    rows = {(r[0], r[1]): r for r in result.collect()}
    assert rows[("e1", "r1")][3] == 11 and rows[("e1", "r1")][4] == "t0"
    assert rows[("e1", "r2")][2] == "C" and rows[("e1", "r2")][4] == "t0"


def test_new_records_insert_with_their_own_ingestion_stamp(spark_session) -> None:
    target = _frame(spark_session, STORED)
    source = _frame(spark_session, [("e1", "r3", "D", 3, "t9")])
    result, updated, inserted = _simulate_merge(spark_session, target, source)
    assert (updated, inserted) == (0, 1)
    assert ("e1", "r3", "D", 3, "t9") in _rows(result)


def test_scoped_replacement_removes_stale_rows_of_one_execution_only(spark_session) -> None:
    target = _frame(spark_session, STORED)
    source = _frame(spark_session, [("e1", "r1", "A", 10, "t9")])  # r2 is stale for e1
    result, updated, _ = _simulate_merge(spark_session, target, source, scope_execution="e1")
    assert updated == 0
    assert _rows(result) == sorted([("e1", "r1", "A", 10, "t0"), ("e2", "r1", "B", 7, "t0")])


def test_an_empty_source_is_supported(spark_session) -> None:
    target = _frame(spark_session, STORED)
    empty = _frame(spark_session, [])
    result, updated, inserted = _simulate_merge(spark_session, target, empty)
    assert (updated, inserted) == (0, 0) and _rows(result) == _rows(target)
    scoped, _, _ = _simulate_merge(spark_session, target, empty, scope_execution="e1")
    assert _rows(scoped) == [("e2", "r1", "B", 7, "t0")]  # other executions untouched


# --- the callers pass their audit columns --------------------------------------------------


def test_historical_and_weather_callers_preserve_their_audit_stamps(monkeypatch) -> None:
    calls: dict[str, tuple] = {}
    monkeypatch.setattr(persistence, "evolve_delta_schema", lambda *a, **k: {})
    monkeypatch.setattr(
        persistence, "merge_delta_table",
        lambda spark, frame, table, *, key_columns, preserve_columns=(): calls.__setitem__(
            table, preserve_columns) or {},
    )  # fmt: skip
    monkeypatch.setattr(persistence, "replace_execution_scope", lambda *a, **k: {})
    trips = SimpleNamespace(columns=["execution_id", "ride_id", "ingested_at"])
    historical.persist_historical_outputs(
        Mock(), valid=trips, quarantine=trips, duplicates=trips, daily_demand=Mock(),
        table_names={"trips": "t", "quarantine": "q", "duplicates": "d", "daily_demand": "g"},
        execution_id="exec-1",
    )  # fmt: skip
    assert calls == {"t": ("ingested_at",), "q": ("ingested_at",), "d": ("ingested_at",)}
    calls.clear()
    hourly = SimpleNamespace(columns=["weather_grid_label", "weather_hour", "retrieved_at"])
    weather.persist_weather_outputs(
        Mock(), weather=hourly, demand_summary=Mock(),
        table_names={"weather": "w", "demand_summary": "s"}, execution_id="w-1",
    )  # fmt: skip
    assert calls == {"w": ("retrieved_at",)}
