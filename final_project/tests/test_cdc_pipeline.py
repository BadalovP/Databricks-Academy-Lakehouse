"""Station-maintenance CDC: SQL safety, notebook ordering, and a full snapshot replay.

The replay uses the Python CDC model (``urbanflow.cdc``) as the executable specification of what
the notebook's MERGEs do with Delta Change Data Feed. Delta itself is exercised only in a
workspace; these tests prove the logic and the statement shapes, not Databricks behaviour.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from urbanflow import cdc_sql
from urbanflow.cdc import (
    ChangeType,
    apply_cdf,
    load_snapshot_csv,
    reconcile,
    snapshot_change_events,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOTS = PROJECT_ROOT / "fixtures" / "maintenance"
NOTEBOOK = PROJECT_ROOT / "notebooks" / "11_federation_cdc.py"
TABLES = cdc_sql.CdcTables("workspace", "urbanflow_final_dev")


def _snapshot(number: int) -> dict:
    return load_snapshot_csv(SNAPSHOTS / f"station_maintenance_batch_{number:03d}.csv")


# --- the fictional snapshots and the CDC model --------------------------------------------


def test_snapshots_are_labelled_fictional_and_use_real_sample_station_ids(sample_dir) -> None:
    import json

    readme = (SNAPSHOTS / "README.md").read_text(encoding="utf-8")
    assert "FICTIONAL, SYNTHETIC DATA" in readme
    stations = json.loads((sample_dir / "station_information.sample.json").read_text("utf-8"))
    known = {station["station_id"] for station in stations["data"]["stations"]}
    for number in (1, 2, 3):
        assert {row["station_id"] for row in _snapshot(number).values()} <= known


def test_each_snapshot_produces_exactly_the_documented_change_events() -> None:
    first = snapshot_change_events({}, _snapshot(1), commit_version=1, commit_timestamp="t1")
    second = snapshot_change_events(
        _snapshot(1), _snapshot(2), commit_version=2, commit_timestamp="t2"
    )
    third = snapshot_change_events(
        _snapshot(2), _snapshot(3), commit_version=3, commit_timestamp="t3"
    )

    assert [e.change_type for e in first] == [ChangeType.INSERT] * 5
    assert sorted((e.maintenance_id, e.change_type.value) for e in second) == [
        ("MT-1001", "update_postimage"),
        ("MT-1001", "update_preimage"),
        ("MT-1002", "update_postimage"),
        ("MT-1002", "update_preimage"),
        ("MT-1003", "delete"),
        ("MT-1006", "insert"),
    ]
    assert third == []  # an identical snapshot produces no change at all


def _replay(batches: list[int]) -> tuple[dict, list]:
    target, checkpoint, history, previous = {}, -1, [], {}
    for version, number in enumerate(batches, start=1):
        current = _snapshot(number)
        events = snapshot_change_events(
            previous, current, commit_version=version, commit_timestamp=f"t{version}"
        )
        result = apply_cdf(target, events, checkpoint=checkpoint)
        target, checkpoint, previous = result.target, result.checkpoint, current
        history.extend(result.history)
    return target, history


def _comparable(snapshot: dict) -> dict:
    return {key: {**row} for key, row in snapshot.items()}


def test_incremental_replay_ends_equal_to_the_latest_snapshot() -> None:
    target, history = _replay([1, 2, 3])
    reconciliation = reconcile(_comparable(_snapshot(3)), target)

    assert reconciliation.matches, reconciliation
    assert sorted(target) == ["MT-1001", "MT-1002", "MT-1004", "MT-1005", "MT-1006"]
    assert target["MT-1001"]["status"] == "CLOSED"
    assert "MT-1003" not in target  # the cancelled ticket was deleted
    # 5 inserts, then 2 pre-images + 2 post-images + 1 delete + 1 insert; batch 3 adds nothing.
    assert len(history) == 11
    assert len({event.change_id for event in history}) == 11


def test_processing_one_range_or_two_ranges_gives_the_same_target() -> None:
    one_range = apply_cdf(
        {},
        snapshot_change_events({}, _snapshot(1), commit_version=1, commit_timestamp="t1")
        + snapshot_change_events(
            _snapshot(1), _snapshot(2), commit_version=2, commit_timestamp="t2"
        ),
        checkpoint=0,
    )
    two_ranges, _ = _replay([1, 2])
    assert one_range.target == two_ranges


def test_replaying_an_already_processed_range_changes_nothing() -> None:
    events = snapshot_change_events({}, _snapshot(1), commit_version=1, commit_timestamp="t1")
    first = apply_cdf({}, events, checkpoint=0)
    # A failure after the MERGEs but before the checkpoint replays the same range.
    replay = apply_cdf(first.target, events, checkpoint=0)
    assert replay.target == first.target
    assert (replay.inserted, replay.updated, replay.deleted) == (0, 0, 0)


def test_duplicate_snapshot_keys_are_refused(tmp_path) -> None:
    bad = tmp_path / "dup.csv"
    bad.write_text(
        "maintenance_id,station_id,status,severity,updated_at\n"
        "MT-1,s1,OPEN,LOW,2026-10-12T00:00:00Z\nMT-1,s1,OPEN,LOW,2026-10-12T00:00:00Z\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate"):
        load_snapshot_csv(bad)


# --- the SQL statements -------------------------------------------------------------------


def _all_statements() -> list[str]:
    return [
        *cdc_sql.create_tables(TABLES),
        cdc_sql.merge_snapshot(TABLES),
        cdc_sql.checkpoint_version(TABLES),
        cdc_sql.changes_view(TABLES, start_version=1, end_version=3),
        cdc_sql.merge_history(TABLES),
        cdc_sql.latest_changes_view(),
        cdc_sql.merge_target(TABLES),
        cdc_sql.reconciliation(TABLES, processed_version=3),
        cdc_sql.advance_checkpoint(TABLES, processed_version=3),
        cdc_sql.snapshot_contract(),
    ]


def test_no_statement_is_destructive() -> None:
    for statement in _all_statements():
        upper = statement.upper()
        for forbidden in ("DROP ", "TRUNCATE", "CREATE OR REPLACE TABLE", "INSERT OVERWRITE"):
            assert forbidden not in upper, statement[:80]


def test_the_source_has_change_data_feed_from_its_first_version() -> None:
    source_ddl = cdc_sql.create_tables(TABLES)[0]
    assert "station_maintenance_source" in source_ddl
    assert "delta.enableChangeDataFeed = true" in source_ddl
    assert all(
        statement.startswith("CREATE TABLE IF NOT EXISTS")
        for statement in cdc_sql.create_tables(TABLES)
    )


def test_merges_skip_unchanged_rows_so_cdf_holds_only_real_changes() -> None:
    for statement in (cdc_sql.merge_snapshot(TABLES), cdc_sql.merge_target(TABLES)):
        assert "WHEN MATCHED AND NOT (" in statement
        assert "t.status <=> s.status" in statement
    assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in cdc_sql.merge_snapshot(TABLES)
    assert "WHEN MATCHED AND s._change_type = 'delete' THEN DELETE" in cdc_sql.merge_target(TABLES)


def test_change_reads_are_bounded_and_history_is_keyed_by_a_deterministic_event_id() -> None:
    view = cdc_sql.changes_view(TABLES, start_version=4, end_version=7)
    assert "table_changes('workspace.urbanflow_final_dev.station_maintenance_source', 4, 7)" in view
    assert (
        "sha2(concat_ws('|', maintenance_id, CAST(_commit_version AS STRING), _change_type)" in view
    )
    assert "ON t.change_id = s.change_id" in cdc_sql.merge_history(TABLES)
    assert "WHEN MATCHED" not in cdc_sql.merge_history(TABLES)  # history is append-once
    with pytest.raises(ValueError):
        cdc_sql.changes_view(TABLES, start_version=5, end_version=4)
    with pytest.raises(ValueError):
        cdc_sql.changes_view(TABLES, start_version=-1, end_version=4)


def test_latest_change_ignores_preimages_and_prefers_the_final_event() -> None:
    view = cdc_sql.latest_changes_view()
    assert "_change_type IN ('insert', 'update_postimage', 'delete')" in view
    assert "ORDER BY _commit_version DESC" in view


def test_reconciliation_compares_values_both_ways_at_the_processed_version() -> None:
    sql = cdc_sql.reconciliation(TABLES, processed_version=9)
    assert sql.count("VERSION AS OF 9") == 3
    assert sql.count("EXCEPT ALL") == 2
    assert "events_missing_from_history" in sql and "LEFT ANTI JOIN" in sql
    assert "0 AS events_missing_from_history" in cdc_sql.reconciliation(
        TABLES, processed_version=9, check_history=False
    )


def test_the_checkpoint_only_moves_forward() -> None:
    sql = cdc_sql.advance_checkpoint(TABLES, processed_version=3)
    assert "WHEN MATCHED AND t.processed_version < s.processed_version THEN UPDATE" in sql
    with pytest.raises(ValueError):
        cdc_sql.advance_checkpoint(TABLES, processed_version=-1)


@pytest.mark.parametrize("catalog, schema", [("work space", "s"), ("w", "s`; DROP"), ("", "s")])
def test_identifiers_are_validated(catalog, schema) -> None:
    with pytest.raises(ValueError):
        cdc_sql.CdcTables(catalog, schema)


# --- the notebook's ordering contract ------------------------------------------------------


def _code() -> str:
    cells = NOTEBOOK.read_text(encoding="utf-8").split("# COMMAND ----------")
    return "\n".join(cell for cell in cells if "# MAGIC %md" not in cell)


def test_reconciliation_runs_before_the_checkpoint_and_failure_stops_it() -> None:
    code = _code()
    reconcile_at = code.index("cdc_sql.reconciliation(")
    raise_at = code.index("raise AssertionError")
    advance_at = code.index("cdc_sql.advance_checkpoint(")
    assert reconcile_at < raise_at < advance_at


def test_the_notebook_processes_only_the_new_range_and_never_writes_externally() -> None:
    code = _code()
    assert "start_version=checkpoint + 1, end_version=latest_version" in code
    assert "if new_versions:" in code
    assert "select_statement(source)" in code  # the only external statement
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|MERGE)\b[^\n]*federation", code, re.I)
    assert code.index('dbutils.notebook.exit(json.dumps({"status": "SKIPPED"') < code.index(
        "assert_serverless_compute(spark.conf.get)"
    )


def test_the_bundle_syncs_the_snapshots_the_notebook_reads() -> None:
    import yaml

    bundle = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text(encoding="utf-8"))
    assert "fixtures/maintenance/**" in bundle["sync"]["include"]
    assert "fixtures/samples/**" in bundle["sync"]["include"]


# --- the no-op guard: an identical snapshot creates no source commit ------------------------
# Step 3 run 605056708110586: re-merging the unchanged batch_001 left an empty source commit,
# so the checkpoint advanced 1 -> 2 over a version with no events.


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-cdc-noop")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


_ROWS = [
    ("MT-1", "s1", "OPEN", None, "2026-10-12 07:00:00"),
    ("MT-2", "s2", "OPEN", "HIGH", "2026-10-12 07:05:00"),
]


def _lit(value):
    return "CAST(NULL AS STRING)" if value is None else f"'{value}'"


def _view(spark, name, rows):
    """A temp view built with SQL VALUES (local createDataFrame is unreliable here)."""
    if not rows:
        spark.sql(
            f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT CAST(NULL AS STRING) AS maintenance_id, "
            "CAST(NULL AS STRING) AS station_id, CAST(NULL AS STRING) AS status, "
            "CAST(NULL AS STRING) AS severity, CAST(NULL AS TIMESTAMP) AS updated_at WHERE 1 = 0"
        )
        return
    values = ", ".join(
        f"({_lit(m)}, {_lit(s)}, {_lit(st)}, {_lit(sev)}, TIMESTAMP'{u}')"
        for m, s, st, sev, u in rows
    )
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM VALUES {values} "
        "AS v(maintenance_id, station_id, status, severity, updated_at)"
    )


def _differences(spark, snapshot_rows, source_rows):
    _view(spark, "snap_v", snapshot_rows)
    _view(spark, "src_v", source_rows)
    row = spark.sql(cdc_sql._two_way_difference("snap_v", "src_v")).first().asDict()
    return (row["snapshot_rows_not_in_source"], row["source_rows_not_in_snapshot"]), row


_INSERT = _ROWS + [("MT-3", "s3", "OPEN", "LOW", "2026-10-12 07:10:00")]
_UPDATE = [_ROWS[0], ("MT-2", "s2", "CLOSED", "HIGH", "2026-10-12 08:00:00")]
_NULL_TO_VALUE = [("MT-1", "s1", "OPEN", "LOW", "2026-10-12 07:00:00"), _ROWS[1]]


@pytest.mark.parametrize(
    "snapshot, expected, merge",
    [
        (_ROWS, (0, 0), False),  # identical, including a NULL severity on both sides
        (_INSERT, (1, 0), True),
        (_UPDATE, (1, 1), True),
        ([_ROWS[0]], (0, 1), True),  # a deleted ticket
        (_NULL_TO_VALUE, (1, 1), True),  # NULL -> value is a change
        ([], (0, 2), True),  # every row deleted
    ],
)
def test_the_two_way_comparison_decides_the_merge(spark_session, snapshot, expected, merge):
    counts, row = _differences(spark_session, snapshot, _ROWS)
    assert counts == expected
    assert cdc_sql.snapshot_requires_merge(row) is merge


def test_two_empty_datasets_are_identical(spark_session) -> None:
    counts, row = _differences(spark_session, [], [])
    assert counts == (0, 0)
    assert cdc_sql.snapshot_requires_merge(row) is False


def test_the_comparison_reads_the_current_source_and_never_writes() -> None:
    sql = cdc_sql.snapshot_differences(TABLES)
    assert sql.count("EXCEPT ALL") == 2 and "VERSION AS OF" not in sql
    assert f"FROM {cdc_sql.SNAPSHOT_VIEW}" in sql and f"FROM {TABLES.source}" in sql
    for column in cdc_sql.COLUMNS:
        assert sql.count(column) >= 4
    for forbidden in ("MERGE", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE"):
        assert not re.search(rf"\b{forbidden}\b", sql.upper()), forbidden


def test_the_notebook_skips_only_the_source_merge_and_keeps_version_driven_processing() -> None:
    code = _code()
    compare = code.index("cdc_sql.snapshot_differences(tables)")
    guard = code.index("if source_merged:")
    merge = code.index("spark.sql(cdc_sql.merge_snapshot(tables))")
    latest = code.index("latest_version = int(")
    assert compare < guard < merge < latest
    assert code.count("cdc_sql.merge_snapshot(tables)") == 1  # only inside the guard
    # Pending versions are still scanned: the range depends on versions, not the comparison.
    scan = code.index("if latest_version > checkpoint:")
    decide = code.index("new_versions = cdc_sql.range_has_changes(event_counts)")
    apply = code.index("spark.sql(cdc_sql.merge_history(tables))")
    assert latest < scan < decide < apply
    assert "new_versions = latest_version > checkpoint" not in code
    assert "if new_versions:" in code and "if source_merged and" not in code
    assert '"checkpoint_after": latest_version if new_versions else checkpoint' in code


def test_a_range_without_cdf_rows_is_not_work() -> None:
    """Regression (batch_002, 2026-10-13): an automatic OPTIMIZE made version 4 with no rows."""
    assert not cdc_sql.range_has_changes({})
    assert not cdc_sql.range_has_changes({"insert": 0})
    assert cdc_sql.range_has_changes({"delete": 1})
    assert cdc_sql.range_has_changes({"update_preimage": 2, "update_postimage": 2})
    sql = cdc_sql.change_counts()
    assert f"FROM {cdc_sql.CHANGES_VIEW}" in sql and "GROUP BY _change_type" in sql


class _Source:
    """The CDF source as the fixed notebook drives it: a MERGE commits only real differences."""

    def __init__(self) -> None:
        self.rows: dict = {}
        self.version = 0  # CREATE TABLE
        self.events: list = []

    def merge(self, snapshot: dict) -> None:
        self.version += 1
        self.events.extend(
            snapshot_change_events(
                self.rows,
                snapshot,
                commit_version=self.version,
                commit_timestamp=f"t{self.version}",
            )
        )
        self.rows = {key: dict(row) for key, row in snapshot.items()}

    def optimize(self) -> None:
        """A commit without data changes (OPTIMIZE): a new version, no CDF rows."""
        self.version += 1


def _run(source, state, snapshot, *, reconcile_ok=True):
    """One notebook run: compare, maybe merge, process pending versions, reconcile, advance."""
    merged = snapshot != source.rows
    if merged:
        source.merge(snapshot)
    latest = source.version
    scanned = [e for e in source.events if state["checkpoint"] < e.commit_version <= latest]
    new_versions = bool(scanned)  # the notebook decides from CDF rows, not from version numbers
    if new_versions:
        result = apply_cdf(state["target"], source.events, checkpoint=state["checkpoint"])
        state["target"] = result.target
        state["history"] |= {e.change_id for e in scanned}
    if not reconcile_ok or not reconcile(_comparable(source.rows), state["target"]).matches:
        raise AssertionError("CDC reconciliation failed; checkpoint not advanced")
    if new_versions:
        state["checkpoint"] = max(state["checkpoint"], latest)  # forward only
    return {"merged": merged, "latest": latest, "new_versions": new_versions}


def _fresh():
    return {"target": {}, "history": set(), "checkpoint": -1}


def test_an_identical_replay_creates_no_version_and_leaves_the_checkpoint_alone() -> None:
    source, state = _Source(), _fresh()
    _run(source, state, _snapshot(1))
    version, checkpoint, history = source.version, state["checkpoint"], set(state["history"])
    replay = _run(source, state, _snapshot(1))
    assert replay == {"merged": False, "latest": version, "new_versions": False}
    assert (source.version, state["checkpoint"], state["history"]) == (version, checkpoint, history)
    assert len(history) == 5


def test_inserts_updates_and_deletes_still_merge_and_advance() -> None:
    source, state = _Source(), _fresh()
    _run(source, state, _snapshot(1))
    step = _run(source, state, _snapshot(2))  # 2 updates, 1 delete, 1 insert
    assert step["merged"] and step["new_versions"]
    assert state["checkpoint"] == source.version == 2
    assert "MT-1003" not in state["target"] and "MT-1006" in state["target"]


def test_pending_versions_are_processed_even_when_the_snapshot_already_matches() -> None:
    """Recovery: an earlier run merged batch 2 but failed before advancing its checkpoint."""
    source, state = _Source(), _fresh()
    _run(source, state, _snapshot(1))
    source.merge(_snapshot(2))  # committed, but the run died before processing it
    retry = _run(source, state, _snapshot(2))  # identical to the current source
    assert retry == {"merged": False, "latest": 2, "new_versions": True}
    assert state["checkpoint"] == 2
    assert reconcile(_comparable(_snapshot(2)), state["target"]).matches


def test_a_failed_reconciliation_never_moves_the_checkpoint() -> None:
    source, state = _Source(), _fresh()
    _run(source, state, _snapshot(1))
    with pytest.raises(AssertionError, match="checkpoint not advanced"):
        _run(source, state, _snapshot(2), reconcile_ok=False)
    assert state["checkpoint"] == 1  # unchanged; a retry processes version 2 again


def test_deleting_every_row_is_a_real_change() -> None:
    source, state = _Source(), _fresh()
    _run(source, state, _snapshot(1))
    step = _run(source, state, {})
    assert step["merged"] and state["target"] == {} and state["checkpoint"] == 2


def test_an_optimize_after_batch_002_does_not_move_the_checkpoint_on_batch_003() -> None:
    """Regression for the live batch_002 run: source v3 MERGE, then an automatic OPTIMIZE (v4)."""
    source, state = _Source(), _fresh()
    _run(source, state, _snapshot(1))
    _run(source, state, _snapshot(1))  # replay: no commit
    step = _run(source, state, _snapshot(2))
    assert step["new_versions"] and state["checkpoint"] == 2
    source.optimize()  # committed by Databricks after the run read the latest version
    history = set(state["history"])
    replay = _run(source, state, _snapshot(3))  # batch_003: same business state as batch_002
    assert replay == {"merged": False, "latest": 3, "new_versions": False}
    assert state["checkpoint"] == 2 and state["history"] == history


def test_data_after_an_optimize_is_still_processed_through_the_whole_range() -> None:
    source, state = _Source(), _fresh()
    _run(source, state, _snapshot(1))
    source.optimize()
    step = _run(source, state, _snapshot(2))  # scans the OPTIMIZE version and the new MERGE
    assert step["new_versions"] and state["checkpoint"] == source.version == 3
    assert reconcile(_comparable(_snapshot(2)), state["target"]).matches


def test_an_optimize_before_processing_does_not_hide_pending_changes() -> None:
    """The OPTIMIZE lands between the MERGE and the latest-version read: data is still applied."""
    source, state = _Source(), _fresh()
    _run(source, state, _snapshot(1))
    source.merge(_snapshot(2))
    source.optimize()
    retry = _run(source, state, _snapshot(2))
    assert retry == {"merged": False, "latest": 3, "new_versions": True}
    assert state["checkpoint"] == 3 and "MT-1003" not in state["target"]
