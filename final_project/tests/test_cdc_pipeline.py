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
