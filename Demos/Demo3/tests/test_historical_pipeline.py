"""Tests for the production historical trip path: quarantine, dedup, lineage, persistence.

The existing `test_historical.py` covers the Auto Loader contract and the short_name join.
This file covers what turns that read into a reconciled set of tables: every landed trip
getting exactly one outcome, duplicate rides being recorded rather than silently collapsed,
and the daily demand aggregate being correctable when a rerun produces fewer rows.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
import yaml

from urbanflow.historical import (
    daily_trip_demand,
    foreign_checkpoint_owners,
    historical_bronze_table,
    historical_landing_paths,
    persist_historical_outputs,
    reconcile_historical,
    rider_mix_summary,
    split_trips_and_quarantine,
    trip_duration_profile,
    validate_historical_trips,
    with_trip_lineage,
)

RUN = "urbanflow-hist-20240124T000000Z"


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-historical-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _trips(spark: Any) -> Any:
    """Six rows covering every outcome the split must distinguish.

    r1  valid, 10 minutes
    r1  duplicate of r1 (same ride redelivered), identical interval
    r2  valid, 20 minutes, casual
    r3  40 seconds - a false start, below the one-minute floor
    r4  30 hours - an unreturned bike, above the one-day ceiling
    ''  blank ride id, which must quarantine as MISSING_RIDE_ID
    """
    return spark.sql(
        "SELECT * FROM VALUES "
        "('r1','classic_bike',TIMESTAMP'2024-01-24 09:00:00',TIMESTAMP'2024-01-24 09:10:00',"
        "'A','7407.13','B','7463.09','member'), "
        "('r1','classic_bike',TIMESTAMP'2024-01-24 09:00:00',TIMESTAMP'2024-01-24 09:10:00',"
        "'A','7407.13','B','7463.09','member'), "
        "('r2','electric_bike',TIMESTAMP'2024-01-24 10:00:00',TIMESTAMP'2024-01-24 10:20:00',"
        "'A','7407.13','B','7463.09','casual'), "
        "('r3','classic_bike',TIMESTAMP'2024-01-24 11:00:00',TIMESTAMP'2024-01-24 11:00:40',"
        "'A','7407.13','B','7463.09','member'), "
        "('r4','classic_bike',TIMESTAMP'2024-01-24 12:00:00',TIMESTAMP'2024-01-25 18:00:00',"
        "'A','7407.13','B','7463.09','member'), "
        "('','classic_bike',TIMESTAMP'2024-01-24 13:00:00',TIMESTAMP'2024-01-24 13:15:00',"
        "'A','7407.13','B','7463.09','member') "
        "AS t(ride_id, rideable_type, started_at, ended_at, start_station_name, "
        "start_station_id, end_station_name, end_station_id, member_casual)"
    )


# --- landing layout -------------------------------------------------------------


def test_landing_paths_live_inside_the_existing_volume() -> None:
    """No new storage: every path is a subdirectory of the Volume already created."""
    paths = historical_landing_paths("/Volumes/cat/sch/urbanflow_landing", execution_id=RUN)

    for value in paths.values():
        assert value.startswith("/Volumes/cat/sch/urbanflow_landing/")
    # Schema location and streaming checkpoint must be different directories.
    assert paths["schema"] != paths["checkpoint"]
    assert RUN in paths["checkpoint"]
    assert RUN not in paths["schema"]


def test_landing_paths_tolerate_a_trailing_slash() -> None:
    paths = historical_landing_paths("/Volumes/cat/sch/vol/", execution_id=RUN)

    assert paths["landing"] == "/Volumes/cat/sch/vol/landing/historical_trips"


def test_monthly_archive_paths_are_isolated_from_the_validated_sample() -> None:
    root = "/Volumes/cat/sch/vol"
    sample = historical_landing_paths(root, execution_id="urbanflow-hist-devsample40-r1")
    monthly = historical_landing_paths(
        root,
        execution_id="urbanflow-hist-month202401-r1",
        landing_subdir="202401-full",
    )

    assert sample == {
        "landing": f"{root}/landing/historical_trips",
        "schema": f"{root}/schemas/historical_trips",
        "checkpoint_root": f"{root}/checkpoints/historical_trips",
        "checkpoint": f"{root}/checkpoints/historical_trips/urbanflow-hist-devsample40-r1",
        "archive": f"{root}/landing/historical_trips/_archive",
    }
    assert monthly == {
        # Sibling namespaces, not nested: a monthly directory inside the sample's landing root
        # would be discovered recursively by a sample-mode Auto Loader run.
        "landing": f"{root}/landing/historical_trips_202401-full",
        "schema": f"{root}/schemas/historical_trips_202401-full",
        "checkpoint_root": f"{root}/checkpoints/historical_trips_202401-full",
        "checkpoint": (
            f"{root}/checkpoints/historical_trips_202401-full/urbanflow-hist-month202401-r1"
        ),
        "archive": f"{root}/landing/historical_trips_202401-full/_archive",
    }
    assert set(sample.values()).isdisjoint(monthly.values())


@pytest.mark.parametrize("landing_subdir", ["../escape", "nested/path", "with space", ".hidden"])
def test_landing_subdir_refuses_path_traversal_and_nested_paths(landing_subdir: str) -> None:
    with pytest.raises(ValueError):
        historical_landing_paths(
            "/Volumes/cat/sch/vol", execution_id=RUN, landing_subdir=landing_subdir
        )


def test_historical_job_wires_the_landing_subdir_to_notebook_06() -> None:
    root = Path(__file__).resolve().parents[1]
    jobs = yaml.safe_load((root / "resources" / "jobs.yml").read_text(encoding="utf-8"))
    job = jobs["resources"]["jobs"]["urbanflow_historical_trips_test"]
    defaults = {parameter["name"]: parameter["default"] for parameter in job["parameters"]}

    assert defaults["landing_subdir"] == ""
    assert (
        job["tasks"][0]["notebook_task"]["base_parameters"]["landing_subdir"]
        == "{{job.parameters.landing_subdir}}"
    )
    notebook = (root / "notebooks" / "06_historical_trips.py").read_text(encoding="utf-8")
    assert 'dbutils.widgets.text("landing_subdir", "")' in notebook


@pytest.mark.parametrize(
    "volume_root, execution_id", [("", RUN), ("   ", RUN), ("/Volumes/a", " ")]
)
def test_landing_paths_refuse_blank_inputs(volume_root: str, execution_id: str) -> None:
    with pytest.raises(ValueError):
        historical_landing_paths(volume_root, execution_id=execution_id)


# --- duration rules -------------------------------------------------------------


@pytest.mark.spark
def test_implausible_durations_are_flagged_without_being_deleted(spark_session) -> None:
    """Citi Bike's own guidance: sub-minute trips are false starts, day-long ones are losses."""
    validated = validate_historical_trips(_trips(spark_session))

    rules = {row["ride_id"]: set(row["failed_rules"]) for row in validated.collect()}
    assert "TRIP_TOO_SHORT" in rules["r3"]
    assert "TRIP_TOO_LONG" in rules["r4"]
    assert rules["r2"] == set()
    # Flagged, not dropped: every input row survives.
    assert validated.count() == 6


@pytest.mark.spark
def test_a_reversed_interval_raises_one_finding_not_three(spark_session) -> None:
    """Duration rules fire only on a sound interval, so one bad row is one finding."""
    reversed_trip = spark_session.sql(
        "SELECT * FROM VALUES "
        "('rx',TIMESTAMP'2024-01-24 10:00:00',TIMESTAMP'2024-01-24 09:00:00','7407.13','member') "
        "AS t(ride_id, started_at, ended_at, start_station_id, member_casual)"
    )

    rules = set(validate_historical_trips(reversed_trip).collect()[0]["failed_rules"])

    assert rules == {"INVALID_TRIP_INTERVAL"}
    assert "TRIP_TOO_SHORT" not in rules
    assert "TRIP_TOO_LONG" not in rules


@pytest.mark.spark
def test_duration_bounds_are_configurable(spark_session) -> None:
    """A 40-second trip is valid if the archive being studied says it is."""
    validated = validate_historical_trips(_trips(spark_session), min_trip_seconds=10)

    rules = {row["ride_id"]: set(row["failed_rules"]) for row in validated.collect()}
    assert "TRIP_TOO_SHORT" not in rules["r3"]


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"min_trip_seconds": -1}, "must not be negative"),
        ({"max_trip_hours": 0}, "must be positive"),
    ],
)
def test_nonsensical_duration_bounds_are_refused(kwargs: dict[str, int], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_historical_trips(Mock(columns=[]), **kwargs)


# --- the three-way split --------------------------------------------------------


@pytest.mark.spark
def test_every_landed_trip_gets_exactly_one_outcome(spark_session) -> None:
    """The reconciliation identity: landed == valid + quarantine + duplicates."""
    trips = _trips(spark_session)
    split = split_trips_and_quarantine(trips)

    valid = split["valid"].count()
    quarantine = split["quarantine"].count()
    duplicates = split["duplicates"].count()

    assert valid == 2  # r1 (one copy) and r2
    assert quarantine == 3  # r3 too short, r4 too long, blank ride id
    assert duplicates == 1  # the second copy of r1
    assert valid + quarantine + duplicates == trips.count()


@pytest.mark.spark
def test_a_redelivered_ride_is_recorded_not_silently_collapsed(spark_session) -> None:
    split = split_trips_and_quarantine(_trips(spark_session))

    duplicate = split["duplicates"].collect()[0]
    assert duplicate["ride_id"] == "r1"
    assert list(duplicate["failed_rules"]) == ["DUPLICATE_RIDE_ID"]
    assert duplicate["passed_contract"] is False
    # Exactly one copy survives into the valid set.
    assert [row["ride_id"] for row in split["valid"].collect()].count("r1") == 1


@pytest.mark.spark
def test_blank_ride_ids_are_quarantined_not_deduplicated_against_each_other(
    spark_session,
) -> None:
    """Treating every blank as 'the same ride' would collapse unrelated trips into one."""
    blanks = spark_session.sql(
        "SELECT * FROM VALUES "
        "('',TIMESTAMP'2024-01-24 09:00:00',TIMESTAMP'2024-01-24 09:30:00','7407.13','member'), "
        "('',TIMESTAMP'2024-01-24 14:00:00',TIMESTAMP'2024-01-24 14:30:00','7407.13','casual') "
        "AS t(ride_id, started_at, ended_at, start_station_id, member_casual)"
    )

    split = split_trips_and_quarantine(blanks)

    assert split["valid"].count() == 0
    assert split["duplicates"].count() == 0
    # Both rows are preserved as distinct quarantine records.
    assert split["quarantine"].count() == 2


@pytest.mark.spark
def test_the_surviving_copy_of_a_duplicate_is_deterministic(spark_session) -> None:
    """Whichever partition arrives first must not decide which copy is kept."""
    varied = spark_session.sql(
        "SELECT * FROM VALUES "
        "('r9',TIMESTAMP'2024-01-24 12:00:00',TIMESTAMP'2024-01-24 12:30:00','7407.13','casual'), "
        "('r9',TIMESTAMP'2024-01-24 09:00:00',TIMESTAMP'2024-01-24 09:30:00','7407.13','member') "
        "AS t(ride_id, started_at, ended_at, start_station_id, member_casual)"
    )

    kept = split_trips_and_quarantine(varied)["valid"].collect()
    repeated = split_trips_and_quarantine(varied)["valid"].collect()

    assert len(kept) == 1
    # Earliest start wins, and the same copy wins every time.
    assert kept[0]["started_at"].hour == 9
    assert kept[0]["member_casual"] == "member"
    assert kept[0]["started_at"] == repeated[0]["started_at"]


# --- lineage --------------------------------------------------------------------


@pytest.mark.spark
def test_lineage_stamps_the_execution_without_requiring_file_metadata(spark_session) -> None:
    stamped = with_trip_lineage(_trips(spark_session), execution_id=RUN, include_source_file=False)

    assert "execution_id" in stamped.columns
    assert "ingested_at" in stamped.columns
    assert "source_file" not in stamped.columns
    assert {row["execution_id"] for row in stamped.collect()} == {RUN}


def test_lineage_refuses_a_blank_execution_id() -> None:
    with pytest.raises(ValueError, match="execution_id must be non-empty"):
        with_trip_lineage(Mock(), execution_id="  ")


# --- demand, rider mix and duration profile -------------------------------------


@pytest.mark.spark
def test_demand_carries_a_stable_schema_with_or_without_an_execution_id(
    spark_session,
) -> None:
    """An appearing-and-disappearing column broke the Gold fact once already."""
    from tests.conftest_spark import station_information_frame
    from urbanflow.gold import station_dimension
    from urbanflow.historical import join_trips_to_stations

    dimension = station_dimension(station_information_frame(spark_session))
    joined = join_trips_to_stations(_trips(spark_session), dimension)

    without = daily_trip_demand(joined)
    with_id = daily_trip_demand(joined, execution_id=RUN)

    assert without.columns == with_id.columns
    assert "execution_id" in without.columns
    assert {row["execution_id"] for row in without.collect()} == {None}
    assert {row["execution_id"] for row in with_id.collect()} == {RUN}


@pytest.mark.spark
def test_rider_mix_reports_member_share_per_station(spark_session) -> None:
    mix = rider_mix_summary(_trips(spark_session)).collect()

    row = {entry["station_short_name"]: entry for entry in mix}["7407.13"]
    assert row["trips"] == 6
    assert row["member_trips"] == 5
    assert row["casual_trips"] == 1
    assert row["member_share"] == pytest.approx(5 / 6, abs=1e-4)


@pytest.mark.spark
def test_duration_profile_makes_an_implausible_archive_visible(spark_session) -> None:
    profile = trip_duration_profile(_trips(spark_session))

    assert profile["trips"] == 6
    assert profile["min_minutes"] == pytest.approx(40 / 60, abs=0.01)  # the 40-second trip
    assert profile["max_minutes"] == pytest.approx(30 * 60)  # the 30-hour trip


# --- reconciliation -------------------------------------------------------------


def test_reconciliation_passes_when_every_row_is_accounted_for() -> None:
    report = reconcile_historical(
        landed_rows=6,
        valid_rows=2,
        quarantine_rows=3,
        duplicate_rows=1,
        match_rate={"trips": 2, "match_rate": 1.0},
        demand_rows=1,
    )

    assert report["accounted_rows"] == 6
    assert report["counts_reconcile"] is True
    assert report["status"] == "PASS"


def test_reconciliation_fails_when_a_row_went_missing() -> None:
    report = reconcile_historical(
        landed_rows=6,
        valid_rows=2,
        quarantine_rows=2,
        duplicate_rows=1,
        match_rate={"trips": 2, "match_rate": 1.0},
        demand_rows=1,
    )

    assert report["accounted_rows"] == 5
    assert report["status"] == "FAIL"


def test_a_partial_match_rate_is_not_treated_as_a_failure() -> None:
    """Stations are renamed and retired, so a real archive legitimately has unmatched trips."""
    report = reconcile_historical(
        landed_rows=3,
        valid_rows=3,
        quarantine_rows=0,
        duplicate_rows=0,
        match_rate={"trips": 3, "match_rate": 0.6667},
        demand_rows=2,
    )

    assert report["status"] == "PASS"
    assert report["join_produced_no_matches"] is False


def test_a_zero_match_rate_is_treated_as_a_failure() -> None:
    """Zero matches is the signature of joining on the UUID instead of short_name."""
    report = reconcile_historical(
        landed_rows=3,
        valid_rows=3,
        quarantine_rows=0,
        duplicate_rows=0,
        match_rate={"trips": 3, "match_rate": 0.0},
        demand_rows=0,
    )

    assert report["join_produced_no_matches"] is True
    assert report["status"] == "FAIL"


def test_no_trips_at_all_is_not_a_join_failure() -> None:
    """An empty archive has nothing to match, which is different from matching nothing."""
    report = reconcile_historical(
        landed_rows=0,
        valid_rows=0,
        quarantine_rows=0,
        duplicate_rows=0,
        match_rate={"trips": 0, "match_rate": 0.0},
        demand_rows=0,
    )

    assert report["join_produced_no_matches"] is False
    assert report["status"] == "PASS"


# --- persistence ----------------------------------------------------------------

HISTORICAL_TABLES = {
    "trips": "cat.sch.silver_historical_trips",
    "quarantine": "cat.sch.quarantine_historical_trips",
    "duplicates": "cat.sch.duplicate_historical_trips",
    "daily_demand": "cat.sch.gold_daily_trip_demand",
}


def test_persistence_requires_every_table_name() -> None:
    with pytest.raises(ValueError, match="Missing historical table names"):
        persist_historical_outputs(
            Mock(),
            valid=Mock(),
            quarantine=Mock(),
            duplicates=Mock(),
            daily_demand=Mock(),
            table_names={"trips": "cat.sch.t"},
            execution_id=RUN,
        )


def test_demand_is_execution_scoped_while_trips_are_merged(monkeypatch) -> None:
    """A rerun producing fewer demand rows must shrink the aggregate, as Gold does."""
    from urbanflow import persistence

    merged: list[tuple[str, tuple]] = []
    scoped: list[str] = []
    monkeypatch.setattr(persistence, "evolve_delta_schema", lambda *a, **k: {})
    monkeypatch.setattr(
        persistence,
        "merge_delta_table",
        lambda spark, frame, table, *, key_columns: merged.append((table, key_columns)) or {},
    )
    monkeypatch.setattr(
        persistence,
        "replace_execution_scope",
        lambda spark, frame, table, **k: scoped.append(table) or {},
    )

    persist_historical_outputs(
        Mock(),
        valid=Mock(),
        quarantine=Mock(),
        duplicates=Mock(),
        daily_demand=Mock(),
        table_names=HISTORICAL_TABLES,
        execution_id=RUN,
    )

    assert scoped == [HISTORICAL_TABLES["daily_demand"]]
    keys = dict(merged)
    # The 40-row sample is drawn from the full January archive. Including execution_id keeps
    # both lineages instead of letting the monthly MERGE overwrite the sample's 40 rides.
    assert keys[HISTORICAL_TABLES["trips"]] == ("execution_id", "ride_id")
    # Quarantine and duplicate rows are not unique by ride_id alone.
    assert "ride_id" in keys[HISTORICAL_TABLES["quarantine"]]
    assert len(keys[HISTORICAL_TABLES["duplicates"]]) > 1


def test_monthly_namespace_is_a_sibling_of_the_sample_never_a_child() -> None:
    """Auto Loader discovers recursively, so a nested monthly directory is a real hazard.

    An earlier version joined `landing_subdir` with `/`, putting the January archive at
    `landing/historical_trips/202401-full` - inside the sample's own landing root. The sample's
    checkpoint has only ever seen its 40-row CSV, so a later sample-mode run would have discovered
    1.9 million monthly rows as new files and failed its 40-row expectations. This was caught after
    the archive had already been staged, and the files were moved to the sibling path.
    """
    sample = historical_landing_paths("/V", execution_id="sample-x")
    monthly = historical_landing_paths("/V", execution_id="month-x", landing_subdir="202401-full")

    for key in ("landing", "schema", "checkpoint"):
        assert not monthly[key].startswith(sample[key].rstrip("/") + "/"), key
        assert monthly[key] != sample[key], key

    # The already-validated sample paths must stay byte-identical.
    assert sample["landing"] == "/V/landing/historical_trips"
    assert sample["schema"] == "/V/schemas/historical_trips"
    assert monthly["landing"] == "/V/landing/historical_trips_202401-full"


def test_an_empty_subdir_leaves_the_sample_paths_untouched() -> None:
    """The default must never relocate the validated sample."""
    assert historical_landing_paths("/V", execution_id="e") == historical_landing_paths(
        "/V", execution_id="e", landing_subdir=""
    )


def test_each_source_namespace_lands_in_its_own_bronze_table() -> None:
    """Bronze carries no execution column, so namespaces are isolated physically.

    Unified run 295677984549301 exposed the shared table: notebook 06 read ALL of Bronze and
    stamped every row with the current execution, so a monthly run would have re-stamped the
    sample's 40 rides as its own and counted them as duplicates of the same archive.
    """
    assert historical_bronze_table("c.s") == "c.s.bronze_historical_trips"
    assert historical_bronze_table("c.s", landing_subdir="") == "c.s.bronze_historical_trips"
    assert (
        historical_bronze_table("c.s", landing_subdir="202401-full")
        == "c.s.bronze_historical_trips_202401_full"
    )
    for unsafe in ("../x", "a b", "20.24"):
        with pytest.raises(ValueError):
            historical_bronze_table("c.s", landing_subdir=unsafe)


def test_a_second_execution_cannot_claim_an_owned_namespace() -> None:
    sample = "urbanflow-hist-devsample40-20261002T0010Z"
    assert foreign_checkpoint_owners([], execution_id=sample) == []
    assert foreign_checkpoint_owners([f"{sample}/"], execution_id=sample) == []
    assert foreign_checkpoint_owners([f"{sample}/"], execution_id="urbanflow-hist-new") == [sample]
    assert foreign_checkpoint_owners(["b/", "a/", "me/"], execution_id="me") == ["a", "b"]
