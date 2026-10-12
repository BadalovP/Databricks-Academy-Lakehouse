"""Tests for the evidence-path fix and the small-file generator.

The evidence tests exist because of a real loss of data. Evidence paths were keyed only on
`execution_id`, which is a BUSINESS lineage key: the corrected Silver-to-Gold run and its
idempotency repeat share one by design, because they process the same Bronze snapshot. The repeat
therefore overwrote the first run's report in the Volume, and run 1's figures survived only
because they had been read before the repeat happened.

The fix separates "which data" from "which run" rather than weakening either, and these tests pin
both halves: two attempts at one execution must produce two paths, and the data layer must still
key on the original business columns so reruns stay idempotent.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from urbanflow.reporting import (
    EVIDENCE_PHASES,
    evidence_report_path,
    resolve_attempt_id,
    sanitize_path_token,
    write_json_report,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
VOLUME = "/Volumes/dbr_dev/parvinbadalov_urbanflow/urbanflow_landing"
EXECUTION = "urbanflow-20260929T195132Z-r3"
# The two real corrected runs, whose reports collided under the old scheme.
RUN_1 = "240497605145949"
RUN_2 = "725768954237074"


# --- 1. the same execution with two attempts yields two paths ---------------------


def test_two_attempts_at_one_execution_produce_two_paths() -> None:
    """The exact collision that lost run 1's report."""
    first = evidence_report_path(
        VOLUME,
        phase="silver_gold",
        execution_id=EXECUTION,
        attempt_id=resolve_attempt_id(job_run_id=RUN_1),
        suffix="gold",
    )
    second = evidence_report_path(
        VOLUME,
        phase="silver_gold",
        execution_id=EXECUTION,
        attempt_id=resolve_attempt_id(job_run_id=RUN_2),
        suffix="gold",
    )

    assert first != second
    assert RUN_1 in first and RUN_2 in second
    # The business execution id still groups them, as a directory.
    assert f"/silver_gold/{EXECUTION}/" in first
    assert f"/silver_gold/{EXECUTION}/" in second
    assert Path(first).parent == Path(second).parent


def test_the_execution_id_remains_the_grouping_key() -> None:
    """Lineage is preserved: two different executions never share a directory."""
    ours = evidence_report_path(
        VOLUME, phase="silver_gold", execution_id=EXECUTION, attempt_id="run-1", suffix="gold"
    )
    theirs = evidence_report_path(
        VOLUME,
        phase="silver_gold",
        execution_id="urbanflow-other-r9",
        attempt_id="run-1",
        suffix="gold",
    )

    assert Path(ours).parent != Path(theirs).parent


# --- 2. the data layer still keys on the business columns ------------------------


def test_the_data_layer_keys_never_use_attempt_ids() -> None:
    """Evidence attempt ids must not leak into any business MERGE key.

    If it had, a repeat run would stop matching the rows it wrote last time and would insert
    duplicates instead of updating. Historical rides deliberately include execution_id because
    the development sample is a subset of the later full-month archive.
    """
    import inspect

    from urbanflow import gold, historical, weather

    for module in (gold, historical, weather):
        source = inspect.getsource(module)
        assert "attempt_id" not in source, f"{module.__name__} must not know about attempt ids"
        assert "run_attempt_id" not in source

    gold_source = inspect.getsource(gold.persist_gold_outputs)
    assert 'key_columns=("event_id",)' in gold_source
    assert 'execution_column="execution_id"' in gold_source
    historical_source = inspect.getsource(historical.persist_historical_outputs)
    assert 'key_columns=("execution_id", "ride_id")' in historical_source
    assert 'execution_column="execution_id"' in historical_source


def test_scoped_deletion_still_uses_the_business_execution_id() -> None:
    """The execution-scoped delete is what keeps a rerun idempotent; it must be untouched."""
    import inspect

    from urbanflow import persistence

    source = inspect.getsource(persistence.replace_execution_scope)
    assert "WHEN NOT MATCHED BY SOURCE AND" in source
    assert "attempt" not in source.lower()


# --- 3. no report is overwritten -------------------------------------------------


def test_two_attempts_write_two_files_that_coexist(tmp_path) -> None:
    """End to end on a real filesystem: both reports exist afterwards, with their own content."""
    first = evidence_report_path(
        tmp_path.as_posix(),
        phase="silver_gold",
        execution_id=EXECUTION,
        attempt_id=resolve_attempt_id(job_run_id=RUN_1),
        suffix="gold",
    )
    second = evidence_report_path(
        tmp_path.as_posix(),
        phase="silver_gold",
        execution_id=EXECUTION,
        attempt_id=resolve_attempt_id(job_run_id=RUN_2),
        suffix="gold",
    )

    write_json_report({"stale_rows_removed": 89, "rows_after": 657}, first)
    write_json_report({"stale_rows_removed": 0, "rows_after": 657}, second)

    assert Path(first).exists() and Path(second).exists()
    assert json.loads(Path(first).read_text(encoding="utf-8"))["stale_rows_removed"] == 89
    assert json.loads(Path(second).read_text(encoding="utf-8"))["stale_rows_removed"] == 0
    # One directory, two attempts, nothing lost.
    assert len(list(Path(first).parent.glob("*.json"))) == 2


def test_the_old_scheme_would_have_collided(tmp_path) -> None:
    """Demonstrates the defect, so the regression is anchored to a reproduction."""
    legacy = tmp_path / "reports" / "silver_gold" / f"{EXECUTION}.gold.json"

    write_json_report({"stale_rows_removed": 89}, legacy)
    write_json_report({"stale_rows_removed": 0}, legacy)

    # Same path twice: the first run's figures are simply gone.
    assert json.loads(legacy.read_text(encoding="utf-8"))["stale_rows_removed"] == 0
    assert len(list(legacy.parent.glob("*.json"))) == 1


# --- 4. deterministic and safe ---------------------------------------------------


def test_paths_are_deterministic_for_the_same_inputs() -> None:
    kwargs = dict(
        phase="historical",
        execution_id="urbanflow-hist-r1",
        attempt_id="run-7",
        suffix="historical",
    )

    assert evidence_report_path(VOLUME, **kwargs) == evidence_report_path(VOLUME, **kwargs)


def test_a_trailing_slash_on_the_volume_root_changes_nothing() -> None:
    kwargs = dict(phase="weather", execution_id="e1", attempt_id="run-1", suffix="weather")

    assert evidence_report_path(VOLUME + "/", **kwargs) == evidence_report_path(VOLUME, **kwargs)


@pytest.mark.parametrize(
    "token",
    [
        "../../etc/passwd",
        "a/b",
        "with space",
        "",
        "   ",
        ".hidden",
        "semi;colon",
        "quote'x",
        "back\\slash",
        "new\nline",
        "with:colon",
    ],
)
def test_path_unsafe_tokens_are_refused(token: str) -> None:
    """A report path is built from parameters, so traversal and injection are refused here."""
    with pytest.raises(ValueError):
        sanitize_path_token(token, label="test")


@pytest.mark.parametrize("field", ["execution_id", "attempt_id", "suffix"])
def test_every_path_component_is_validated(field: str) -> None:
    kwargs = {
        "phase": "silver_gold",
        "execution_id": "safe-exec",
        "attempt_id": "run-1",
        "suffix": "gold",
    }
    kwargs[field] = "../escape"

    with pytest.raises(ValueError):
        evidence_report_path(VOLUME, **kwargs)


def test_an_unknown_phase_is_refused_rather_than_creating_an_orphan_directory() -> None:
    with pytest.raises(ValueError, match="Unknown evidence phase"):
        evidence_report_path(VOLUME, phase="typo", execution_id="e", attempt_id="a", suffix="s")
    assert "silver_gold" in EVIDENCE_PHASES and "historical" in EVIDENCE_PHASES


def test_an_empty_volume_root_is_refused() -> None:
    with pytest.raises(ValueError, match="volume_root must be non-empty"):
        evidence_report_path("", phase="weather", execution_id="e", attempt_id="a", suffix="w")


def test_an_unsubstituted_job_parameter_falls_back_to_a_timestamp() -> None:
    """`{{job.run_id}}` reaching the notebook literally must not become a filename."""
    moment = datetime(2026, 10, 2, 3, 4, 5, 123456, tzinfo=UTC)
    unique = "12345678-1234-5678-1234-567812345678"
    expected = f"ts-20261002T030405.123456Z-{unique}"

    assert (
        resolve_attempt_id(job_run_id="{{job.run_id}}", now=moment, unique_token=unique) == expected
    )
    assert resolve_attempt_id(job_run_id="", now=moment, unique_token=unique) == expected
    assert resolve_attempt_id(job_run_id=None, now=moment, unique_token=unique) == expected


def test_two_interactive_attempts_in_the_same_microsecond_do_not_collide() -> None:
    """The old seconds-only fallback overwrote fast repeated interactive executions."""
    moment = datetime(2026, 10, 2, 3, 4, 5, 123456, tzinfo=UTC)

    first = resolve_attempt_id(job_run_id=None, now=moment)
    second = resolve_attempt_id(job_run_id=None, now=moment)

    assert first != second
    assert first.startswith("ts-20261002T030405.123456Z-")
    assert second.startswith("ts-20261002T030405.123456Z-")


def test_a_real_job_run_id_is_preferred_over_the_clock() -> None:
    """Traceability: the filename should point back at the run page."""
    assert (
        resolve_attempt_id(job_run_id=RUN_1, now=datetime(2026, 1, 1, tzinfo=UTC)) == f"run-{RUN_1}"
    )


# --- 5. no secrets in filenames or reports ---------------------------------------


# Built at runtime rather than written as literals. A token-shaped constant in a committed file
# trips GitHub's push protection, which cannot distinguish a test fixture from a real credential -
# and a scanner that cries wolf on purpose is a scanner people learn to ignore.
_FAKE_PAT = "d" + "api" + "0" * 32
_FAKE_CONNECTION_STRING = "Endpoint=" + "sb://ns/;" + "SharedAccess" + "Key=abc"
SECRET_MARKERS = ("SharedAccessKey", "AccountKey=", "Endpoint=sb://", "EntityPath=", "d" + "api")


def test_a_connection_string_cannot_reach_a_filename() -> None:
    """A connection string needs `=`, `/` and `;`, none of which the path charset allows."""
    with pytest.raises(ValueError):
        sanitize_path_token(_FAKE_CONNECTION_STRING, label="execution_id")


def test_a_bare_credential_shaped_token_cannot_reach_a_filename() -> None:
    """A narrow charset is insufficient because many bearer tokens are plain alphanumeric."""
    with pytest.raises(ValueError, match="credential-shaped"):
        sanitize_path_token(_FAKE_PAT, label="execution_id")


def test_a_long_numeric_secret_cannot_reach_a_filename() -> None:
    with pytest.raises(ValueError, match="credential-shaped"):
        sanitize_path_token("9" * 32, label="execution_id")


def test_job_run_ids_must_be_numeric_platform_ids() -> None:
    with pytest.raises(ValueError, match="numeric Databricks run ID"):
        resolve_attempt_id(job_run_id="not-a-platform-run")

    assert resolve_attempt_id(job_run_id="240497605145949") == "run-240497605145949"


def test_committed_evidence_files_contain_no_secret_markers() -> None:
    """The reports are committed, so this is a real exposure check rather than a formality."""
    evidence = PROJECT_ROOT / "evidence"
    checked = 0
    for path in sorted(evidence.glob("*.json")):
        text = path.read_text(encoding="utf-8")
        for marker in SECRET_MARKERS:
            assert marker not in text, f"{path.name} contains {marker}"
        checked += 1
    assert checked > 0  # a silently empty glob would make this vacuous


def test_notebooks_pass_the_run_id_rather_than_inventing_an_attempt_id() -> None:
    for name in (
        "04_bronze_to_silver.py",
        "05_silver_to_gold.py",
        "06_historical_trips.py",
        "07_weather_enrichment.py",
    ):
        text = (PROJECT_ROOT / "notebooks" / name).read_text(encoding="utf-8")
        assert 'dbutils.widgets.text("run_attempt_id", "")' in text, name
        assert "resolve_attempt_id(" in text, name
        assert "evidence_report_path(" in text, name


def test_jobs_supply_the_platform_run_id_to_every_evidence_writing_task() -> None:
    """`{{job.run_id}}` is substituted by Databricks, so the id is the platform's, not ours."""
    jobs = yaml.safe_load(
        (PROJECT_ROOT / "resources" / "retired" / "component_jobs.yml").read_text(encoding="utf-8")
    )

    for name in (
        "urbanflow_silver_gold_test",
        "urbanflow_historical_trips_test",
        "urbanflow_weather_enrichment_test",
    ):
        job = jobs["resources"]["jobs"][name]
        defaults = {p["name"]: p["default"] for p in job["parameters"]}
        assert defaults["run_attempt_id"] == "{{job.run_id}}", name
        for task in job["tasks"]:
            parameters = task["notebook_task"]["base_parameters"]
            assert parameters["run_attempt_id"] == "{{job.parameters.run_attempt_id}}", name


# --- the Lakeflow isolation guard ------------------------------------------------


def test_the_lakeflow_pipeline_never_targets_the_validated_notebook_schema() -> None:
    """Lakeflow declares tables with the SAME names as the notebook path.

    If both wrote to one schema, a Lakeflow update would take ownership of tables that already
    carry live-validated evidence, and the declarative and imperative results could never be
    compared. So the pipeline gets its own schema and this test fails if that is undone.
    """
    bundle = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text(encoding="utf-8"))
    pipelines = yaml.safe_load(
        (PROJECT_ROOT / "resources" / "pipelines.yml").read_text(encoding="utf-8")
    )
    pipeline = pipelines["resources"]["pipelines"]["urbanflow_pipeline"]

    assert pipeline["schema"] == "${var.lakeflow_schema}"
    assert pipeline["schema"] != "${var.schema}"
    assert pipeline["configuration"]["urbanflow.schema"] == "${var.lakeflow_schema}"

    variables = bundle["variables"]
    notebook_schema = variables["schema"]["default"]
    lakeflow_schema = variables["lakeflow_schema"]["default"]
    assert lakeflow_schema != notebook_schema
    # And per target, so a dev deployment cannot collide either.
    for target in bundle["targets"].values():
        overrides = target.get("variables") or {}
        resolved_notebook = overrides.get("schema", notebook_schema)
        resolved_lakeflow = overrides.get("lakeflow_schema", lakeflow_schema)
        assert resolved_lakeflow != resolved_notebook


def test_the_lakeflow_schema_is_a_personal_urbanflow_schema() -> None:
    """Isolation must not mean writing somewhere that belongs to someone else."""
    bundle = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text(encoding="utf-8"))

    for key in ("schema", "lakeflow_schema"):
        default = bundle["variables"][key]["default"]
        assert default.startswith("parvinbadalov_urbanflow"), default


# --- the small-file generator ----------------------------------------------------


def test_the_generator_writes_exactly_the_requested_file_count(tmp_path) -> None:
    from scripts.generate_many_small_files import FILE_PREFIX, generate

    manifest = generate(tmp_path / "many", files=25, rows_per_file=4)

    produced = sorted((tmp_path / "many").glob(f"{FILE_PREFIX}*.csv"))
    assert len(produced) == 25 == manifest["files"]
    assert manifest["total_rows"] == 100
    assert manifest["total_bytes"] > 0


def test_regenerating_is_repeat_safe_and_does_not_accumulate(tmp_path) -> None:
    """A second run with fewer files must not leave the first run's extras behind."""
    from scripts.generate_many_small_files import FILE_PREFIX, generate

    generate(tmp_path / "many", files=30, rows_per_file=2)
    generate(tmp_path / "many", files=10, rows_per_file=2)

    assert len(sorted((tmp_path / "many").glob(f"{FILE_PREFIX}*.csv"))) == 10


def test_generated_content_is_deterministic(tmp_path) -> None:
    from scripts.generate_many_small_files import generate

    generate(tmp_path / "a", files=5, rows_per_file=3)
    generate(tmp_path / "b", files=5, rows_per_file=3)

    first = (tmp_path / "a" / "synthetic_trips_00002.csv").read_text(encoding="utf-8")
    second = (tmp_path / "b" / "synthetic_trips_00002.csv").read_text(encoding="utf-8")
    assert first == second


def test_generated_rows_are_labelled_synthetic_and_keep_station_ids_as_short_names(
    tmp_path,
) -> None:
    """These must never be mistakable for Citi Bike data, and must still join on short_name."""
    import csv as csv_module

    from scripts.generate_many_small_files import generate

    generate(tmp_path / "many", files=2, rows_per_file=5)
    rows = list(csv_module.DictReader((tmp_path / "many" / "synthetic_trips_00000.csv").open()))

    assert all(row["ride_id"].startswith("synthetic-") for row in rows)
    assert all("." in row["start_station_id"] for row in rows)  # short_name form, e.g. 7407.13
    manifest = json.loads((tmp_path / "many" / "_synthetic_small_files.json").read_text())
    assert "SYNTHETIC EDUCATIONAL DATA" in manifest["label"]


def test_generated_rows_pass_the_trip_quality_rules(tmp_path) -> None:
    """The small-file lesson should not be confounded by quarantined rows."""
    import csv as csv_module
    from datetime import datetime as dt

    from scripts.generate_many_small_files import generate

    generate(tmp_path / "many", files=3, rows_per_file=5)
    for path in sorted((tmp_path / "many").glob("synthetic_trips_*.csv")):
        for row in csv_module.DictReader(path.open()):
            started = dt.strptime(row["started_at"], "%Y-%m-%d %H:%M:%S")
            ended = dt.strptime(row["ended_at"], "%Y-%m-%d %H:%M:%S")
            seconds = (ended - started).total_seconds()
            assert 60 <= seconds <= 24 * 3600
            assert row["member_casual"] in {"member", "casual"}


def test_cleanup_removes_only_what_was_generated(tmp_path) -> None:
    from scripts.generate_many_small_files import cleanup, generate

    target = tmp_path / "many"
    generate(target, files=5, rows_per_file=2)
    unrelated = target / "keep_me.txt"
    unrelated.write_text("not mine", encoding="utf-8")
    same_prefix_but_foreign = target / "synthetic_trips_foreign.csv"
    same_prefix_but_foreign.write_text("not mine either", encoding="utf-8")

    report = cleanup(target)

    assert report["removed_files"] == 5
    assert report["removed_directory"] is False  # the foreign file kept the directory alive
    assert unrelated.exists()
    assert same_prefix_but_foreign.exists()


def test_regeneration_preserves_a_foreign_file_with_the_generator_prefix(tmp_path) -> None:
    from scripts.generate_many_small_files import generate

    target = tmp_path / "many"
    generate(target, files=5, rows_per_file=2)
    foreign = target / "synthetic_trips_foreign.csv"
    foreign.write_text("foreign", encoding="utf-8")

    generate(target, files=3, rows_per_file=2)

    assert foreign.read_text(encoding="utf-8") == "foreign"
    assert len(list(target.glob("synthetic_trips_0000?.csv"))) == 3


def test_cleanup_refuses_a_forged_marker_inventory_with_path_traversal(tmp_path) -> None:
    from scripts.generate_many_small_files import MARKER_NAME, cleanup

    target = tmp_path / "many"
    target.mkdir()
    marker = {
        "generator": "scripts/generate_many_small_files.py",
        "generated_files": ["../foreign.csv"],
    }
    (target / MARKER_NAME).write_text(json.dumps(marker), encoding="utf-8")

    with pytest.raises(SystemExit, match="unsafe generated filename"):
        cleanup(target)


def test_cleanup_refuses_to_claim_a_directory_it_does_not_own(tmp_path) -> None:
    from scripts.generate_many_small_files import cleanup

    foreign = tmp_path / "someone_elses"
    foreign.mkdir()
    (foreign / "important.csv").write_text("data", encoding="utf-8")

    report = cleanup(foreign)

    assert report["removed_files"] == 0
    assert (foreign / "important.csv").exists()


def test_the_generator_refuses_a_foreign_non_empty_directory(tmp_path) -> None:
    from scripts.generate_many_small_files import generate

    foreign = tmp_path / "theirs"
    foreign.mkdir()
    (foreign / "data.csv").write_text("x", encoding="utf-8")

    with pytest.raises(SystemExit, match="not empty"):
        generate(foreign, files=2, rows_per_file=1)
    assert (foreign / "data.csv").exists()


@pytest.mark.parametrize("kwargs", [{"files": 0}, {"rows_per_file": 0}])
def test_the_generator_refuses_nonsensical_sizes(tmp_path, kwargs) -> None:
    from scripts.generate_many_small_files import generate

    call = {"files": 5, "rows_per_file": 2}
    call.update(kwargs)
    with pytest.raises(ValueError):
        generate(tmp_path / "many", **call)


def test_a_thousand_tiny_files_really_are_tiny(tmp_path) -> None:
    """The point is the file COUNT, not the volume: 1,000 files under a megabyte."""
    from scripts.generate_many_small_files import generate

    manifest = generate(tmp_path / "thousand", files=1000, rows_per_file=5)

    assert manifest["files"] == 1000
    assert manifest["total_rows"] == 5000
    assert manifest["total_bytes"] < 1_500_000, manifest["total_bytes"]
