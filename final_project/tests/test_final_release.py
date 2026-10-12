"""Release safety for UrbanFlow 2.0: the platform Job DAG, its parameter wiring, the release
validators, and the Final Project CI/CD workflows."""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
from pathlib import Path

import pytest
import yaml

from scripts.validate_bundle import load_bundle
from scripts.validate_job_run import EXPECTED_TASKS, validate_job_run
from scripts.validate_release_plan import validate_release_plan

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parent  # final_project/ sits directly under the repository root
WORKFLOWS = REPOSITORY_ROOT / ".github" / "workflows"
CI = WORKFLOWS / "final_project_ci.yml"
CD = WORKFLOWS / "final_project_cd.yml"
JOBS_FILE = PROJECT_ROOT / "resources" / "urbanflow_final.job.yml"


def _jobs() -> dict:
    return yaml.safe_load(JOBS_FILE.read_text(encoding="utf-8"))["resources"]["jobs"]


def _platform() -> dict:
    return _jobs()["urbanflow_final_platform"]


def _tasks() -> dict:
    return {task["task_key"]: task for task in _platform()["tasks"]}


# --- the platform Job --------------------------------------------------------------------


def test_platform_job_is_the_reviewed_dag() -> None:
    tasks = _tasks()
    assert tuple(tasks) == EXPECTED_TASKS
    deps = {key: [d["task_key"] for d in task.get("depends_on", [])] for key, task in tasks.items()}
    assert deps["land_sources"] == ["preflight"]
    assert deps["station_source_check"] == ["land_sources"]
    assert deps["station_silver"] == ["station_source_check"]
    assert deps["station_gold"] == ["station_silver"]
    assert deps["historical_trips"] == ["land_sources"]
    assert deps["weather_enrichment"] == ["historical_trips"]
    assert deps["validate_medallion"] == ["station_gold", "historical_trips", "weather_enrichment"]
    assert deps["federation_cdc"] == ["validate_medallion"]
    assert tasks["refresh_lakeflow"]["depends_on"] == [
        {"task_key": "should_refresh_lakeflow", "outcome": "true"}
    ]


def test_platform_job_defaults_to_a_bounded_sample_run_that_does_nothing_unasked() -> None:
    job = _platform()
    defaults = {p["name"]: p["default"] for p in job["parameters"]}
    for name, value in defaults.items():
        if name.startswith("run_") and name != "run_attempt_id":
            assert value == "false", name
    assert defaults["station_source"] == "sample_autoloader"
    assert defaults["historical_execution_id"] == "urbanflow-final-hist-devsample40"
    assert defaults["weather_execution_id"] == "urbanflow-final-weather-devsample48h"
    assert defaults["historical_landing_subdir"] == ""
    assert defaults["weather_source"] == "sample_json"
    assert defaults["source_execution_id"] == "urbanflow-final-{{job.run_id}}"
    assert job["max_concurrent_runs"] == 1
    assert float(defaults["stream_timeout_seconds"]) < _tasks()["land_sources"]["timeout_seconds"]
    assert (
        float(defaults["stream_timeout_seconds"]) < _tasks()["historical_trips"]["timeout_seconds"]
    )
    assert all(task.get("timeout_seconds", 0) <= job["timeout_seconds"] for task in job["tasks"])
    # Every task that does work has its own bound; only the condition check is instantaneous.
    unbounded = [t["task_key"] for t in job["tasks"] if "condition_task" not in t and
                 not t.get("timeout_seconds")]  # fmt: skip
    assert not unbounded, unbounded


def test_platform_job_reuses_the_business_notebooks() -> None:
    paths = {
        key: task["notebook_task"]["notebook_path"]
        for key, task in _tasks().items()
        if "notebook_task" in task
    }
    assert paths == {
        "preflight": "../notebooks/08_unified_preflight.py",
        "land_sources": "../notebooks/02_land_bounded_sources.py",
        "station_source_check": "../notebooks/09_station_source_check.py",
        "station_silver": "../notebooks/04_bronze_to_silver.py",
        "station_gold": "../notebooks/05_silver_to_gold.py",
        "historical_trips": "../notebooks/06_historical_trips.py",
        "weather_enrichment": "../notebooks/07_weather_enrichment.py",
        "validate_medallion": "../notebooks/10_unified_final_validation.py",
        "federation_cdc": "../notebooks/11_federation_cdc.py",
    }
    for path in paths.values():
        assert (JOBS_FILE.parent / path).resolve().is_file(), path


def _widgets(text: str) -> set[str]:
    names = set(re.findall(r'widgets\.(?:text|dropdown|combobox)\(\s*"([^"]+)"', text))
    names |= set(re.findall(r'widget\(\s*"([^"]+)"', text))
    if "widgets.text(name" in text:
        for body in re.findall(r"names = \(([^)]*)\)", text, flags=re.S):
            names |= set(re.findall(r'"([a-z_]+)"', body))
    return names


@pytest.mark.parametrize("job_key", ["urbanflow_final_platform", "urbanflow_final_rag"])
def test_every_notebook_widget_is_supplied_by_the_job(job_key: str) -> None:
    """A widget the Job does not supply silently falls back to its default (often a dry run)."""
    job = _jobs()[job_key]
    job_parameters = {p["name"] for p in job["parameters"]}
    for task in job["tasks"]:
        notebook = (task.get("notebook_task") or {}).get("notebook_path")
        if not notebook:
            continue
        base = task["notebook_task"].get("base_parameters") or {}
        text = (JOBS_FILE.parent / notebook).resolve().read_text(encoding="utf-8")
        missing = _widgets(text) - set(base) - job_parameters
        assert not missing, (task["task_key"], sorted(missing))
        for value in base.values():
            match = re.fullmatch(r"\{\{job\.parameters\.(\w+)\}\}", value)
            if match:
                assert match.group(1) in job_parameters, (task["task_key"], value)


def test_no_task_parameter_is_shadowed_by_a_job_parameter() -> None:
    # Databricks pushes Job parameters to every task and lets them win over a task parameter with
    # the same key, so such a key may only ever carry the Job parameter's own value.
    checked = 0
    for job_key, job in _jobs().items():
        job_parameters = {p["name"] for p in job.get("parameters", [])}
        for task in job["tasks"]:
            for key, value in (task.get("notebook_task") or {}).get("base_parameters", {}).items():
                checked += 1
                if key in job_parameters:
                    assert value == f"{{{{job.parameters.{key}}}}}", (
                        job_key,
                        task["task_key"],
                        key,
                    )
    assert checked > 0


def test_weather_task_selects_trips_by_an_unshadowable_historical_id() -> None:
    parameters = _tasks()["weather_enrichment"]["notebook_task"]["base_parameters"]
    assert parameters["historical_execution_id"] == "{{job.parameters.historical_execution_id}}"
    assert "source_execution_id" not in parameters


def test_historical_bronze_is_read_from_the_namespace_table_everywhere() -> None:
    ingest = (PROJECT_ROOT / "notebooks/06_historical_trips.py").read_text(encoding="utf-8")
    validate = (PROJECT_ROOT / "notebooks/10_unified_final_validation.py").read_text("utf-8")
    assert "historical_bronze_table(table_root, landing_subdir=landing_subdir)" in ingest
    assert ingest.index("foreign_checkpoint_owners(owners") < ingest.index("read_trips_autoloader(")
    assert '"bronze_historical_trips"' not in validate
    assert _tasks()["validate_medallion"]["notebook_task"]["base_parameters"][
        "historical_landing_subdir"
    ] == ("{{job.parameters.historical_landing_subdir}}")


def test_final_validation_notebook_is_read_only() -> None:
    text = (PROJECT_ROOT / "notebooks/10_unified_final_validation.py").read_text(encoding="utf-8")
    code = "\n".join(
        cell for cell in text.split("# COMMAND ----------") if "# MAGIC %md" not in cell
    )
    for forbidden in (".write", "saveAsTable", "CREATE TABLE", "MERGE INTO", "DELETE FROM"):
        assert forbidden not in code
    assert "dbutils.notebook.exit" in code


def test_weather_sources_sent_and_required_are_ones_the_notebook_accepts() -> None:
    notebook = (PROJECT_ROOT / "notebooks/07_weather_enrichment.py").read_text(encoding="utf-8")
    choices = re.search(r'dropdown\("weather_source", "[^"]+", (\[[^\]]+\])\)', notebook)
    accepted = set(json.loads(choices.group(1)))
    preflight = (PROJECT_ROOT / "notebooks/08_unified_preflight.py").read_text(encoding="utf-8")
    required = set(re.findall(r'values\["weather_source"\] != "([a-z_]+)"', preflight))
    default = {p["name"]: p["default"] for p in _platform()["parameters"]}["weather_source"]
    assert {"archive_api", "sample_json"} <= accepted
    assert required <= accepted and default in accepted


# --- the release plan validator -----------------------------------------------------------


def _job_state(target: str, key: str, name: str) -> dict:
    tasks = (
        [{"task_key": task} for task in EXPECTED_TASKS]
        if key == "urbanflow_final_platform"
        else [{"task_key": "ingest"}]
    )
    return {
        "name": f"[dev someone] [{target}] {name}",
        "tasks": tasks,
        "deployment": {
            "metadata_file_path": f"/Workspace/Users/u/.bundle/urbanflow_final_project/{target}/state/metadata.json"
        },
    }


def _plan(action: str = "create", target: str = "personal_dev", **extra: dict) -> dict:
    platform = _job_state(target, "urbanflow_final_platform", "UrbanFlow 2.0 Platform")
    rag = _job_state(target, "urbanflow_final_rag", "UrbanFlow 2.0 Mobility RAG")
    pipeline = {
        "name": f"[{target}] UrbanFlow 2.0 Medallion Pipeline",
        "serverless": True,
        "continuous": False,
        "schema": "urbanflow_final_lakeflow_dev",
    }
    resources = {
        "resources.jobs.urbanflow_final_platform": {
            "action": action,
            "new_state": {"value": platform},
            "remote_state": {"job_id": 11},
        },
        "resources.jobs.urbanflow_final_rag": {
            "action": action,
            "new_state": {"value": rag},
            "remote_state": {"job_id": 12},
        },
        "resources.pipelines.urbanflow_final_pipeline": {
            "action": action,
            "new_state": {"value": pipeline},
        },
        "resources.schemas.urbanflow_final_schema": {"action": action},
    }
    resources.update(extra)
    return {"plan": resources}


def test_plan_validator_accepts_a_first_create_and_an_unchanged_repeat() -> None:
    first = validate_release_plan(
        _plan("create"), phase="pre", target="personal_dev", jobs_payload=[]
    )
    assert first["status"] == "PASS"
    again = validate_release_plan(_plan("skip"), phase="post", target="personal_dev")
    assert again["job_ids"] == {"urbanflow_final_platform": 11, "urbanflow_final_rag": 12}


def test_plan_validator_refuses_deletes_foreign_resources_and_duplicates() -> None:
    plan = _plan("update")
    plan["plan"]["resources.jobs.urbanflow_final_platform"]["changes"] = {
        "tasks[0]": {"action": "delete"}
    }
    with pytest.raises(ValueError, match="delete/remove"):
        validate_release_plan(plan, phase="pre", target="personal_dev")
    with pytest.raises(ValueError, match="outside the reviewed bundle"):
        validate_release_plan(
            _plan(**{"resources.jobs.other": {"action": "create"}}),
            phase="pre",
            target="personal_dev",
        )
    with pytest.raises(ValueError, match="destructive"):
        validate_release_plan(_plan("delete"), phase="pre", target="personal_dev")
    existing = [{"settings": {"name": "[dev someone] [personal_dev] UrbanFlow 2.0 Platform"}}]
    with pytest.raises(ValueError, match="already exists"):
        validate_release_plan(
            _plan("create"), phase="pre", target="personal_dev", jobs_payload=existing
        )
    with pytest.raises(ValueError, match="unchanged"):
        validate_release_plan(_plan("update"), phase="post", target="personal_dev")


def test_plan_validator_refuses_clusters_schedules_and_the_wrong_target() -> None:
    plan = _plan()
    plan["plan"]["resources.jobs.urbanflow_final_platform"]["new_state"]["value"]["tasks"][0][
        "existing_cluster_id"
    ] = "0702-132442-toro5spu"
    with pytest.raises(ValueError, match="cluster compute"):
        validate_release_plan(plan, phase="pre", target="personal_dev")
    plan = _plan()
    plan["plan"]["resources.jobs.urbanflow_final_rag"]["new_state"]["value"]["schedule"] = {"x": 1}
    with pytest.raises(ValueError, match="schedule"):
        validate_release_plan(plan, phase="pre", target="personal_dev")
    plan = _plan()
    plan["plan"]["resources.pipelines.urbanflow_final_pipeline"]["new_state"]["value"][
        "continuous"
    ] = True
    with pytest.raises(ValueError, match="triggered"):
        validate_release_plan(plan, phase="pre", target="personal_dev")
    with pytest.raises(ValueError, match="named"):
        validate_release_plan(_plan(target="personal_dev"), phase="pre", target="azure_prod")


# --- the run validator --------------------------------------------------------------------


def _run(excluded_refresh: bool = True) -> dict:
    tasks = []
    for index, key in enumerate(EXPECTED_TASKS):
        result = "EXCLUDED" if key == "refresh_lakeflow" and excluded_refresh else "SUCCESS"
        tasks.append(
            {
                "task_key": key,
                "run_id": 1000 + index,
                "attempt_number": 0,
                "state": {"life_cycle_state": "TERMINATED", "result_state": result},
            }
        )
    return {
        "job_id": 123,
        "run_id": 456,
        "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
        "tasks": tasks,
    }


def test_run_validator_accepts_a_clean_run_with_an_excluded_lakeflow_refresh() -> None:
    output = {"notebook_output": {"result": json.dumps({"status": "PASS"})}}
    result = validate_job_run(_run(), expected_job_id=123, final_output=output)
    assert result["status"] == "PASS"
    assert result["final_task_run_id"] == 1000 + EXPECTED_TASKS.index("validate_medallion")


def test_run_validator_refuses_failures_retries_exclusions_and_non_pass() -> None:
    failed = _run()
    failed["tasks"][3]["state"]["result_state"] = "FAILED"
    with pytest.raises(ValueError, match="station_silver"):
        validate_job_run(failed, expected_job_id=123)
    retried = _run()
    retried["tasks"][0]["attempt_number"] = 1
    with pytest.raises(ValueError, match="retry"):
        validate_job_run(retried, expected_job_id=123)
    skipped = _run()
    skipped["tasks"][1]["state"]["result_state"] = "EXCLUDED"
    with pytest.raises(ValueError, match="land_sources"):
        validate_job_run(skipped, expected_job_id=123)
    output = {"notebook_output": {"result": json.dumps({"status": "FAIL"})}}
    with pytest.raises(ValueError, match="not PASS"):
        validate_job_run(_run(), expected_job_id=123, final_output=output)


# --- the workflows ------------------------------------------------------------------------


def _workflow(path: Path) -> dict:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["on"] = document.pop(True, document.get("on"))  # YAML 1.1 reads `on` as True
    return document


def test_ci_runs_every_offline_gate_without_secrets_or_cloud_access() -> None:
    text = CI.read_text(encoding="utf-8")
    workflow = _workflow(CI)
    assert set(workflow["on"]) == {"pull_request", "push", "workflow_dispatch"}
    assert workflow["permissions"] == {"contents": "read"}
    for gate in ("ruff check .", "black --check .", "pytest", "python scripts/validate_bundle.py"):
        assert gate in text
    for forbidden in ("secrets.", "databricks bundle deploy", "jobs run-now", "id-token"):
        assert forbidden not in text


def test_cd_is_manual_confirmed_approved_and_pinned_per_target() -> None:
    text = CD.read_text(encoding="utf-8")
    workflow = _workflow(CD)
    assert set(workflow["on"]) == {"workflow_dispatch"}
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    assert inputs["target"]["options"] == ["personal_dev", "azure_prod"]
    assert inputs["run_platform"]["default"] is False
    jobs = workflow["jobs"]
    assert jobs["approve-release"]["environment"] == "final-project-approval"
    assert jobs["release"]["environment"] == "final-project-${{ inputs.target }}"
    assert jobs["release"]["needs"] == "approve-release"
    assert 'expected="RELEASE_URBANFLOW_FINAL_${TARGET^^}"' in text
    assert 'host="https://dbc-1750318a-76a9.cloud.databricks.com"' in text
    assert 'host="https://adb-7405618361895520.0.azuredatabricks.net"' in text
    assert "FORBIDDEN_HOST_DBR_DEV: https://adb-7405604503619901.1.azuredatabricks.net" in text
    assert 'auth="github-oidc"' in text and jobs["release"]["permissions"]["id-token"] == "write"
    assert "DATABRICKS_TOKEN" not in text and "azure/login" not in text


def test_cd_validates_the_plan_before_and_after_deploy_and_the_run_at_the_end() -> None:
    text = CD.read_text(encoding="utf-8")
    pre = text.index("--phase pre")
    deploy = text.index("databricks bundle deploy")
    post = text.index("--phase post")
    assert pre < deploy < post
    assert "--fail-on-active-runs" in text
    assert "scripts/validate_job_run.py" in text
    assert 't["task_key"]=="validate_medallion"' in text
    # The Job id and idempotency token travel in the request body; no positional id.
    assert 'databricks jobs run-now --json @"$RUNNER_TEMP/run-request.json"' in text
    assert 'run-now "$JOB_ID"' not in text
    assert "schedule" not in text


def _bash_usable() -> bool:
    try:
        return subprocess.run(["bash", "-c", "exit 0"], capture_output=True).returncode == 0
    except OSError:
        return False


def test_every_final_project_workflow_run_block_is_valid_shell() -> None:
    """`bash -n` over every `run:` block; YAML parsing cannot catch shell syntax errors."""
    if not _bash_usable():
        pytest.skip("no usable bash on this platform")
    failures, checked = [], 0
    for workflow in (CI, CD):
        for job_name, job in _workflow(workflow)["jobs"].items():
            for index, step in enumerate(job.get("steps") or [], start=1):
                script = step.get("run")
                if not script:
                    continue
                checked += 1
                with tempfile.NamedTemporaryFile(
                    "w", suffix=".sh", delete=False, encoding="utf-8"
                ) as handle:
                    handle.write(script)
                result = subprocess.run(["bash", "-n", handle.name], capture_output=True, text=True)
                Path(handle.name).unlink(missing_ok=True)
                if result.returncode:
                    failures.append(f"{workflow.name}/{job_name}/{index}: {result.stderr.strip()}")
    assert checked > 0
    assert not failures, failures


def test_offline_bundle_validation_includes_every_resource_file() -> None:
    """Validating databricks.yml alone would leave the Job, pipeline and dashboard unchecked."""
    resources = load_bundle()["resources"]
    assert set(resources["jobs"]) == {"urbanflow_final_platform", "urbanflow_final_rag"}
    assert set(resources["pipelines"]) == {"urbanflow_final_pipeline"}
    assert set(resources["dashboards"]) == {"urbanflow_final_dashboard"}
    assert set(resources["volumes"]) == {"urbanflow_final_landing"}


def test_ci_installs_the_cli_before_offline_bundle_validation() -> None:
    workflow = (PROJECT_ROOT.parent / ".github" / "workflows" / "final_project_ci.yml").read_text(
        encoding="utf-8"
    )
    assert workflow.index("databricks/setup-cli") < workflow.index("scripts/validate_bundle.py")
    assert "scripts/build_dashboard.py --check" in workflow
    assert "secrets." not in workflow  # CI never authenticates to a workspace
