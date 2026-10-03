from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from scripts.validate_job_run import EXPECTED_TASKS, validate_job_run
from scripts.validate_release_plan import (
    EXPECTED_METADATA_PATH,
    EXPECTED_NAME,
    RESOURCE_KEY,
    validate_release_plan,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]
GP1_ID = "0702-132442-toro5spu"
WORKFLOW = REPOSITORY_ROOT / ".github/workflows/demo3_urbanflow_deploy.yml"


def _job() -> dict:
    resources = yaml.safe_load((PROJECT_ROOT / "resources/jobs.yml").read_text(encoding="utf-8"))
    return resources["resources"]["jobs"]["urbanflow_end_to_end"]


def test_unified_job_is_the_real_seven_task_primary_dag() -> None:
    job = _job()
    tasks = {task["task_key"]: task for task in job["tasks"]}

    assert job["name"] == "[${bundle.target}] UrbanFlow End-to-End"
    assert tuple(tasks) == EXPECTED_TASKS
    assert tasks["02_station_source_check"]["depends_on"] == [{"task_key": "01_preflight"}]
    assert tasks["03_station_silver"]["depends_on"] == [{"task_key": "02_station_source_check"}]
    assert tasks["04_station_gold"]["depends_on"] == [{"task_key": "03_station_silver"}]
    assert tasks["05_historical_trips"]["depends_on"] == [{"task_key": "01_preflight"}]
    assert tasks["06_weather_enrichment"]["depends_on"] == [{"task_key": "05_historical_trips"}]
    assert tasks["07_final_validation"]["depends_on"] == [
        {"task_key": "04_station_gold"},
        {"task_key": "05_historical_trips"},
        {"task_key": "06_weather_enrichment"},
    ]


def test_unified_job_uses_gp1_only_and_never_contains_a_producer() -> None:
    job = _job()
    text = json.dumps(job)

    assert "schedule" not in job
    assert "03_eventhubs_to_bronze" not in text
    assert "producer" not in " ".join(task["task_key"] for task in job["tasks"])
    for task in job["tasks"]:
        assert task["existing_cluster_id"] == GP1_ID
        assert "new_cluster" not in task
    databricks = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text(encoding="utf-8"))
    assert databricks["variables"]["compute_cluster_id"]["default"] == GP1_ID


def test_unified_job_defaults_to_bounded_sample_mode() -> None:
    job = _job()
    defaults = {item["name"]: item["default"] for item in job["parameters"]}

    assert defaults["run_station_pipeline"] == "true"
    assert defaults["run_historical"] == "true"
    assert defaults["run_weather"] == "true"
    assert defaults["run_full_month"] == "false"
    assert defaults["source_execution_id"] == "urbanflow-20260929T195132Z-r3"
    assert defaults["historical_execution_id"] == "urbanflow-hist-devsample40-20261002T0010Z"
    assert defaults["historical_landing_subdir"] == ""
    assert defaults["weather_source"] == "sample_json"
    assert defaults["stream_timeout_seconds"] == "900"
    assert job["timeout_seconds"] == 5400

    historical = next(task for task in job["tasks"] if task["task_key"] == "05_historical_trips")
    assert historical["timeout_seconds"] == 3600


def test_unified_job_reuses_existing_business_notebooks() -> None:
    tasks = {task["task_key"]: task for task in _job()["tasks"]}
    paths = {key: value["notebook_task"]["notebook_path"] for key, value in tasks.items()}

    assert paths["03_station_silver"] == "../notebooks/04_bronze_to_silver.py"
    assert paths["04_station_gold"] == "../notebooks/05_silver_to_gold.py"
    assert paths["05_historical_trips"] == "../notebooks/06_historical_trips.py"
    assert paths["06_weather_enrichment"] == "../notebooks/07_weather_enrichment.py"
    assert paths["07_final_validation"] == "../notebooks/10_unified_final_validation.py"


def test_final_validation_notebook_is_read_only() -> None:
    text = (PROJECT_ROOT / "notebooks/10_unified_final_validation.py").read_text(encoding="utf-8")
    code = "\n".join(
        cell for cell in text.split("# COMMAND ----------") if "# MAGIC %md" not in cell
    )

    for forbidden in (".write", "saveAsTable", "CREATE TABLE", "MERGE INTO", "DELETE FROM"):
        assert forbidden not in code
    assert "dbutils.notebook.exit" in code
    assert '"status": "PASS"' in code


def test_release_workflow_is_dispatch_only_and_protected() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.load(text, Loader=yaml.BaseLoader)

    assert set(workflow["on"]) == {"workflow_dispatch"}
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    assert inputs["run_job"]["default"] == "true"
    assert inputs["full_month"]["default"] == "false"
    assert "DEPLOY_AND_RUN_URBANFLOW" in text
    assert "environment: azure-release-approval" in text
    assert "needs: approve-release" in text


def test_release_workflow_deploys_exactly_one_selected_job_and_rejects_deletes() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    deploy_lines = [
        line.strip() for line in text.splitlines() if "databricks bundle deploy" in line
    ]
    assert deploy_lines == [
        "databricks bundle deploy -t azure --select jobs.urbanflow_end_to_end --auto-approve --fail-on-active-runs"
    ]
    assert text.count("--select jobs.urbanflow_end_to_end") == 3
    assert "scripts/validate_release_plan.py --phase pre" in text
    assert "scripts/validate_release_plan.py --phase post" in text
    assert "bundle destroy" not in text


def test_release_workflow_guards_gp1_and_never_mutates_its_lifecycle() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert f"databricks clusters get {GP1_ID}" in text
    assert 'cluster["state"] == "RUNNING"' in text
    assert "databricks workspace get-status" in text
    for forbidden in (
        "databricks clusters start",
        "databricks clusters restart",
        "databricks clusters resize",
        "databricks clusters delete",
    ):
        assert forbidden not in text


def test_release_workflow_monitors_first_run_and_gates_the_repeat() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "databricks jobs run-now" in text
    assert "databricks jobs get-run" in text
    assert "databricks jobs get-run-output" in text
    assert "run_once first" in text
    assert 'if [[ "$REPEAT_RUN" == "true" ]]; then run_once second; fi' in text
    assert "scripts/validate_job_run.py" in text


def test_release_workflow_gives_monthly_mode_a_bounded_but_realistic_window() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert '"stream_timeout_seconds": "2700" if full else "900"' in text
    assert "for _ in $(seq 1 420); do" in text


def _plan(action: str, *, name: str = EXPECTED_NAME, changes: dict | None = None) -> dict:
    resource = {"action": action, "changes": changes or {}}
    state = {
        "job_id": 123,
        "name": name,
        "deployment": {"metadata_file_path": EXPECTED_METADATA_PATH},
        "tasks": [{"task_key": key, "existing_cluster_id": GP1_ID} for key in EXPECTED_TASKS],
    }
    if action != "create":
        resource["remote_state"] = state
    else:
        resource["new_state"] = {"value": state}
    return {"plan": {RESOURCE_KEY: resource}}


def _jobs(*ids: int) -> list[dict]:
    return [{"job_id": value, "settings": {"name": EXPECTED_NAME}} for value in ids]


def test_plan_validator_allows_first_create_and_subsequent_unchanged() -> None:
    assert (
        validate_release_plan(_plan("create"), phase="pre", jobs_payload=_jobs())["action"]
        == "create"
    )
    result = validate_release_plan(_plan("skip"), phase="post", jobs_payload=_jobs(123))
    assert result == {"status": "PASS", "phase": "post", "action": "skip", "job_id": 123}


def test_plan_validator_blocks_duplicate_create_delete_and_other_resources() -> None:
    with pytest.raises(ValueError, match="already exists"):
        validate_release_plan(_plan("create"), phase="pre", jobs_payload=_jobs(123))
    with pytest.raises(ValueError, match="delete/remove"):
        validate_release_plan(
            _plan("update", changes={"tasks[0]": {"action": "delete"}}), phase="pre"
        )
    with pytest.raises(ValueError, match="exactly"):
        validate_release_plan(
            {
                "plan": {
                    RESOURCE_KEY: {"action": "skip"},
                    "resources.jobs.other": {"action": "skip"},
                }
            },
            phase="pre",
        )


def _successful_run() -> dict:
    return {
        "job_id": 123,
        "run_id": 456,
        "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
        "tasks": [
            {
                "task_key": key,
                "run_id": index + 1000,
                "attempt_number": 0,
                "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
            }
            for index, key in enumerate(EXPECTED_TASKS)
        ],
    }


def test_job_run_validator_requires_every_successful_task_and_final_pass() -> None:
    output = {"notebook_output": {"result": json.dumps({"status": "PASS", "checks": 31})}}
    result = validate_job_run(_successful_run(), expected_job_id=123, final_output=output)
    assert result["status"] == "PASS"
    assert result["final_task_run_id"] == 1006


def test_job_run_validator_rejects_failure_retry_and_non_pass_output() -> None:
    failed = _successful_run()
    failed["tasks"][2]["state"]["result_state"] = "FAILED"
    with pytest.raises(ValueError, match="03_station_silver"):
        validate_job_run(failed, expected_job_id=123)

    retried = _successful_run()
    retried["tasks"][0]["attempt_number"] = 1
    with pytest.raises(ValueError, match="retry"):
        validate_job_run(retried, expected_job_id=123)

    output = {"notebook_output": {"result": json.dumps({"status": "FAIL"})}}
    with pytest.raises(ValueError, match="not PASS"):
        validate_job_run(_successful_run(), expected_job_id=123, final_output=output)


def test_release_workflow_passes_job_id_in_the_run_now_body_not_positionally() -> None:
    """`databricks jobs run-now` rejects a positional JOB_ID when --json is supplied.

    The first live release attempt failed exactly here, after a successful deploy and a clean
    post-deploy plan: "when --json flag is specified, no positional arguments are allowed.
    Provide 'job_id' in your JSON input". The id and the idempotency token are body fields, so
    this test pins the corrected shape rather than the error message.
    """
    text = WORKFLOW.read_text(encoding="utf-8")

    assert 'databricks jobs run-now --json @"$RUNNER_TEMP/${label}-request.json"' in text
    # No positional job id alongside --json, in any form.
    assert 'run-now "$JOB_ID"' not in text
    assert "--idempotency-token" not in text
    # Both fields must travel inside the request body, built without a heredoc.
    assert '"job_id": int(sys.argv[1])' in text
    assert '"idempotency_token": sys.argv[2]' in text
    # A heredoc inside the function body is what broke this step once; keep it out.
    assert "<<'PY'" not in text.split("run_once()")[-1]


def _bash_usable() -> bool:
    """True only when a real bash can execute. On Windows, `bash` may resolve to a WSL shim."""
    import subprocess

    try:
        return subprocess.run(["bash", "-c", "exit 0"], capture_output=True).returncode == 0
    except OSError:
        return False


def test_every_workflow_run_block_is_valid_shell() -> None:
    """`bash -n` over every `run:` block in every workflow.

    This exists because of a real failure that reached the live workspace. A `<<'PY'` heredoc was
    added inside a bash function with its terminator indented to match the surrounding code. Bash
    only matches an unindented terminator, so it consumed the rest of the script and failed with
    "syntax error: unexpected end of file" - after a successful deploy, in the step that triggers
    the Job. YAML parsing cannot catch that, because the block is a valid YAML string.

    Skipped where bash cannot execute; CI runs on ubuntu, which is where it matters.
    """
    import subprocess
    import tempfile

    import pytest as _pytest
    import yaml as _yaml

    if not _bash_usable():
        _pytest.skip("no usable bash on this platform")

    root = WORKFLOW.resolve().parent  # the workflows directory itself, not .github/
    failures: list[str] = []
    checked = 0
    for workflow in sorted(root.glob("*.yml")):
        document = _yaml.safe_load(workflow.read_text(encoding="utf-8"))
        for job_name, job in (document.get("jobs") or {}).items():
            for index, step in enumerate(job.get("steps") or [], start=1):
                script = step.get("run")
                if not script:
                    continue
                checked += 1
                with tempfile.NamedTemporaryFile(
                    "w", suffix=".sh", delete=False, encoding="utf-8"
                ) as handle:
                    handle.write(script)
                    path = handle.name
                result = subprocess.run(["bash", "-n", path], capture_output=True, text=True)
                Path(path).unlink(missing_ok=True)
                if result.returncode != 0:
                    failures.append(
                        f"{workflow.name}/{job_name}/step {index}: {result.stderr.strip()}"
                    )

    assert checked > 0, "no run blocks found - the glob is wrong and this test is vacuous"
    assert not failures, "shell syntax errors:\n" + "\n".join(failures)


def test_no_task_parameter_is_shadowed_by_a_job_parameter() -> None:
    # Databricks pushes job parameters down to every task and lets them win over a task
    # parameter of the same key. Unified run 157686710394279 failed exactly this way: the
    # weather task's `source_execution_id <- historical_execution_id` mapping was silently
    # replaced by the station `source_execution_id` job parameter.
    resources = yaml.safe_load((PROJECT_ROOT / "resources/jobs.yml").read_text(encoding="utf-8"))
    checked = 0
    for job_key, job in resources["resources"]["jobs"].items():
        job_parameters = {parameter["name"] for parameter in job.get("parameters", [])}
        for task in job["tasks"]:
            for key, value in task.get("notebook_task", {}).get("base_parameters", {}).items():
                checked += 1
                if key in job_parameters:
                    assert value == f"{{{{job.parameters.{key}}}}}", (
                        job_key,
                        task["task_key"],
                        key,
                    )
    assert checked > 0


def test_weather_task_selects_trips_by_an_unshadowable_historical_id() -> None:
    task = {task["task_key"]: task for task in _job()["tasks"]}["06_weather_enrichment"]
    parameters = task["notebook_task"]["base_parameters"]
    assert parameters["historical_execution_id"] == "{{job.parameters.historical_execution_id}}"
    assert "source_execution_id" not in parameters

    notebook = (PROJECT_ROOT / "notebooks/07_weather_enrichment.py").read_text(encoding="utf-8")
    assert 'dbutils.widgets.text("historical_execution_id", "")' in notebook
    # The dedicated widget must take precedence; the fallback keeps the standalone Job working.
    assert notebook.index('widgets.get("historical_execution_id")') < notebook.index(
        'source_execution_id or dbutils.widgets.get("source_execution_id")'
    )
