"""The CD sample run must never revert or duplicate the data a target already holds."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "cd_sample_request", ROOT / "scripts" / "cd_sample_request.py"
)
builder = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = builder
_SPEC.loader.exec_module(builder)


def test_personal_dev_replay_keeps_batch_003_and_the_recorded_station_execution() -> None:
    params = builder.job_parameters("personal_dev", "replay_recorded")
    assert params["cdc_snapshot"] == "batch_003"  # never batch_001: that would revert CDC
    assert params["source_execution_id"] == "urbanflow-final-304828008837365"
    assert params["run_lakeflow"] == "false" and params["run_full_month"] == "false"
    for branch in ("run_station_pipeline", "run_historical", "run_weather", "run_federation_cdc"):
        assert params[branch] == "true"


def test_the_recorded_state_matches_the_evidence() -> None:
    evidence = ROOT / "evidence"
    step3 = (evidence / "2026-10-12_personal_dev_step3_retry_pass.md").read_text("utf-8")
    assert "412521206243476" in step3
    batch003 = (evidence / "2026-10-12_personal_dev_step4_batch003.md").read_text("utf-8")
    assert "checkpoint moved **3 → 4**" in batch003
    azure = (evidence / "2026-10-13_azure_prod_step9_deploy_and_sample_run.md").read_text("utf-8")
    assert "urbanflow-final-240527908342651" in azure
    assert builder.RECORDED_STATE["azure_prod"] == {
        "source_execution_id": "urbanflow-final-240527908342651",
        "cdc_snapshot": "batch_001",
    }


def test_first_run_keeps_the_job_defaults_for_an_empty_target() -> None:
    params = builder.job_parameters("personal_dev", "first_run")
    assert "source_execution_id" not in params  # the Job derives a new one from its run ID
    assert params["cdc_snapshot"] == "batch_001"


@pytest.mark.parametrize("target, profile", [("dbr_dev", "replay_recorded"), ("personal_dev", "x")])
def test_unknown_targets_and_profiles_are_refused(target, profile) -> None:
    with pytest.raises(ValueError):
        builder.job_parameters(target, profile)


def test_the_request_carries_the_job_and_token_and_is_written_as_json(tmp_path) -> None:
    out = tmp_path / "request.json"
    assert (
        builder.main(
            [
                "--target", "personal_dev", "--profile", "replay_recorded",
                "--job-id", "158911626270744", "--token", "urbanflow-final-1-1", "--out", str(out),
            ]
        )  # fmt: skip
        == 0
    )
    request = json.loads(out.read_text(encoding="utf-8"))
    assert request["job_id"] == 158911626270744
    assert request["idempotency_token"] == "urbanflow-final-1-1"
    assert request["job_parameters"]["cdc_snapshot"] == "batch_003"
    with pytest.raises(ValueError):
        builder.build_request("personal_dev", "replay_recorded", 0, "t")


def test_every_parameter_is_one_the_platform_job_declares() -> None:
    import yaml

    job = yaml.safe_load((ROOT / "resources" / "urbanflow_final.job.yml").read_text("utf-8"))
    declared = {
        p["name"] for p in job["resources"]["jobs"]["urbanflow_final_platform"]["parameters"]
    }
    for target in builder.RECORDED_STATE:
        for profile in builder.PROFILES:
            assert set(builder.job_parameters(target, profile)) <= declared
