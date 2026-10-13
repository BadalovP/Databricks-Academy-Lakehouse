"""Build the run-now request for the CD release's single bounded sample run.

The platform Job's defaults describe a FIRST run on an empty target: a new station execution
(`urbanflow-final-{{job.run_id}}`) and CDC snapshot `batch_001`. Run against a target that
already holds data, those defaults are not harmless:

- a new station execution adds another 40 events through Bronze, Silver and Gold, so every
  snapshot count on the dashboard doubles;
- `batch_001` is MERGEd over the current maintenance state, reverting later batches
  (Personal DEV is at `batch_003`) and writing new change events into the audit history.

So the release defaults to `replay_recorded`: the executions each target already holds,
replayed exactly. That is the configuration the strict idempotency test (step 3) proved writes
no business data: landing reports the files as present, Auto Loader finds nothing new, every
MERGE matches, and an identical CDC snapshot creates no source commit. `first_run` keeps the
Job defaults and is meant only for a target with no Final Project data.

Usage: ``python scripts/cd_sample_request.py --target personal_dev --profile replay_recorded
--job-id <id> --token <idempotency token> --out request.json``
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

# The executions recorded in evidence/ for each target, and the CDC snapshot that equals the
# target's current maintenance source. Update together with a new recorded release.
RECORDED_STATE: dict[str, dict[str, str]] = {
    "personal_dev": {
        # Step 2 run 896549012431425 and step 3 run 412521206243476 used this execution.
        "source_execution_id": "urbanflow-final-304828008837365",
        # Step 4C: the source equals batch_003 (checkpoint 4, history 11).
        "cdc_snapshot": "batch_003",
    },
    "azure_prod": {
        # Step 9 run 240527908342651.
        "source_execution_id": "urbanflow-final-240527908342651",
        "cdc_snapshot": "batch_001",
    },
}
BRANCHES = {
    "run_station_pipeline": "true",
    "run_historical": "true",
    "run_weather": "true",
    "run_federation_cdc": "true",
    "run_full_month": "false",
    "run_lakeflow": "false",
}
PROFILES = ("replay_recorded", "first_run")


def job_parameters(target: str, profile: str) -> dict[str, str]:
    if target not in RECORDED_STATE:
        raise ValueError(f"Unknown target {target!r}.")
    if profile not in PROFILES:
        raise ValueError(f"Unknown sample profile {profile!r}; use one of {PROFILES}.")
    parameters = dict(BRANCHES)
    if profile == "replay_recorded":
        parameters.update(RECORDED_STATE[target])
    else:  # first_run: the Job's own defaults - a new execution and batch_001
        parameters["cdc_snapshot"] = "batch_001"
    return parameters


def build_request(target: str, profile: str, job_id: int, token: str) -> dict:
    if job_id <= 0 or not token:
        raise ValueError("A positive Job ID and an idempotency token are required.")
    return {
        "job_id": job_id,
        "idempotency_token": token,
        "job_parameters": job_parameters(target, profile),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True)
    parser.add_argument("--profile", required=True, choices=PROFILES)
    parser.add_argument("--job-id", type=int, required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    request = build_request(args.target, args.profile, args.job_id, args.token)
    args.out.write_text(json.dumps(request, indent=1), encoding="utf-8")
    print(json.dumps(request["job_parameters"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
