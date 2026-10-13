# GitHub CD `plan_only`, corrected dispatch (2026-10-13): service-principal plan converged; workflow FAIL one step later

**Outcome:**
- The workflow authenticated to Personal DEV as the read-only service principal `sp-urbanflow-final-dev`.
- `bundle validate` and `bundle plan` succeeded as that principal.
- The uploaded plan shows **all seven resources `skip` with zero field changes**, so the principal read the existing deployment state and recognised every resource (the state-path pin works).
- The run then **failed** at `databricks jobs list`, which lacked the same `-t "$TARGET"` flag. The release validator therefore did not run inside the workflow.
- Approval and release were skipped. There were **zero Databricks writes and zero compute runs**: the principal holds only read grants.

| Item | Value |
|---|---|
| Run | [38389951946](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38389951946), `workflow_dispatch` on `main` at `34b458b5f6fe225c484bf48dd1e79f001d026ff7` (after PR #76) |
| Inputs | `target=personal_dev`, `mode=plan_only`, `confirm_release=RELEASE_URBANFLOW_FINAL_PERSONAL_DEV` |
| Static validation | **PASS** (typed confirmation, secret scan, Ruff, Black, pytest, dashboard freshness, offline bundle schema) |
| Identity | **`Acting as sp-urbanflow-final-dev 9e50ac7f-fe72-4768-bd9c-8a9bee33d981`** (OAuth M2M, environment `final-project-personal_dev`) |
| `bundle validate` / `bundle plan` | **PASS** as the principal |
| Plan artifact | `final-project-plan-personal_dev-38389951946` (artifact id 11808586909, 45 KB): `pre-plan.json` with dashboard, both Jobs, pipeline, both schemas and Volume all `skip`, no non-skip field |
| Failed step | `Validate bundle and the pre-deploy plan` → `databricks jobs list --output json` → `Error: please specify target` |
| Approve / release jobs | Skipped |
| Cloud changes | None |

## Release validator on the real CD plan (run offline afterwards)

`validate_release_plan.py` was run locally on the downloaded `pre-plan.json`. The `--jobs` list came from the owner's read-only `jobs list`.

| Phase | Result |
|---|---|
| `--phase pre` | **PASS**: the 7 reviewed resources, no create, update or delete, no collision |
| `--phase post` | **PASS**: every action `skip`, i.e. the principal's view is converged with the deployment |

This is an offline check of a plan produced by GitHub Actions, not a validator step inside the workflow.

## Correction (merged before any further dispatch)

All seven workspace commands that run inside the bundle folder now pass `-t "$TARGET"`:
- `jobs list` ×2;
- `jobs run-now`;
- `jobs get-run`;
- `jobs cancel-run` ×2;
- `jobs get-run-output`.

A test asserts that every one of the nine workspace commands (including the two `current-user me`) carries the flag. Locally, `jobs list -t personal_dev` and `jobs get-run -t personal_dev` return clean JSON on stdout from inside the bundle folder.

Per the one-run rule, the workflow was **not dispatched again**. A third dispatch needs the owner's go-ahead:

```bash
gh workflow run final_project_cd.yml --ref main -f target=personal_dev -f mode=plan_only -f confirm_release=RELEASE_URBANFLOW_FINAL_PERSONAL_DEV
```

The OAuth secret stored on 2026-10-13 at 10:22:34 UTC has a 24-hour lifetime. After about 10:22 UTC on 2026-10-14 it must be replaced in the environment before that dispatch.
