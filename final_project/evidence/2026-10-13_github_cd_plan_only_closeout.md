# GitHub CD `plan_only` closeout - 2026-10-13

## Outcome

- Run: [38382998636](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38382998636), commit `225545fd1f54607d23996c58daee7ef7e1c924fd`.
- Request: `personal_dev`, `plan_only`, typed confirmation accepted.
- Result: **FAIL**, with no Databricks write and no compute run.
- Static validation: **PASS**. Secret scan, Ruff, Black, dashboard freshness and offline bundle validation passed; pytest reported **859 passed, 1 skipped** on Linux.
- The approval and release jobs were skipped, as required for `plan_only`.

## Exact failure

The read-only plan job selected the pinned Personal DEV host and OAuth M2M authentication, then stopped in **Verify the deploying identity** with:

```text
Error: please specify target
```

The command ran from the bundle root as `databricks current-user me`. This project intentionally has no default bundle target, so the CLI required `-t "$TARGET"` before it attempted the identity API call. Authentication identity, bundle plan and plan validator were therefore not observed. The artifact step ran with `if: always()`, but no plan file existed to upload.

The smallest correction adds `-t "$TARGET"` to both identity checks and a regression assertion. Per the one-run rule, the workflow was **not retried**.

## Identity and environments

The repository now has:

- `final-project-approval`: required reviewer `BadalovP`, administrator bypass disabled;
- `final-project-personal_dev`: `main` only, `DATABRICKS_CLIENT_ID`, `DATABRICKS_WAREHOUSE_ID`, and one encrypted `DATABRICKS_CLIENT_SECRET`;
- `final-project-azure_prod`: `main` only and `DATABRICKS_WAREHOUSE_ID`; no Databricks identity credential.

Personal DEV service principal `sp-urbanflow-final-dev` has workspace access and view-only access to the two Final Project Jobs, pipeline, dashboard and warehouse metadata. It has no admin role, ownership or `CAN MANAGE` permission. Its 24-hour OAuth secret was transferred directly to GitHub and was not printed or written to disk.

## Bundle-state boundary

The owner-identity plan is converged: all seven resources are `skip` under:

```text
/Workspace/Users/parvinbadalov@yahoo.com/.bundle/urbanflow_final_project/personal_dev
```

The GitHub run failed before a service-principal plan, so it did **not** establish whether the principal recognized those resources. Because `workspace.root_path` contains `${workspace.current_user.userName}`, the principal would resolve a different state location. No ownership transfer, state rewrite or deployment was attempted. A separately reviewed state/identity migration is required before any service-principal deploy.

## Final idle-state audit

Personal DEV: both Jobs have zero active runs and no schedule/trigger; the pipeline is `IDLE`; warehouse `410e5f3b4784e981` is `STOPPED`; no AI Search endpoint, cleanup guard Job, temporary governance schema, or active cluster remains.

Azure Trial 2: both Jobs have zero active runs and no schedule/trigger; the pipeline is `IDLE`; warehouse `d8b8ff9942968cb8` is `STOPPED`; no Final Project AI Search endpoint or active cluster remains. No Final Project service principal was visible in the workspace. Account-level OIDC federation remains **BLOCKED BY IDENTITY SETUP**; no Academy permission was changed.

## Continuation after the handoff (2026-10-13, Claude Code)

The record above is kept as written. The continuation verified it and closed the gaps before a second dispatch:

1. **Configuration verified (read-only):**
   - environments, reviewer, branch policies, variable and secret names (the values stay redacted; the secret was set at 10:22:34 UTC);
   - the service principal's entitlements and grants, exactly as described above.
2. **State isolation fixed:**
   - Both `workspace.root_path` values are pinned to the existing deployment folders.
   - The owner plans stayed all `skip` for `personal_dev` and `azure_prod`, and both post-phase validators passed. Read-only.
   - `test_every_target_reads_the_existing_deployment_state_whoever_authenticates` guards the pin.
3. **Plan needs only reads:**
   - `bundle plan --debug` issues GET requests only, and takes no lock.
   - The principal received the remaining read-only grants it needs:
     - `CAN_READ` on the state folder;
     - `USE CATALOG` on `workspace`;
     - `USE SCHEMA` on the two schemas;
     - `READ VOLUME` on the Volume.
   - No admin role, ownership or `CAN_MANAGE`.
4. **The `-t "$TARGET"` fix** is merged with these changes before the single corrected dispatch. Its result is recorded in a separate evidence file.
