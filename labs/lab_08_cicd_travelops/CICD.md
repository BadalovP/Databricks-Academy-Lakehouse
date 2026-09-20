# LAB 08 - TravelOps CI/CD

## Pull Requests

PR workflows always run unit tests, ruff, black, and strict bundle validation. When `LAB08_ENABLE_AZURE_OIDC=true`, they also run Personal Terraform validation and read-only Personal/Azure PROD remote-state plans. The temporary PAT path never runs on a PR. PRs never apply Terraform, deploy Azure PROD, run Azure PROD, or run Personal PROD.

## Main

Merges to `main` use separate jobs and `needs:` dependencies. Both modes first run Unit Tests -> Bundle Validate -> Deploy DEV -> Validate DEV -> Deploy Personal PROD -> Validate Personal PROD. `LAB08_ENABLE_AZURE_OIDC=true` continues through Terraform Plan -> Terraform Apply -> Deploy Azure PROD -> Validate Azure PROD. `LAB08_ENABLE_AZURE_OIDC=false` instead runs one temporary PAT job that validates, plans, deploys, runs, and verifies Azure PROD without invoking Azure CLI or Terraform.

The advanced Terraform jobs require the complete imported Azure PROD state inventory and generate fresh saved plans. The Access Connector role assignment must have the exact Terraform action `no-op`; any create, update, delete, replacement, or missing plan entry fails before apply because the GitHub identity does not have RBAC administration permission. The apply job then refuses every remaining non-no-op resource change, applies only the verified saved plan, and requires a final detailed-exit-code plan of `0`. A fully unchanged plan does not require `roleAssignments/write`.

The advanced Azure PROD application job runs only when both `LAB08_ENABLE_AZURE_OIDC=true` and `LAB08_RUN_AZURE_PROD_JOB=true`. The temporary PAT path runs the promotion job automatically after a delete-free bundle plan and deployment, then requires normal validation and a final `0 add, 0 change, 0 delete` plan. Azure PROD intentionally avoids `--strict` because its pinned human-owned workspace root retains a human `CAN_MANAGE` ACL outside bundle management; declaring that ACL in the bundle would conflict with preserving the live job and pipeline `IS_OWNER` ownership.

## Manual Workflow Dispatch (Live Demonstration)

In addition to `pull_request` and `push`, the workflow accepts a manual `workflow_dispatch` run from `main`. This exists to demonstrate the full release chain on demand without waiting for a real code change, and it deliberately bypasses the `detect-changes` docs/workflow-only guard described above -- a human explicitly asked for a full run. Every other safeguard still applies exactly as it does for a push: Unit Tests, Bundle Validate, the `personal-prod-approval` gate, and the `azure-release-approval` gate (for whichever Azure path is selected) all still run.

A manual run takes two inputs instead of reading `LAB08_ENABLE_AZURE_OIDC` / `LAB08_RUN_AZURE_PROD_JOB`:

- `azure_auth_mode` (`oidc`, the default, or `pat`) selects which Azure path executes for that one run only. It does not read or change the `LAB08_ENABLE_AZURE_OIDC` repository variable, which continues to select the Azure path for an ordinary push exactly as before. As with the variable, only one path can ever be eligible per run: every job in the OIDC path also checks that `azure_auth_mode == 'oidc'` (or, for a push, that the variable is `true`), and every job in the PAT path checks the opposite, so choosing one always leaves the other path's jobs correctly `skipped`.
- `run_azure_prod_job` (boolean, default `false`) gates only the final Azure PROD job-execution step -- `run-azure-prod` for OIDC, `run-azure-prod-pat` for PAT -- for that one manual run, after Azure deployment (and, for OIDC, validation) succeeds. It never reads or writes the `LAB08_RUN_AZURE_PROD_JOB` repository variable. Leaving it unchecked still deploys and runs Personal DEV, deploys and runs Personal PROD, and deploys (and, for OIDC, validates) Azure PROD -- it only skips the very last job.

This is also why the PAT path's deployment (`deploy-azure-prod-pat`) and job-execution (`run-azure-prod-pat`) steps are split into two separate jobs: splitting them lets `run_azure_prod_job` gate execution the same way for both Azure paths on a manual run. On an ordinary push, the PAT path's job-run step keeps its original unconditional behavior (it always runs once PAT deployment succeeds, with no equivalent of `LAB08_RUN_AZURE_PROD_JOB`); the OIDC path's push behavior is unchanged too, and still requires `LAB08_RUN_AZURE_PROD_JOB=true`. Only the manual-dispatch case treats the two paths symmetrically.

Choosing `azure_auth_mode: oidc` on a manual run leaves `approve-azure-release-pat`, `deploy-azure-prod-pat`, and `run-azure-prod-pat` correctly `skipped`; choosing `pat` leaves `terraform-plan`, `approve-azure-release`, `terraform-apply`, `deploy-azure-prod`, `validate-azure-prod`, and `run-azure-prod` correctly `skipped`. A skipped job in the unused path is expected and is not a failure -- see the README's "What grey boxes mean" note.

Both paths have now been exercised end to end by a real manual run, including the final Azure PROD job: OIDC in run #46, PAT in run #45. See `README.md` for the illustrated walkthrough and `evidence/FINAL_RUNS_2026-09-20.md` for the run-level record.

## Repository Variables

Temporary PAT mode requires repository variable `LAB08_ENABLE_AZURE_OIDC=false` and `DATABRICKS_AZURE_HOST`, plus the two tokens listed below. It uses the bundle's Personal workspace host and known storage account as fallbacks, so the current four-setting configuration is sufficient.

The complete variable inventory for both modes is:

- `DATABRICKS_PERSONAL_HOST` (optional override; defaults to the Personal workspace configured in the bundle)
- `DATABRICKS_AZURE_HOST`
- `AZURE_SUBSCRIPTION_ID`
- `AZURE_TENANT_ID`
- `AZURE_CLIENT_ID`
- `LAB08_RESOURCE_GROUP_NAME`
- `LAB08_PERSONAL_VOLUME_GRANTEE`
- `LAB08_AZURE_VOLUME_GRANTEE`
- `LAB08_AZURE_STORAGE_ACCOUNT_NAME` (required by advanced OIDC mode; the temporary PAT path defaults to current resource `lab08travelops63e621`)
- `TF_STATE_RESOURCE_GROUP`
- `TF_STATE_STORAGE_ACCOUNT`
- `TF_STATE_CONTAINER`
- `LAB08_ENABLE_AZURE_OIDC` (`false` for the temporary PAT path; `true` for OIDC/Terraform; only consulted for a push -- a manual `workflow_dispatch` run selects its Azure path with the `azure_auth_mode` input instead, independently of this variable's current value; see "Manual Workflow Dispatch" above)
- `LAB08_RUN_AZURE_PROD_JOB` (leave unset or `false` in current safety mode; only consulted for a push on the OIDC path -- a manual `workflow_dispatch` run instead gates the same final job step with the `run_azure_prod_job` input, independently of this variable)

Azure identity, resource-group, storage-account, and remote-state variables are consumed only by the preserved advanced OIDC/Terraform jobs.

## Repository Secrets

Personal Databricks always requires one scoped secret:

- `DATABRICKS_PERSONAL_TOKEN`

The temporary Azure path additionally requires:

- `DATABRICKS_AZURE_TOKEN`

When `LAB08_ENABLE_AZURE_OIDC=true`, Azure jobs use GitHub OIDC plus Azure CLI authentication and do not use the PAT. When it is `false`, only the temporary Azure DAB job receives `DATABRICKS_AZURE_TOKEN`; Terraform and managed-identity code remain present but are skipped.

## GitHub Environments

Use `personal-dev`, `personal-prod`, and `azure-prod` for scoped variables, secrets, and auditability. Configure GitHub-to-Azure federated credentials for the advanced workflow subjects that use `azure-prod`. The temporary PAT can be scoped to the `azure-prod` environment. Do not add mandatory human approval to `azure-prod` if the academy final criterion requires automatic main-to-PROD deployment.

## Remote State Safety

Checked-in Terraform roots retain separate local backends for independent workstation use. GitHub Actions stages each root with `.github/terraform/lab08-azurerm-backend.tf` and uses distinct Azure Blob keys: `lab08/personal.tfstate` and `lab08/azure-prod.tfstate`.

Remote state is used only by the advanced OIDC/Terraform path. The temporary PAT path does not initialize, read, migrate, or mutate Terraform state and therefore does not require Azure control-plane or RBAC permissions.

On 2026-09-15, the complete local states were hash-backed up outside the repository and migrated with `terraform init -migrate-state`. The remote keys now exist with preserved lineages: `lab08/personal.tfstate` contains four managed resources and `lab08/azure-prod.tfstate` contains nine. Fresh remote-backed plans returned `No changes`; the Azure RBAC assignment and application schema both planned `no-op`. The checked-in local backend files remain for isolated workstation configuration, but the Azure Blob states are authoritative for CI and local state files must not be applied after migration.

The workflow refuses to plan if Personal state is missing either Volume/grant or if Azure state is missing any of its nine final resources: storage account, filesystem, access connector, imported RBAC assignment, storage credential, external location, external Volume, singular Volume grant, or application schema. It also verifies the Azure state storage-account output against `LAB08_AZURE_STORAGE_ACCOUNT_NAME`. These checks prevent an ephemeral runner from recreating existing resources or using the wrong state.

The OIDC principal must also be added to the Azure Databricks workspace with access to the pinned bundle root and existing DAB resources. This is separate from its Azure control-plane roles. The shared SQL warehouse requires a one-time `CAN_USE` bootstrap because it is referenced but not owned by the bundle or Terraform.

## Concurrency

The workflow uses concurrency groups so personal PROD, Terraform PROD, and Azure PROD deployment do not collide with another run.

## Current Azure Status

The Azure administrator created the `Storage Blob Data Contributor` assignment for connector principal `dbb45359-22b9-4744-8467-2ea8633bd999` at storage account `lab08travelops63e621`. Assignment `c0e24625-edc3-444a-8112-a7327380a96a` is imported as `azurerm_role_assignment.uc_storage_blob_data_contributor`; Terraform must not create a duplicate. The GitHub identity is federated for the `azure-prod` environment, has Contributor on `PL_24_Databricks`, and has Storage Blob Data Contributor on backend account `dlspl21databricks`. It does not have RBAC Administrator, which is safe while the assignment remains unchanged.

**(Historical -- superseded by the "Update" and "Further update" paragraphs below.)** Advanced run `35021845707` authenticated with OIDC, completed the remote-state Terraform plan/apply path with no infrastructure changes, and reached Azure DAB deployment. That deployment exposed an owner-permission reconciliation caused by declaring the existing human `IS_OWNER` principal as `CAN_MANAGE`; the declaration is now removed, and `LAB08_ENABLE_AZURE_OIDC` remains `false` pending review and an authorized rerun. Service principal `github-lab08-travelops` (SCIM ID `141097843869075`, client ID `3ec7e8df-66a2-4102-ab57-e4448b4e0e01`) is active with workspace and SQL access. It has direct `CAN_MANAGE` on the pinned Azure bundle root, promotion job, and Lakeflow pipeline; dashboard and alert management inherit from the root; and it has direct `CAN_USE` on warehouse `3ed106620db591d9`. Existing user ownership remains unchanged.

The Azure target declares only the service principal at target-level `CAN_MANAGE`. The existing human `IS_OWNER` entry is intentionally omitted from bundle-managed permissions so a non-admin OIDC deployment cannot reinterpret it as `CAN_MANAGE` and attempt an ownership change. The owner and run-as identity remain `parvinbadalov@softserve.academy`. No Unity Catalog grant was added: effective-grant inspection for the service principal already returns the existing `account users` `ALL_PRIVILEGES` grant on `dbr_dev`, inherited by the application schema, raw schema, and raw Volume. The Terraform-managed singular Volume grant remains assigned only to `parvinbadalov@softserve.academy`. PAT mode remains operational and runs the promotion job automatically. Advanced application runs additionally require `LAB08_RUN_AZURE_PROD_JOB=true`.

**Update:** `LAB08_ENABLE_AZURE_OIDC` is now `true` (the owner-permission reconciliation above was resolved). The advanced OIDC/Terraform/Deploy Azure PROD path has since completed successfully end to end, gated by two dedicated manual-approval jobs added after this status was first written: `approve-personal-prod` (environment `personal-prod-approval`, before `deploy-personal-prod`) and `approve-azure-release` (environment `azure-release-approval`, before `terraform-apply`, with a parallel `approve-azure-release-pat` for the PAT fallback path). A separate, standalone `LAB 08 Azure PROD Manual Run` workflow (also gated by `azure-release-approval`) was added to run the Azure PROD job on demand without setting `LAB08_RUN_AZURE_PROD_JOB`; its first use triggered job run `989280124372132` successfully, though the workflow's own post-trigger monitoring failed due to a workspace-host resolution defect unrelated to the job itself — see `evidence/lab08_azure_prod_run_989280124372132.md`.

**Further update (20 September 2026):** a manual `workflow_dispatch` trigger with an `azure_auth_mode` selector (`oidc` / `pat`) and a `run_azure_prod_job` checkbox was added on top of the two approval gates above, so either Azure path can be demonstrated on demand from `main` -- see "Manual Workflow Dispatch (Live Demonstration)" above. An early manual PAT attempt (run #42) failed at PAT preflight: the standalone `databricks auth describe` / `current-user me` / `jobs get` calls ran from a working directory containing `databricks.yml`, so the Databricks CLI's bundle-context auto-detection silently resolved authentication against the bundle's default (Personal DEV) target instead of the explicitly set `DATABRICKS_AZURE_HOST` / `DATABRICKS_AZURE_TOKEN`, and the PAT was never actually checked against Azure PROD. PR #20 fixed this by running that preflight from `${{ runner.temp }}` (outside the checked-out bundle directory) and by explicitly verifying the resolved host contains the Azure PROD workspace ID before checking the token or job permissions. With that fix live, both Azure paths have since completed real, independent manual end-to-end runs including the final Azure PROD job: OIDC in run #46, PAT in run #45. Neither run retroactively cleans up historical Bronze duplicates or independently re-queries the Azure Gold health row via SQL -- see Known Limitations below.

## Personal Ownership Model

Personal Terraform owns `dbr_dev.parvinbadalov.lab08_dev_travelops_raw` and `dbr_dev.parvinbadalov.lab08_prod_travelops_raw`. DAB owns the DEV and PROD pipelines, jobs, dashboard, alerts, notebooks, application configuration and Bronze/Silver/Gold datasets. Personal DEV publishes to `dbr_dev.parvinbadalov_lab08_dev`; Personal PROD publishes to `dbr_dev.parvinbadalov_lab08_prod` by re-adopting the historical PROD pipeline that already owned that namespace. Do not delete old schemas, volumes, tables, pipelines, or Lakeflow internals as part of CI/CD.

If a historical Personal PROD pipeline is pointed at a new raw source Volume, normal refresh can fail because retained Auto Loader source state still refers to the previous landing path. For this migration, a one-time supported Lakeflow full refresh was used only after confirming the new raw Volume was complete, batch-readable and replayable. CI/CD should continue to use normal bundle deploy/run after that migration step; it must not manually delete checkpoint files or old UC objects.

## Known Limitations

- **Historical Bronze duplicates.** Earlier raw-file overwrites produced duplicate Auto Loader ingestion before the write-once seed identity existed. That protection now prevents new duplicates on ordinary reruns, but it does not retroactively remove rows already ingested; the scoped remediation in `evidence/lab08_production_remediation_plan.md` remains written but unexecuted. No deletion or full refresh is implied by the green CI/CD results above.
- **No independent Azure SQL health audit.** The Azure `validate_gold_health` task has passed in every recorded successful job run, including runs #45 and #46, which means its own internal assertions completed. That is not the same as a separately captured, independent read-only SQL query of the exact Azure `gold_production_health` row values for those runs; none was taken.
- **Standalone monitoring workflow not reverified.** The separate `LAB 08 Azure PROD Manual Run` workflow (see "Current Azure Status" above) previously triggered a successful Databricks job but failed during its own post-trigger monitoring, due to the same class of host-resolution defect fixed by PR #20 in the integrated workflow. Runs #45 and #46 verify the integrated `lab08_cicd.yml` workflow's own PAT preflight; they do not verify that the separate standalone monitor's monitoring step was also fixed.

See `README.md` section 7 ("Known limitations") and `evidence/FINAL_RUNS_2026-09-20.md` for the illustrated, run-referenced version of these same boundaries.
