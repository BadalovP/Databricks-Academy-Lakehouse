# LAB 08 Evidence

All retained command evidence is sanitized text encoded as UTF-8 without a BOM.
The logs intentionally omit repetitive polling, transient retries, credentials,
Terraform state, and saved plan files.

| Requirement | Environment | Outcome | Retained evidence and why it matters |
|---|---|---|---|
| Ruff | Local | Pass | `logs/final_ruff.txt` records the final lint gate. |
| Black | Local | Pass | `logs/final_black.txt` records the final formatting gate. |
| Pytest | Local | 9 passed | `logs/final_pytest.txt` records the final unit-test count. |
| Strict bundle validation | Personal DEV | Pass | `logs/personal_dev_bundle_validate.txt` proves strict DAB validation. |
| Final bundle plan | Personal DEV | No changes | `logs/personal_dev_bundle_plan.txt` proves zero add/change/delete. |
| Deployment and run | Personal DEV | Success | `logs/personal_dev_success_summary.json` records the bound pipeline, dedicated target schema, Terraform-owned raw Volume, and successful promotion run. |
| Strict bundle validation | Personal PROD | Pass | `logs/personal_prod_bundle_validate.txt` proves strict DAB validation. |
| Final bundle plan | Personal PROD | No changes | `logs/personal_prod_bundle_plan.txt` proves zero add/change/delete. |
| Deployment and run | Personal PROD | Success | `logs/personal_prod_success_summary.json` records the adopted pipeline, dedicated target schema, Terraform-owned raw Volume, completed full refresh, and successful promotion run. |
| Health gate | Personal PROD | 0 failed checks | `logs/personal_prod_output_health_verification.json` and `logs/personal_prod_output_health_verification.sql` show the final production-health result and its query. |
| Raw-source replayability | Personal PROD | Confirmed | `logs/batch_read_current_prod_raw_all_sources.json` and `logs/batch_read_historical_prod_raw_all_sources.json` show matching row counts for all seven inputs. |
| Full-refresh decision | Personal PROD | Safe and intentional | `logs/personal_prod_full_refresh_safety_decision.txt` records scope, replayability, and the decision not to delete checkpoints or UC objects. |
| Full-refresh execution | Personal PROD | Completed | `logs/personal_prod_full_refresh_success.txt` records the supported reset and terminal update state. |
| Target schema migration | Personal | Non-destructive | `logs/target_schema_migration_summary.json` and `logs/lab08_schema_visible_summary.json` record dedicated schemas, pipeline adoption, and preservation decisions. |
| UC quota root cause | Personal | Resolved architecturally | `logs/uc_quota_error_extract.txt` records the shared-schema table quota that motivated dedicated target schemas. |
| Historical source root cause | Personal PROD | Resolved by full refresh | `logs/location_overlap_prod_flow_path_extract.json` shows current and retained historical Auto Loader paths. |
| Terraform ownership | Personal | No changes | `logs/personal_terraform_plan.txt` proves both managed raw Volumes remain in state with an idempotent plan. |
| Terraform validation | Azure PROD | Pass | `logs/azure_prod_validate.txt` records the current configuration validation. |
| Imported RBAC state | Azure PROD | Verified | `logs/azure_prod_rbac_import_state.txt` records the imported role-assignment ID, principal, role, and storage-account scope. |
| Pre-apply Terraform plan | Azure PROD | 3 add, 0 change, 0 destroy | `logs/azure_prod_terraform_plan.txt` identifies only the external location, external Volume, and singular grant, with no replacement. |
| Terraform apply | Azure PROD | 3 added, 0 changed, 0 destroyed | `logs/azure_prod_terraform_apply.txt` records the safe fresh apply result. |
| Final Terraform plan | Azure PROD | No changes | `logs/azure_prod_terraform_final_plan.txt` records detailed exit code 0 and the final idempotent state. |
| UC storage verification | Azure PROD | Pass | `logs/azure_prod_uc_storage_verification.json` records the ADLS-backed external location, EXTERNAL Volume, and singular `READ_VOLUME`/`WRITE_VOLUME` grant. |
| Strict bundle validation | Azure PROD | Pass | `logs/azure_prod_bundle_validate.txt` records strict DAB validation against the Azure workspace. |
| Pre-deploy bundle plan | Azure PROD | 8 add, 0 change, 0 delete | `logs/azure_prod_bundle_plan_before_deploy.txt` records the four DAB-owned resources and their permissions with no deletion or replacement. |
| Bundle deployment | Azure PROD | Success | `logs/azure_prod_bundle_deploy.txt` records the deployment-only result. |
| First application run | Azure PROD | Failed safely | `logs/azure_prod_failed_run_missing_schema.json` records run `234618529385350`, the missing target-schema precondition, and skipped downstream tasks. |
| Application schema migration | Azure PROD | 1 added, 0 changed, 0 destroyed | `logs/azure_prod_schema_terraform_migration.txt` records the single-resource plan/apply and final no-change plan. |
| Application schema verification | Azure PROD | Terraform-managed | `logs/azure_prod_schema_verification.json` records `dbr_dev.parvinbadalov_lab08_prod` and its Terraform state address. |
| Authorized application rerun | Azure PROD | Success | `logs/azure_prod_successful_run.json` records run `829850820027662` and all three successful tasks. |
| Production outputs and health | Azure PROD | 20 tables; health passed | `logs/azure_prod_health_verification.json` records the Bronze/Silver/Gold inventory and the single green health row. |
| Post-run inventory | Azure PROD | Verified | `logs/azure_prod_bundle_inventory.json` records deployed resource IDs, runtime configuration, synchronized files, application schema, and both grading runs. |
| Final bundle plan | Azure PROD | 0 add, 0 change, 0 delete | `logs/azure_prod_bundle_final_plan.txt` records eight unchanged resources. |
| Remote state migration | Personal and Azure PROD | Complete | `logs/terraform_remote_state_migration_20260915.txt` records prechecks, backup hashes, preserved lineages, remote inventories, and final no-change plans. |
| OIDC identity readiness | GitHub/Azure | Databricks bootstrap complete | `logs/oidc_readiness_20260915.txt` records federation, Azure roles, registration, and the current disabled-mode safety posture. |
| OIDC Databricks permissions | Azure PROD | Minimum access verified | `logs/oidc_databricks_permissions_20260915.txt` records object IDs, before/after ACLs, inherited UC access, and the delete-free read-only bundle plan. |
| Workflow policy | GitHub Actions | Pass | `logs/workflow_validation.txt` records YAML parsing, dependency, PR safety, temporary PAT mode, advanced OIDC mode, and Azure-run gating checks. |
| Payment idempotency root cause | Azure PROD | Proven | `lab08_photon_fix_and_reconciliation_observation.md` records the job-run task attempts, Gold health snapshot, reconciliation split, and Bronze duplicate row counts behind the 100% payment-mismatch finding. |
| Bronze-to-Silver drop root cause | Azure PROD | Proven: negative amount only | Direct query of `payments_bronze` found exactly 2,723 rows with `amount < 0` and zero rows failing on null `payment_id` or invalid `status`, matching the 175,000 → 172,277 drop exactly. |
| Negative booking amount root cause | Azure PROD | Proven: booking_updates bypassed BOOKING_EXPECTATIONS | `bookings_silver` (quality-gated) had 0 negative rows; `booking_updates_bronze` (previously ungated) had 84, of which 4 were the "current" state for their booking_id. Statuses spanned pending/cancelled/completed/confirmed, not a clean refund pattern, so this was fixed as a consistency bug, not a business rule. |
| Referential sampling root cause | Azure PROD | Proven: partially sampling-induced | Of the 15,977 sampled bookings with no payment in Silver, 7,038 (44%) do have a payment record in the full unsampled `samples.wanderbricks.payments` source — proving the independent per-table row-order sampling in `00_seed_raw_data.ipynb` was a material cause, not solely genuine unpaid bookings. |
| Payment business key verification | Azure PROD | Proven: payment_id alone is unsafe | Of 20,795 distinct `payment_id` values in `payments_silver`, 3,470 map to more than one distinct `(payment_id, booking_id, amount, status, payment_date)` combination (up to 4), confirming `payment_id` alone would wrongly collapse distinct events. |
| Production remediation plan | Azure PROD | Not executed | `lab08_production_remediation_plan.md` documents whether a Bronze full refresh is necessary (not for correctness — Silver/Gold self-heal on the next normal run once this fix deploys), its scope, backup/rollback via Delta time travel, validation steps, and cost/downtime, without running any of it. |

Azure Terraform, DAB deployment, and the authorized Azure PROD application run
are complete and idempotent. The first run failed safely before downstream work
because the application schema was absent; Terraform now owns that schema, and
the successful rerun produced a green health result. Screenshots of GitHub
Actions, Lakeflow, dashboards, expectations, and Azure resources remain a manual
evidence step.

A subsequent run surfaced a Photon/`Standard_F4` incompatibility (fixed by PR #13)
and a payment-reconciliation defect investigated and fixed by
`fix/lab08-payment-idempotency-reconciliation`: duplicate raw ingestion via
Auto Loader re-processing overwritten seed files, an independently-sampled
payments table breaking referential consistency with sampled bookings, and
booking_updates bypassing booking quality expectations. See
`lab08_photon_fix_and_reconciliation_observation.md` and
`lab08_production_remediation_plan.md`.
