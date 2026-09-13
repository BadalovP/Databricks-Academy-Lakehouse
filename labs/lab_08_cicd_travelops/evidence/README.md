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
| Terraform plan | Azure PROD | 4 add, 0 change, 0 destroy | `logs/azure_prod_plan.txt` identifies the exact remaining additions and shows no replacement. |
| Existing-resource safety | Azure PROD | Four resources already tracked | `logs/azure_prod_state_and_remote_inventory.txt` distinguishes existing state from genuinely pending resources. |
| Grant safety | Azure PROD | Singular least privilege | `logs/azure_prod_grant_safety.txt` confirms `databricks_grant` with only `READ_VOLUME` and `WRITE_VOLUME`. |
| RBAC prerequisite | Azure PROD | Blocked at last check | `logs/azure_prod_rbac_status.json` and `logs/azure_prod_role_assignment_check.txt` record effective permission and assignment visibility without applying infrastructure. |
| Workflow policy | GitHub Actions | Pass | `logs/workflow_validation.txt` records YAML parsing, dependency, PR safety, OIDC, and Azure-run gating checks. |

Azure Terraform apply, Azure DAB deployment, and the Azure application job have
not run. Screenshots of GitHub Actions, Lakeflow, dashboards, expectations, and
Azure resources must be captured manually only after the corresponding remote
operation succeeds.
