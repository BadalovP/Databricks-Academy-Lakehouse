# Administrator request — Lab 10 automated deployment identity

**To:** Łukasz / Academy Databricks account administrator
**From:** `parvinbadalov@softserve.academy`
**Databricks account:** `4b6644a4-eaf3-438d-bba3-6bd00ce5137e`
**Metastore:** `metastore_azure_eastus` (`7af05576-c79e-4f56-b84f-ead80be5c8b6`)
**Target workspace:** `dbr_dev_trial_2` (`7405618361895520`) — `https://adb-7405618361895520.0.azuredatabricks.net`

> [!NOTE]
> **Lab 10 is already deployed and validated in `dbr_dev_trial_2` without this request.** The Job was
> deployed with Terraform from my own Azure CLI identity and passed its end-to-end run
> ([evidence](evidence/2026-10-12-manual-azure-deployment.md)). This request is **only** for the
> automated GitHub Actions path, so future deployments run from `main` through an approved workflow
> instead of from my workstation. The foreign catalog request in the earlier version of this file is
> no longer needed: I created `parvinbadalov_lab10_neon_catalog` myself.

## The one request: a dedicated deployment identity and federation policy

Please create or confirm the Databricks account service principal **`github-databricks-academy-deployer`**, assign it only to workspace `7405618361895520` with `workspace-access`, and create one service-principal federation policy:

| Field | Exact value |
|---|---|
| Issuer | `https://token.actions.githubusercontent.com` |
| Audience | `4b6644a4-eaf3-438d-bba3-6bd00ce5137e` |
| Subject claim | `sub` |
| Subject | `repo:BadalovP/Databricks-Academy-Lakehouse:environment:lab10-azure` |

The repository was created on 2026-07-06, before GitHub's 2026-07-15 immutable-subject cutoff, so the name-based subject above is the expected format. The workflows use native Databricks workload identity federation (`DATABRICKS_AUTH_TYPE=github-oidc`), so this identity needs no Azure client secret, Databricks PAT, Azure Storage role or Azure subscription role. Creating federation policies requires a Databricks account administrator; my account-level read returns that the API is disabled for non-account-admin users.

## What I will do myself afterwards

Each step below is mine as the catalog and Job owner, done only after the service principal's application ID is supplied:

```sql
-- Read-only access to the isolated foreign catalog (bound read-only to dbr_dev_trial_2 only).
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG parvinbadalov_lab10_neon_catalog TO `<application-id>`;
```

1. Grant the service principal `CAN_MANAGE` on Job `582983448494007`, so Terraform can update the Job that HCP state `lab10-azure-prod` already holds.
2. Set GitHub variables: `DATABRICKS_CLIENT_ID` (environment `lab10-azure`, not secret) and repository variable `LAB10_AZURE_PROD_JOB_ID=582983448494007`; add the secret `HCP_TERRAFORM_TOKEN`.
3. Run the Azure PROD CD workflow with an empty confirmation (plan only) to prove the token exchange before any apply.

## What this does NOT request

- No metastore-wide privilege and no foreign catalog (both already handled).
- No cluster, cluster policy or cluster-create entitlement; GP1 and GP2 are not used.
- No storage account, external location or storage credential.
- No Azure client secret, Databricks PAT, or subscription-wide Azure RBAC role.
- No change to the existing Lab 08 managed identity, to `dbr_dev`, or to any other student's objects.
