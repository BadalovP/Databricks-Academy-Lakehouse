# Administrator request — Lab 10 Azure PROD prerequisites

**To:** Łukasz / Academy Databricks account and metastore administrator
**From:** `parvinbadalov@softserve.academy`
**Databricks account:** `4b6644a4-eaf3-438d-bba3-6bd00ce5137e`
**Metastore:** `metastore_azure_eastus` (`7af05576-c79e-4f56-b84f-ead80be5c8b6`)
**Target workspace:** `dbr_dev_trial_2` (`7405618361895520`) — `https://adb-7405618361895520.0.azuredatabricks.net`
**Purpose:** Lab 10 (Lakehouse Federation & CDC) as a PROD-style, Terraform-deployed serverless Job.

Lab 10 needs two narrowly scoped administrator actions. The checks below were repeated read-only on 2026-10-12; neither prerequisite currently exists.

## 1. Create one read-only foreign catalog

Please create **one foreign catalog** on my existing Neon connection and transfer its ownership to me. No metastore-wide grant to my user is needed.

```sql
-- 1. Create the catalog on the connection I already own (read-only Neon PostgreSQL).
CREATE FOREIGN CATALOG parvinbadalov_lab10_neon_catalog
  USING CONNECTION parvinbadalov_lab10_neon_conn
  OPTIONS (database 'neondb')
  COMMENT 'Lab 10 Lakehouse Federation - read-only Neon PostgreSQL (parvinbadalov)';

-- 2. Make me the owner, so I can manage grants on this one catalog myself.
ALTER CATALOG parvinbadalov_lab10_neon_catalog OWNER TO `parvinbadalov@softserve.academy`;
```

## Why I need an administrator

| Fact (checked read-only on 2026-10-12) | Result |
|---|---|
| Connection `parvinbadalov_lab10_neon_conn` | Exists, PostgreSQL, `read_only = true`, owner `parvinbadalov@softserve.academy`; visible in `dbr_dev`, `dbr_dev_trial` and `dbr_dev_trial_2` |
| My metastore privileges | Only `USE_MARKETPLACE_ASSETS` — no `CREATE CATALOG`, in any workspace (it is a metastore privilege, so being workspace admin in `dbr_dev_trial_2` does not help) |
| Foreign catalog for Lab 10 | Does not exist |

**Alternative, broader:** `GRANT CREATE CATALOG ON METASTORE TO \`parvinbadalov@softserve.academy\`;` — this allows creating any catalog, so I would prefer the single-catalog request above.

## What I will do myself afterwards (no administrator action)

After ownership is transferred, I will request approval before binding the catalog read-only to only `dbr_dev_trial_2`. The approved commands will add the binding before setting isolation, so the catalog is never left isolated without a binding:

```powershell
$env:DATABRICKS_HOST = "https://adb-7405618361895520.0.azuredatabricks.net"
$env:DATABRICKS_AUTH_TYPE = "azure-cli"
databricks workspace-bindings update-bindings catalog parvinbadalov_lab10_neon_catalog `
  --json '{"add": [{"workspace_id": 7405618361895520, "binding_type": "BINDING_TYPE_READ_ONLY"}]}'
databricks catalogs update parvinbadalov_lab10_neon_catalog --isolation-mode ISOLATED
```

Without this the catalog would be `OPEN`, like `dbr_dev`, and readable from every workspace on the metastore.

## 2. Create the dedicated Databricks deployment identity and federation policy

Please create or confirm the Databricks account service principal **`github-databricks-academy-deployer`**, assign it only to workspace `7405618361895520` with `workspace-access`, and create one service-principal federation policy:

| Field | Exact value |
|---|---|
| Issuer | `https://token.actions.githubusercontent.com` |
| Audience | `4b6644a4-eaf3-438d-bba3-6bd00ce5137e` |
| Subject claim | `sub` |
| Subject | `repo:BadalovP/Databricks-Academy-Lakehouse:environment:lab10-azure` |

The repository was created on 2026-07-06, before GitHub's 2026-07-15 immutable-subject cutoff, so the name-based subject above is the expected format. The first plan-only workflow run will verify the actual token exchange before any apply is allowed. My account-level read returned that this API is disabled for non-account-admin users, and neither this service principal nor `github-lab08-travelops` is currently assigned to `dbr_dev_trial_2`.

The workflow uses native Databricks workload identity federation (`DATABRICKS_AUTH_TYPE=github-oidc`), so this identity needs no Azure client secret, Databricks PAT, Azure Storage role, or Azure subscription role.

## What I will grant after both requests are complete

After the service principal application ID is supplied, I will request approval before granting the deployment identity read-only access to the foreign catalog:

```sql
-- <application-id> is the application ID of github-databricks-academy-deployer.
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG parvinbadalov_lab10_neon_catalog TO `<application-id>`;
```

| Privilege the Job needs | Status |
|---|---|
| `USE CATALOG`, `USE SCHEMA`, `SELECT` on `parvinbadalov_lab10_neon_catalog` | **Missing** — granted by me after this request |
| Read/write `dbr_dev.parvinbadalov_lab10_prod` | Broad access is currently inherited from the Academy `account users` grant on `dbr_dev`; the dedicated principal's effective access must be verified after workspace assignment |
| `USE CONNECTION` | **Not needed**: queries go through the foreign catalog |
| Cluster creation | **Not needed**: the Job uses serverless Jobs compute |

## What this does NOT request

- No metastore-wide privilege for my user.
- No cluster, cluster policy or cluster-create entitlement; GP1 and GP2 are not used.
- No storage account, external location or storage credential.
- No Azure client secret, Databricks PAT, or subscription-wide Azure RBAC role.
- No change to the existing Lab 08 managed identity or its GitHub variables.
- No change to the connection, to `dbr_dev`, or to any other student's objects.
