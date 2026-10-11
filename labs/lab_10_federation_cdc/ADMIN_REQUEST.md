# Access request — Lab 10 foreign catalog in the Academy Azure workspace

**Requested by:** `parvinbadalov@softserve.academy`
**Workspace:** `https://adb-7405604503619901.1.azuredatabricks.net`
**Metastore:** `7af05576-c79e-4f56-b84f-ead80be5c8b6`
**Purpose:** Run Lab 10 (Lakehouse Federation & CDC) on serverless Jobs compute in an isolated schema.

## What already exists

| Object | State |
|---|---|
| Connection `parvinbadalov_lab10_neon_conn` | Exists. PostgreSQL, read-only, owned by me. Points to a Neon database used only for this lab |
| Schema `dbr_dev.parvinbadalov_lab10_prod` | Not yet created. I can create it myself (`account users` already holds privileges on `dbr_dev`) |

## The one thing I cannot do

Creating a foreign catalog requires `CREATE CATALOG` on the metastore. My only metastore privilege is `USE_MARKETPLACE_ASSETS`, so I am asking for **one** of the following. Option A is preferred because it grants nothing metastore-wide.

### Option A (preferred) — an administrator creates the catalog and transfers ownership

```sql
CREATE FOREIGN CATALOG parvinbadalov_lab10_neon_catalog
  USING CONNECTION parvinbadalov_lab10_neon_conn
  OPTIONS (database 'neondb')
  COMMENT 'Lab 10 Lakehouse Federation - read-only Neon PostgreSQL (parvinbadalov)';

ALTER CATALOG parvinbadalov_lab10_neon_catalog OWNER TO `parvinbadalov@softserve.academy`;
```

Ownership of this single catalog lets me grant read access to my deployment identity myself, without any further administrator action.

### Option B — grant me `CREATE CATALOG` on the metastore

```sql
GRANT CREATE CATALOG ON METASTORE TO `parvinbadalov@softserve.academy`;
```

This is broader: it allows creating any catalog, so it can be revoked once the Lab 10 catalog exists.

## What I will grant myself afterwards (no administrator action)

The Lab 10 Job is deployed by Terraform through GitHub OIDC and therefore runs as the existing Academy service principal **`github-lab08-travelops`** (the identity Labs 8 and 9 already use). As owner of the catalog, connection and schema, I will grant it only what the three notebooks need:

```sql
-- Read the federated Neon tables (read-only connection).
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG parvinbadalov_lab10_neon_catalog TO `<github-lab08-travelops application ID>`;

-- Read and write only the isolated Lab 10 schema.
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA dbr_dev.parvinbadalov_lab10_prod TO `<github-lab08-travelops application ID>`;
```

No shared schema, other student's data or existing table is affected.

## What this does NOT request

- No new cluster, cluster policy or cluster-create entitlement (the Job uses serverless Jobs compute).
- No change to GP1, GP2 or any shared compute.
- No storage account, external location or storage credential.
- No change to the existing connection, network settings or workspace configuration.
