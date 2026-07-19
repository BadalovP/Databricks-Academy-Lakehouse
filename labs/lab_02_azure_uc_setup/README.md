# Lab 2 — Azure UC Setup & Bronze Ingestion

This folder contains my Lab 2 work for the Databricks Academy Lakehouse project.

The lab is completed in two stages:

1. **Stage 1** — Provision core Azure services and review their configuration
2. **Stage 2** — Build a governed personal lakehouse area in the shared academy environment, ingest Bronze data, and deploy a Databricks Job with Asset Bundles

## Folder Structure

```text
lab_02_azure_uc_setup/
├── images/                                          # screenshots (add before submission)
│   ├── stage1-storage-account.png
│   ├── stage1-key-vault-overview.png
│   ├── stage1-key-vault-access-configuration.png
│   ├── stage1-databricks-workspace.png
│   ├── stage2-bundle-cli-success.png
│   └── stage2-bundle-deployed-job.png
├── notebooks/
│   ├── lab02_bronze_ingestion_auto_loader.ipynb   # Auto Loader ingestion
│   ├── lab02_bronze_ingestion_copy_into.ipynb     # COPY INTO ingestion
│   └── Drop before test.ipynb                     # utility notebook for resetting test tables
└── README.md
```

Related repository files:

```text
resources/lab02_bronze_ingestion_job.yml   # Databricks Job definition
databricks.yml                             # root Asset Bundle configuration
```

## Architecture Overview

```text
Stage 1 (personal Azure resources)
  stparvinlab02          → ADLS Gen2 storage
  kv-parvin-lab02        → Key Vault + personal secret scope
  dbr-parvin-lab02       → Databricks workspace

Stage 2 (shared academy environment)
  dlspl21databricks/parvinbadalov/
    ├── ingestion/       → source CSV files
    ├── bronze/          → Bronze managed storage
    ├── silver/          → Silver managed storage (scaffolded)
    ├── gold/            → Gold managed storage (scaffolded)
    └── checkpoints/     → Auto Loader checkpoints

  Unity Catalog (dbr_dev)
    ├── parvinbadalov_external_location
    ├── parvinbadalov_bronze  → raw_landing Volume, Bronze tables
    ├── parvinbadalov_silver  → scaffolded for future labs
    └── parvinbadalov_gold    → scaffolded for future labs

  Ingestion flow
    ADLS ingestion/ → UC Volume → Auto Loader / COPY INTO → Bronze Delta tables
```



# Stage 1 — Azure Services Provisioning

## Overview

Stage 1 focused on provisioning the core Azure services individually and reviewing the configuration parameters required for a Databricks lakehouse environment.

All resources were created in the same Azure subscription, resource group, and region.

| Setting | Value |
|---|---|
| Subscription | `subscr-ita-vkoldov` |
| Resource Group | `PL_24_Databricks` |
| Region | `East US` |

## 1. Storage Account — ADLS Gen2

**Resource name:** `stparvinlab02`

| Parameter | Value |
|---|---|
| Region | `East US` |
| Performance | `Standard` |
| Redundancy | `Locally-redundant storage (LRS)` |
| Account kind | `StorageV2` |
| Hierarchical namespace | `Enabled` |
| Default access tier | `Hot` |
| Public network access | `Enabled` |
| Public network access scope | `Enable from all networks` |

Hierarchical namespace is enabled, which provides ADLS Gen2 capabilities.

![ADLS Gen2 configuration](images/stage1-storage-account.png)

## 2. Azure Key Vault

**Resource name:** `kv-parvin-lab02`

| Parameter | Value |
|---|---|
| Region | `East US` |
| Pricing tier | `Standard` |
| Permission model | `Vault access policy` |
| Soft-delete | `Enabled` |
| Purge protection | `Enabled` |

The Key Vault was also used to create a personal Azure Key Vault-backed Databricks secret scope for learning purposes.

### Databricks secret scope

| Field | Value |
|---|---|
| Scope Name | `parvin-lab02-kv-scope` |
| Manage Principal | `Creator` |
| DNS Name | `https://kv-parvin-lab02.vault.azure.net/` |
| Resource ID | Azure Key Vault resource ID |

![Key Vault overview](images/stage1-key-vault-overview.png)

![Key Vault permission model](images/stage1-key-vault-access-configuration.png)

## 3. Azure Databricks Workspace

**Resource name:** `dbr-parvin-lab02`

| Parameter | Value |
|---|---|
| Region | `East US` |
| Pricing tier | `Premium` |
| Workspace type | `Hybrid` |
| Managed resource group | Automatically created by Azure Databricks |
| Secure Cluster Connectivity / No Public IP | `Enabled` |
| VNet injection | `Not configured` |

Azure automatically created a managed resource group for the infrastructure managed by the Databricks workspace.

![Databricks workspace configuration](images/stage1-databricks-workspace.png)

### Naming convention

| Resource | Name |
|---|---|
| Storage Account | `stparvinlab02` |
| Key Vault | `kv-parvin-lab02` |
| Databricks Workspace | `dbr-parvin-lab02` |

All three resources are deployed in `PL_24_Databricks` (`East US`). The Storage Account provides ADLS Gen2 storage, Key Vault provides secret management, and Azure Databricks provides the data processing workspace.

### Stage 1 outcome

An ADLS Gen2 storage account, Azure Key Vault, and Azure Databricks workspace were provisioned, and their important configuration parameters were reviewed and documented.

---

# Stage 2 — Governed Lakehouse Setup

## Overview

In Stage 2, the work moved from individually created learning resources to the shared academy environment.

The goal was to use shared Azure and Databricks resources while creating a personal governed lakehouse area with:

- a personal ADLS container
- a personal Unity Catalog external location
- personal Bronze, Silver, and Gold schemas
- a personal Azure Key Vault-backed secret scope
- idempotent Bronze ingestion with metadata columns
- a Databricks Job on the shared all-purpose cluster
- legacy Service Principal storage-access awareness
- a comparison of **Auto Loader** vs **`COPY INTO`**

> Silver and Gold schemas are created and storage paths are configured, but only Bronze ingestion is implemented in this lab. Silver and Gold are scaffolded for future labs.

## 4. Shared ADLS Container

A personal container was created in the shared ADLS Gen2 storage account.

| Setting | Value |
|---|---|
| Shared storage account | `dlspl21databricks` |
| Personal container | `parvinbadalov` |
| Root path | `abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/` |

All Stage 2 data is stored under this personal container.

## 5. Unity Catalog External Location

A personal Unity Catalog external location was created using the shared storage credential.

| Setting | Value |
|---|---|
| External location | `parvinbadalov_external_location` |
| Storage path | `abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/` |

The external location provides the Unity Catalog governance layer over the personal ADLS container.

The notebooks build ABFSS paths from the `storage_account` and `container` parameters. Those paths are covered by the external location even though the external location name itself is not referenced directly in the notebook code.

## 6. Medallion Schemas

Three personal schemas were created in the shared `dbr_dev` catalog.

| Layer | Schema | Status |
|---|---|---|
| Bronze | `dbr_dev.parvinbadalov_bronze` | Implemented |
| Silver | `dbr_dev.parvinbadalov_silver` | Scaffolded |
| Gold | `dbr_dev.parvinbadalov_gold` | Scaffolded |

Storage layout in the personal container:

```text
bronze/
silver/
gold/
```

Example:

```sql
CREATE SCHEMA IF NOT EXISTS dbr_dev.parvinbadalov_bronze
MANAGED LOCATION
'abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/bronze/';
```

## 7. Secret Scopes

### Personal scope (Stage 1 Key Vault)

| Setting | Value |
|---|---|
| Personal Key Vault | `kv-parvin-lab02` |
| Personal Databricks secret scope | `parvin-lab02-kv-scope` |

### Shared scope (academy environment)

| Setting | Value |
|---|---|
| Shared Databricks secret scope | `default2` |

The shared scope contains Service Principal credentials used for legacy storage-access testing:

```text
tenant-id
sp-databricks-adls-appid
sp-databricks-adls-appkey
```

Secret values are retrieved with `dbutils.secrets.get(...)` and are never printed.

## 8. Source Dataset

**Dataset:** [E-Commerce Online Sales Records](https://www.kaggle.com/datasets/kpatel123/ecommerce-online-sales-records)

The original CSV was split into **six monthly files** to simulate incremental file arrival and test idempotent ingestion:

```text
ecommerce_sales_2026-01.csv
ecommerce_sales_2026-02.csv
ecommerce_sales_2026-03.csv
ecommerce_sales_2026-04.csv
ecommerce_sales_2026-05.csv
ecommerce_sales_2026-06.csv
```

Files are uploaded to:

```text
abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/ingestion/
```

### Source schema

| Column | Data Type |
|---|---|
| `Order_ID` | STRING |
| `Product` | STRING |
| `Category` | STRING |
| `Quantity` | INT |
| `Price` | DOUBLE |
| `City` | STRING |
| `Date` | DATE |

The schema is explicitly defined in the ingestion logic instead of relying completely on automatic schema inference.

## 9. Unity Catalog Landing Volume

A Unity Catalog external Volume was created inside the Bronze schema to provide a governed landing path for the source CSV files.

| Setting | Value |
|---|---|
| Volume | `dbr_dev.parvinbadalov_bronze.raw_landing` |
| Cloud location | `abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/ingestion/` |
| Volume path | `/Volumes/dbr_dev/parvinbadalov_bronze/raw_landing/` |

```sql
CREATE EXTERNAL VOLUME IF NOT EXISTS dbr_dev.parvinbadalov_bronze.raw_landing
LOCATION 'abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/ingestion/';
```

The Volume does **not** copy or duplicate the CSV files. The cloud path and the `/Volumes/...` path refer to the same physical files.

Both ingestion notebooks read from:

```text
/Volumes/dbr_dev/parvinbadalov_bronze/raw_landing/
```

## 10. Bronze Metadata Columns

| Column | Purpose |
|---|---|
| `_source_file` | Source file path |
| `_ingested_at` | Ingestion timestamp |
| `_load_date` | Load date |

These columns improve traceability and auditing of Bronze data.

## 11. Auto Loader Ingestion

**Notebook:** `notebooks/lab02_bronze_ingestion_auto_loader.ipynb`

| Setting | Value |
|---|---|
| Target table | `dbr_dev.parvinbadalov_bronze.orders_autoloader` |
| Source Volume | `/Volumes/dbr_dev/parvinbadalov_bronze/raw_landing/` |
| Checkpoint | `abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/checkpoints/orders_autoloader/` |

The implementation uses Databricks Auto Loader:

```python
spark.readStream.format("cloudFiles")
```

with micro-batch processing:

```python
.trigger(availableNow=True)
```

`availableNow=True` processes all currently available unprocessed files and then stops the stream. The checkpoint stores ingestion progress and ensures already-processed files are not loaded again.

### Idempotency test

1. Load the initial files
2. Rerun the notebook with no new files → row count unchanged
3. Add another CSV file
4. Rerun the notebook → only the new file is processed

## 12. COPY INTO Ingestion

**Notebook:** `notebooks/lab02_bronze_ingestion_copy_into.ipynb`

| Setting | Value |
|---|---|
| Target table | `dbr_dev.parvinbadalov_bronze.orders_copy_into` |
| Source Volume | `/Volumes/dbr_dev/parvinbadalov_bronze/raw_landing/` |

`COPY INTO` was implemented as a batch comparison with Auto Loader.

Unlike Auto Loader, `COPY INTO` does not use a Structured Streaming checkpoint. It tracks files previously loaded into the target table and normally skips them on subsequent executions.

The same idempotency test was applied: rerunning without new files confirmed the row count did not increase.

## 13. Auto Loader vs COPY INTO

| Feature | Auto Loader | COPY INTO |
|---|---|---|
| Processing model | Structured Streaming | Batch |
| Source access | Unity Catalog Volume | Unity Catalog Volume |
| Incremental ingestion | Yes | Yes |
| File tracking | Checkpoint | COPY INTO load history |
| Continuous ingestion | Strong fit | Less suitable |
| Simple batch ingestion | Supported | Strong fit |
| Target table | `orders_autoloader` | `orders_copy_into` |

The two methods write to separate Bronze tables so their behavior can be compared independently.

## 14. Databricks Job

A Databricks Job orchestrates both ingestion notebooks on the shared all-purpose cluster `GP1`.

| Setting | Value |
|---|---|
| Display name | `lab02_bronze_ingestion_Auto_loader+Copy_into` |
| Bundle resource key | `lab02_bronze_ingestion_Auto_loader_Copy_into` |
| Cluster | `GP1` (shared all-purpose) |

### Task graph


The two tasks run **sequentially**. The `COPY INTO` task depends on the Auto Loader task through the `depends_on` configuration.

```text
bronze_ingestion_autoloader
            |
            v
bronze_ingestion_copy_into
```
This matches the `depends_on` configuration in `resources/lab02_bronze_ingestion_job.yml`.

### Task parameters

The Job passes selected widget parameters to each notebook:

| Parameter | Auto Loader task | COPY INTO task |
|---|---|---|
| `volume_name` | `raw_landing` | `raw_landing` |
| `target_table` | `orders_autoloader` | `orders_copy_into` |
| `storage_account` | from widget default | from widget default |
| `container` | from widget default | from widget default |
| `catalog` | from widget default | from widget default |
| `bronze_schema` | from widget default | from widget default |
| `run_validation` | from widget default | from widget default |

Other notebook widgets use their default values unless explicitly overridden by the Job.

## 15. File Arrival Trigger

A file-arrival trigger was configured for:

```text
abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/ingestion/
```

The trigger is currently **paused**.

During testing, enabling the trigger failed with Azure `403 AuthorizationFailure` while Databricks attempted to provision file-event resources. Missing Azure IAM permissions included:

- Storage Account Contributor
- Storage Blob Data Contributor
- EventGrid EventSubscription Contributor
- Storage Queue Data Contributor

The academy user does not have permission to assign these roles, so the trigger cannot be enabled without administrator support. Manual Job execution remains functional.

## 16. Legacy Service Principal Access

The lab required awareness of the legacy storage-access pattern based on a Service Principal and `dbutils.fs.mount()`.

Credentials are read from the shared secret scope `default2` using keys `tenant-id`, `sp-databricks-adls-appid`, and `sp-databricks-adls-appkey`.

The mount operation was attempted, but the academy shared-access cluster does not whitelist `DBUtilsCore.mount()`, so `dbutils.fs.mount()` could not be used.

As a supervisor-recommended alternative, OAuth configuration was applied through `spark.conf`, and direct storage access through the `abfss://` path was successfully validated.

## 17. Databricks Asset Bundle

The Job definition is stored as code in `resources/lab02_bronze_ingestion_job.yml` and included from the root `databricks.yml`:

```yaml
include:
  - resources/*.yml
```

### CLI workflow

```bash
databricks bundle validate --strict --target dev
databricks bundle deploy --target dev
databricks bundle run lab02_bronze_ingestion_Auto_loader_Copy_into --target dev
```

> **Note:** The `bundle run` command uses the YAML resource key with underscores (`lab02_bronze_ingestion_Auto_loader_Copy_into`). The deployed Job display name in the Databricks UI uses a `+` character (`lab02_bronze_ingestion_Auto_loader+Copy_into`). These refer to the same Job.

The bundle was successfully validated, deployed, and executed from the Databricks CLI.

![Bundle validation, deployment, and execution](images/stage2-bundle-cli-success.png)

In `dev` mode, the deployed Job is prefixed with the development target and user identity:

```text
[dev parvinbadalov] lab02_bronze_ingestion_Auto_loader+Copy_into
```

![Bundle-deployed Databricks Job](images/stage2-bundle-deployed-job.png)

The Job resource YAML contains:

- the paused file-arrival trigger
- the Auto Loader task with `depends_on` predecessor for COPY INTO
- shared all-purpose cluster configuration
- notebook task parameters for Volume and target-table values

---

## How to Run

### Prerequisites

- Access to the shared academy Databricks workspace and `GP1` cluster
- Personal container `parvinbadalov` in `dlspl21databricks`
- Unity Catalog objects created (external location, schemas, Volume)
- Source CSV files uploaded to the `ingestion/` folder
- Databricks CLI configured for bundle deployment

### Run notebooks manually

1. Attach to the shared `GP1` cluster
2. Open `notebooks/lab02_bronze_ingestion_auto_loader.ipynb` or `lab02_bronze_ingestion_copy_into.ipynb`
3. Review widget defaults (`storage_account`, `container`, `catalog`, `bronze_schema`, `volume_name`, `target_table`)
4. Run all cells
5. Use `Drop before test.ipynb` to reset Bronze tables before re-testing idempotency

### Run via Databricks Job (CLI)

From the repository root:

```bash
databricks bundle validate --strict --target dev
databricks bundle deploy --target dev
databricks bundle run lab02_bronze_ingestion_Auto_loader_Copy_into --target dev
```

### Expected output tables

```text
dbr_dev.parvinbadalov_bronze.orders_autoloader
dbr_dev.parvinbadalov_bronze.orders_copy_into
```

---

## Known Limitations

| Item | Status | Notes |
|---|---|---|
| File-arrival trigger | Paused | Blocked by missing Azure IAM permissions |
| `dbutils.fs.mount()` | Not available | Shared cluster policy blocks `DBUtilsCore.mount()` |
| Silver / Gold layers | Scaffolded only | Schemas exist; no tables implemented yet |
| Screenshots | In `images/` folder | Stage 1 and Stage 2 screenshots captured |

---

## Lab 2 Results

### Stage 1

- ADLS Gen2 storage account provisioned (`stparvinlab02`)
- Azure Key Vault provisioned (`kv-parvin-lab02`)
- Azure Databricks workspace provisioned (`dbr-parvin-lab02`)
- Configuration parameters reviewed and documented

### Stage 2

- Personal container created in shared ADLS
- Personal Unity Catalog external location created
- Bronze, Silver, and Gold schemas created
- External Unity Catalog landing Volume created in Bronze schema
- Auto Loader and `COPY INTO` configured to read through the Volume path
- Bronze data ingested as Delta with metadata columns
- Auto Loader and `COPY INTO` idempotency validated
- Databricks Job created on shared all-purpose cluster `GP1`
- Auto Loader and `COPY INTO` orchestrated as **sequential** Job tasks
- File-arrival trigger configured, paused, and documented
- Legacy Service Principal access tested through direct ABFSS access
- Databricks Asset Bundle validated, deployed, and executed via CLI

### Extensions beyond base requirements

- Unity Catalog external Volume for governed landing files
- Side-by-side comparison of Auto Loader vs `COPY INTO`
- Parameterized notebook execution via Job widgets
- Source-controlled Job configuration with Asset Bundles
