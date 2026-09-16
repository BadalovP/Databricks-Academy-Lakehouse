# LAB 08 - TravelOps Architecture

## Data Architecture

```text
samples.wanderbricks
        |
        v
00_seed_raw_data
        |
        v
PERSONAL DEV: Terraform managed Volume dbr_dev.parvinbadalov.lab08_dev_travelops_raw
PERSONAL PROD: Terraform managed Volume dbr_dev.parvinbadalov.lab08_prod_travelops_raw
AZURE PROD: Terraform ADLS Gen2 -> Access Connector/RBAC -> Storage Credential -> External Location -> External Volume dbr_dev.parvinbadalov.lab08_prod_travelops_raw + application schema dbr_dev.parvinbadalov_lab08_prod
        |
        v
Lakeflow target schemas:
  personal_dev  -> dbr_dev.parvinbadalov_lab08_dev
  personal_prod -> dbr_dev.parvinbadalov_lab08_prod
  azure_prod    -> dbr_dev.parvinbadalov_lab08_prod
        |
        v
Bronze datasets
        |
        v
Silver datasets
        |
        v
Gold managed tables
   |              |
   v              v
Dashboard      SQL health alert
```

Auto Loader is used inside Lakeflow, but Auto Loader is not exclusive to Lakeflow. Classic Spark jobs can also use `spark.readStream.format("cloudFiles")`; the important distinction here is that Lakeflow owns declarative dataset lifecycle and orchestration.

## Compute Architecture

- **Personal DEV and Personal PROD**:
  - Promotion Job notebook tasks (`seed_raw_data`, `validate_gold_health`): serverless compute.
  - Lakeflow Pipeline (`travelops_pipeline`): serverless compute (`serverless: true`).
- **Azure PROD**:
  - `seed_raw_data` notebook task: runs on existing GP2 all-purpose compute (`0702-171207-xo9bbc0y`) via configurable bundle variable `azure_job_cluster_id`.
  - `validate_gold_health` notebook task: runs on existing GP2 all-purpose compute (`0702-171207-xo9bbc0y`) via configurable bundle variable `azure_job_cluster_id`.
  - `run_lakeflow_pipeline` pipeline task: executes the Lakeflow pipeline using **classic pipeline-managed compute** (`serverless: false`, single-label `default` cluster with `Standard_F4` nodes, `ON_DEMAND_AZURE`, and enhanced autoscaling `min_workers: 1`, `max_workers: 2`).
  - **Important**: GP2 is an all-purpose cluster used strictly for notebook tasks; GP2 is **not** the compute backing the Lakeflow pipeline.

## CI/CD Architecture

```text
Developer
  |
feature branch
  |
Pull Request
  |
Unit Tests / Ruff / Black
  |
Bundle Validate
  |
Terraform validate + read-only plan

Push main
  |
Unit Tests -> Bundle Validate
  |
Personal DEV deploy/run -> zero-change validation
  |
Personal PROD deploy/run -> zero-change validation
  |
LAB08_ENABLE_AZURE_OIDC=true:
  Terraform plan -> imported-state gate -> RBAC must be no-op
  -> fresh no-change plan/apply -> final no-change plan
  -> Azure PROD bundle deploy -> zero-change validation

LAB08_ENABLE_AZURE_OIDC=false:
  PAT validation -> delete-free plan -> Azure PROD deploy/run
  -> validation -> zero-change plan
```

## Design Notes

Raw landing data is file-based and target-specific so the pipeline reads a realistic ingestion surface instead of querying samples directly. Terraform reads the existing `dbr_dev.parvinbadalov` raw schema without owning it and owns the raw Volumes beneath it. Azure Terraform also owns `dbr_dev.parvinbadalov_lab08_prod` so the application namespace exists before a DAB job starts. Personal Lakeflow target schemas intentionally differ from the raw Volume schema: DEV publishes to `dbr_dev.parvinbadalov_lab08_dev`, while PROD publishes to `dbr_dev.parvinbadalov_lab08_prod`. Bronze preserves raw semantics, Silver applies quality and current-state logic, and Gold is optimized for operational reporting and CI/CD health checks.

Personal DEV and Personal PROD share one Personal workspace but publish to isolated dedicated schemas, so both environments use the same unprefixed dataset names without colliding. DEV receives a new DAB-owned pipeline because the dedicated DEV schema is effectively empty apart from an old raw Volume. PROD re-adopts historical pipeline `4d36399f-2f34-46f8-a2d1-b9340fd1556a` because it already targets `dbr_dev.parvinbadalov_lab08_prod` and owns the existing unprefixed PROD tables and Lakeflow internals. No historical pipeline, schema, Volume, table, or materialization object is deleted during this migration.

The Personal PROD raw source changed from the historical managed Volume `dbr_dev.parvinbadalov_lab08_prod.lab08_travelops_raw` to the Terraform-owned managed Volume `dbr_dev.parvinbadalov.lab08_prod_travelops_raw`. Detailed Lakeflow events showed retained Auto Loader offsets with `lastInputPath` in the historical Volume while the active `CloudFilesSource` used the new Volume. This is a historical stream-source change, not an explicit metadata-path problem: the pipeline code lets Lakeflow manage schema/checkpoint state and does not set `cloudFiles.schemaLocation` or `checkpointLocation`. A supported full refresh of the Personal PROD rehearsal pipeline reset streaming state and recomputed downstream Silver/Gold outputs from the complete replayable raw seed.

Azure PROD now has the complete Terraform storage chain, Terraform-owned application schema, and deployed DAB application definition. The first application run proved the seed notebook precondition by failing safely before downstream work when the target schema was absent. Terraform added only `dbr_dev.parvinbadalov_lab08_prod`; the rerun then completed seed, pipeline, and health tasks successfully. The external raw Volume remains at `abfss://lab08-travelops@lab08travelops63e621.dfs.core.windows.net/raw`, and DAB owns the 20 Bronze/Silver/Gold tables created inside the application schema.

PAT and OIDC deployments share the pinned Azure bundle root `/Workspace/Users/parvinbadalov@softserve.academy/.bundle/lab08-travelops-cicd/azure_prod`. The existing human identity remains the job and pipeline run-as identity and owner. The GitHub service principal has `CAN_MANAGE` on that root, job, and pipeline; dashboard and alert management inherit from the root, and the shared warehouse grants only `CAN_USE`. Azure target-level DAB permissions manage only the service-principal grant. The human `IS_OWNER` entry remains implicit so a non-admin deployment never attempts an ownership change.
