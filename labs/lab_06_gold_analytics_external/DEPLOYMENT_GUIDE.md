# Lab 06 External V2 — Deployment Guide

## Deployment Model

Lab 06 supports four Databricks Asset Bundle targets:

```text
azure_dev
azure_prod
personal_dev
personal_prod
```

Compute model:

```text
Azure targets    -> existing all-purpose cluster (GP1 / GP2 / auto)
Personal targets -> Serverless
```

Normal Lab 06 selective deployment uses:

```text
jobs.lab06_external_gold_job
dashboards.lab06_external_healthcare_dashboard
genie_spaces.lab06_external_healthcare_genie
alerts.lab06_external_healthcare_volume_drop
```

`08_governance` is part of the main `lab06_external_gold_job`; there is no separate governance Job.

---

## Prerequisites

Before deployment:

1. Configure a Databricks CLI profile for the target workspace.
2. Confirm that the required target exists in `databricks.yml`.
3. For Azure targets, confirm that the configured existing cluster is available.
4. Confirm access to the Lab 06 source Volume and external ADLS Gold root.
5. Run Lab 06 targets sequentially because they share the same external Gold storage.

---

## Validate a Target

```bash
databricks bundle validate \
  -t <bundle-target> \
  --profile <databricks-cli-profile>
```

For an Azure target with an explicit cluster override:

```bash
databricks bundle validate \
  -t <azure-target> \
  --profile <databricks-cli-profile> \
  --var lab06_cluster_id=<existing-cluster-id>
```

Expected result:

```text
Validation OK!
```

---

## Deploy and Run with the Repository Runner

### Personal targets

Personal targets use Serverless compute:

```bash
bash tools/run_academy_lab.sh \
  --lab 06 \
  --target <personal-target> \
  --profile <databricks-cli-profile>
```

### Azure targets

```bash
bash tools/run_academy_lab.sh \
  --lab 06 \
  --cluster <gp1|gp2|auto> \
  --target <azure-target> \
  --profile <databricks-cli-profile>
```

Cluster modes:

```text
gp1   -> configured GP1 existing cluster
gp2   -> configured GP2 existing cluster
auto  -> choose an available configured Azure cluster
```

### Deploy without executing the Job

Useful for production deployment when execution is intentionally deferred:

```bash
bash tools/run_academy_lab.sh \
  --lab 06 \
  --cluster <gp1|gp2|auto> \
  --target <azure-target> \
  --profile <databricks-cli-profile> \
  --skip-run
```

### Run an already deployed target without redeploying

```bash
bash tools/run_academy_lab.sh \
  --lab 06 \
  --target <bundle-target> \
  --profile <databricks-cli-profile> \
  --skip-deploy
```

---

## Direct Bundle Deployment

If the repository runner is not used, the Job can be deployed directly:

```bash
databricks bundle deploy \
  -t <bundle-target> \
  --profile <databricks-cli-profile> \
  --select jobs.lab06_external_gold_job
```

For Azure:

```bash
databricks bundle deploy \
  -t <azure-target> \
  --profile <databricks-cli-profile> \
  --var lab06_cluster_id=<existing-cluster-id> \
  --select jobs.lab06_external_gold_job
```

The repository runner intentionally deploys the selected Lab 06 Job resource. Dashboard, Genie, and alert resources can also be deployed with the full Lab 06 selector when required:

```text
jobs.lab06_external_gold_job,
dashboards.lab06_external_healthcare_dashboard,
genie_spaces.lab06_external_healthcare_genie,
alerts.lab06_external_healthcare_volume_drop
```

---

## Final Eight-Task Job

```text
01_dimensions
      |
      v
02_fact_encounters
      |
      v
03_fact_conditions
      |
      v
04_aggregations
   /             \
  v               v
05_register       06_alert_metrics
      |                 |
      v                 |
08_governance           |
      \                 /
       \               /
        v             v
          07_validation
```

Dependencies:

```text
01 -> 02 -> 03 -> 04
04 -> 05 -> 08 -> 07
04 -> 06 -> 07
```

`07_validation` waits for both `06_alert_metrics` and `08_governance`.

---

## Governance Execution Modes

The Job parameter is:

```text
run_demo
```

### Normal recurring execution

```text
run_demo = false
```

The governance task:

- ensures the helper access tables exist;
- initializes/maintains the current execution identity mappings safely;
- creates or replaces the secure encounter and patient views;
- leaves demonstration-only RLS/CLS restriction tests skipped;
- completes without changing the normal recurring pipeline behavior.

### Governance evidence execution

```text
run_demo = true
```

The governance notebook can temporarily exercise restricted and masked behavior, validate it, and then restore the invoking identity's full/privileged mappings.

The two secure views are:

```text
vw_fact_encounters_secure
vw_dim_patient_secure
```

The base external Gold tables remain policy-free so path-based writes remain compatible.

---

## Verified Final Deployment State

| Target | Compute | Deployment | Execution |
|---|---|---|---|
| `personal_dev` | Serverless | Complete | 8/8 tasks succeeded |
| `personal_prod` | Serverless | Complete | 8/8 tasks succeeded |
| `azure_dev` | Existing Azure cluster | Complete | 8/8 tasks succeeded |
| `azure_prod` | Existing Azure cluster | Complete | Intentionally not run |

This verifies the same source-controlled implementation on both Serverless and existing-cluster compute.

---

## Post-Deployment Checks

### 1. Job graph

Confirm that the Job contains all eight tasks and that `08_governance` appears between `05_register_shared_tables` and `07_validation`.

### 2. Job parameters

Confirm the expected parameters, including:

```text
catalog
source_schema
source_volume_name
target_schema
external_gold_root
date_start
date_end
rebuild_dim_date
run_validation
drop_threshold_pct
simulated_observed_pct
run_demo
```

### 3. Gold validation

Expected validated fact counts:

```text
fact_encounters     61,459
fact_conditions     38,094
```

### 4. Governance objects

Expected objects in the target schema:

```text
lab06_user_organization_access
lab06_patient_data_privileged_users
vw_fact_encounters_secure
vw_dim_patient_secure
```

### 5. Alert state

Expected controlled demonstration state:

```text
should_alert       = 1
alert_status       = TRIGGERED
volume_drop_pct    = 80.06
drop_threshold_pct = 30
```

Concrete SQL for manual verification:

```sql
SELECT
  should_alert,
  alert_status,
  data_source,
  test_month,
  baseline_encounter_count,
  observed_encounter_count,
  volume_drop_pct,
  drop_threshold_pct,
  generated_at
FROM dbr_dev.parvinbadalov_lab06_ext.lab06_data_volume_metrics
LIMIT 1;
```

Bundle variables such as `${var.lab06_external_catalog}` are resolved during bundle processing and should not be pasted directly into the Databricks SQL Editor.

---


### 6. Governance evidence run

For evidence, run the governance notebook with:

```text
catalog       = dbr_dev
target_schema = parvinbadalov_lab06_ext
run_demo      = true
```

Expected governance results:

```text
RLS restricted demo           PASS
RLS full-access restoration   PASS
CLS masked demo               PASS
CLS privileged restoration    PASS
```

During the CLS demonstration, `vw_dim_patient_secure` must show `***MASKED***` for the protected fields. After the notebook restores the current user to the privileged mapping, a later manual query of the secure view will show the original values; this is expected.


## Shared External Storage Rule

All four targets use the same External V2 ADLS Gold root.

```text
Do not run multiple Lab 06 targets concurrently.
```

Run them sequentially to avoid overlapping writes to the same external Delta locations.


## Final Test Verification

Before final submission, run:

```bash
python -m pytest labs/lab_06_gold_analytics_external/tests -v
```

Final verified result:

```text
46 passed
```

Governance-only verification:

```bash
python -m pytest   labs/lab_06_gold_analytics_external/tests/test_governance.py   -v
```

Final verified result:

```text
6 passed
```

The corresponding terminal evidence is stored in the README screenshots and can also be retained as `evidence/pytest_full_suite.txt` and `evidence/pytest_governance.txt`.
