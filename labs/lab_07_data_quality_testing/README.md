# Lab 07 — Data Quality Testing on Databricks

End-to-end data quality engineering project built with Databricks Lakeflow, Unity Catalog, PySpark, Delta Lake, SCD Type 2, native Lakeflow `TestPipeline`, Data Quality Monitoring, Lakeview dashboards, and Databricks Asset Bundles.

This lab implements a production-style data quality gate for Chicago Business License data, covering ingestion, validation, quarantine, historical tracking, monitoring, reporting, automated testing, and Dev/Prod deployment.

---

## 1. Project Goal

Lab 07 implements a production-style data quality gate:

```text
City of Chicago API
        ↓
     Landing
        ↓
      Bronze
        ↓
Quality Classification
   ├── VALID / WARN ──→ Validated
   └── QUARANTINE ───→ Quarantine
                         ↓
                    SCD Type 2
                         ↓
                    Gold Metrics
                         ↓
        Monitoring + Lakeview Dashboard
                         ↓
                   Final Quality Gate
```

The workflow covers source preparation, expectations, quarantine rules, SCD2 history, Delta checks, reconciliation, freshness, schema contracts, monitoring, scorecard generation, and a final quality gate.

---

## 2. Data Source

Lab 07 uses the **City of Chicago Business Licenses** public dataset.

| Property | Value |
|---|---|
| Provider | City of Chicago Data Portal |
| Dataset | Business Licenses |
| Dataset ID | `r5kz-chrr` |
| API style | Socrata / SODA JSON |
| API endpoint | `https://data.cityofchicago.org/resource/r5kz-chrr.json` |
| Default source start date | `2024-01-01` |
| Maximum rows | `300000` |
| Page size | `50000` |
| Sort order | `date_issued ASC, license_id ASC, id ASC` |

The loader records lineage/provenance metadata on every row:

```text
_source_batch_id
_source_offset
_source_api
_source_dataset_id
_source_dataset_updated_at
_ingested_at
_fixture_kind
```

The loader can optionally use `CHICAGO_APP_TOKEN`.

---

## 3. Storage and Unity Catalog

```text
Catalog : dbr_dev
Schema  : parvinbadalov
Volume  : lab07_data_quality
```

Unity Catalog volume path:

```text
/Volumes/dbr_dev/parvinbadalov/lab07_data_quality
```

External ADLS Gen2 location:

```text
abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/lab07_data_quality
```

The source-preparation notebook verifies that this is an **EXTERNAL Volume** before loading data.

For this academy project, Azure Dev and Azure Prod intentionally reuse the same Lab 07 Unity Catalog volume. The existing volume is bound to the Azure Prod deployment state instead of being recreated.

---

## 4. Main Lakehouse Objects

| Object | Purpose |
|---|---|
| `business_license_landing` | Prepared source data plus deterministic test fixtures |
| `business_license_bronze` | Bronze ingestion layer with expectations |
| `business_license_classified` | Normalized records with DQ status/reasons/dimensions |
| `business_license_validated` | Trusted `VALID` and `WARN` records |
| `business_license_quarantine` | Critical quality failures |
| `business_license_snapshot_feed` | Versioned canonical snapshots |
| `dim_license_scd2` | SCD Type 2 license history |
| `license_quality_daily` | Daily quality totals and quality score |
| `license_quality_by_dimension` | Issues by quality dimension |
| `license_status_summary` | Trusted licenses by status |
| `license_volume_daily` | Trusted row-volume trend |

---

## 5. Controlled Data Quality Fixtures

The source-preparation step injects deterministic records so important rules can be tested repeatedly.

| Fixture | Expected outcome |
|---|---|
| Invalid license status | `QUARANTINE` / validity |
| Invalid ZIP | `QUARANTINE` / validity |
| Missing DBA name | `WARN` / completeness |
| Expiration before start date | `QUARANTINE` / consistency |

Artificial fixture rows are excluded from the canonical SCD2 snapshot source.

---

## 6. Quality Model

Every classified record receives:

```text
_dq_status
_dq_warn_reasons
_dq_quarantine_reasons
_dq_dimensions
```

### States

- **VALID** — no blocking issue
- **WARN** — trusted but has a non-blocking issue
- **QUARANTINE** — critical failure; excluded from trusted output

### Dimensions

- **Completeness** — missing ID, license number, legal name, DBA warning
- **Uniqueness** — duplicate source ID
- **Validity** — invalid application type, status, ZIP, latitude, longitude
- **Consistency** — expiration before start, missing required status-change date

---

## 7. Snapshot and SCD Type 2 Design

Configured snapshot cutoffs:

```text
2024-12-31
2025-12-31
2026-08-15
```

The snapshot feed is versioned and consumed by Lakeflow AUTO CDC FROM SNAPSHOT logic using `license_number` as the business key.

The SCD2 output includes Lakeflow system columns:

```text
__START_AT
__END_AT
```

The native SCD2 test proves that:
- a changing license creates multiple historical versions,
- an unchanged license remains one version,
- exactly one current record exists for each example license.

---

## 8. Lakeflow Pipeline

Pipeline source files:

```text
pipeline/bronze.py
pipeline/quality.py
pipeline/scd2.py
pipeline/gold.py
```

Important deployed settings:

```yaml
serverless: true
continuous: false
edition: ADVANCED
channel: CURRENT
```

The base resource file defines `PREVIEW`, while Azure Dev and Azure Prod explicitly override the deployed pipeline to `CURRENT`.

---

## 9. Native Lakeflow TestPipeline — 4/4 Passed

Native tests:

```text
src/tests/
├── test_expectations.py
├── test_quarantine.py
├── test_gold_quality_flow.py
└── test_scd2_snapshot_flow.py
```

### Expectations

Validates `VALID`, `WARN`, and trusted-only behavior.

![Lakeflow expectations test](evidence/screenshots/11_lakeflow_test_expectations_passed.png)

### Quarantine

Validates expected reasons for invalid status, invalid ZIP, and invalid dates.

![Lakeflow quarantine test](evidence/screenshots/12_lakeflow_test_quarantine_passed.png)

### Gold quality reconciliation

Validates trusted/quarantined/warning counts and quality-score reconciliation.

![Lakeflow Gold test](evidence/screenshots/13_lakeflow_test_gold_quality_passed.png)

### SCD2 snapshot history

Validates history preservation and current-row behavior.

![Lakeflow SCD2 test](evidence/screenshots/14_lakeflow_test_scd2_passed.png)

**Native Lakeflow result: `4/4 PASSED`.**

---

## 10. Local Tests

### Pytest

```bash
PYTHONPATH=labs/lab_07_data_quality_testing/src python -m pytest   labs/lab_07_data_quality_testing/tests   -m "not remote"   -v
```

Result:

```text
16 passed, 1 deselected
```

![Local pytest](evidence/screenshots/08_local_pytest_16_passed.png)

The deselected case is the remote/deployed integration test excluded by `-m "not remote"`.

### Chispa

```bash
PYTHONPATH=labs/lab_07_data_quality_testing/src python -m pytest   labs/lab_07_data_quality_testing/tests/unit/test_chispa_dataframes.py   -v
```

Result:

```text
3 passed
```

![Chispa](evidence/screenshots/09_chispa_3_passed.png)

---

## 11. Code Quality

Ruff:

```bash
python -m ruff check   labs/lab_07_data_quality_testing/src/lab07   labs/lab_07_data_quality_testing/tests   labs/lab_07_data_quality_testing/tools
```

Black:

```bash
python -m black --check   labs/lab_07_data_quality_testing/src/lab07   labs/lab_07_data_quality_testing/tests   labs/lab_07_data_quality_testing/tools
```

Evidence:

```text
Ruff  : All checks passed!
Black : 36 files would be left unchanged
```

![Ruff and Black](evidence/screenshots/10_ruff_black_passed.png)

---

## 12. Job Orchestration

```text
00_source_preparation
        ↓
01_quality_pipeline
        ↓
02_scd2_validation
        ↓
03_quarantine_review
        ↓
04_delta_constraints
        ↓
05_gx_validation
       ↙ ↘
06_reconciliation   07_freshness_volume
       ↘ ↙
08_schema_contract
        ↓
09_monitoring_validation
        ↓
10_quality_scorecard
        ↓
11_final_gate
```

### Azure Dev — full successful run

Every task from `00` through `11` completed successfully.

![Azure Dev timeline](evidence/screenshots/01_azure_dev_job_timeline_success.png)

![Azure Dev DAG](evidence/screenshots/02_azure_dev_job_dag_success.png)

---

## 13. Data Quality Monitoring

Monitored table:

```text
dbr_dev.parvinbadalov.business_license_validated
```

Configuration:

```text
Granularity : 1 day
Timestamp   : _ingested_at
```

Generated monitoring tables include:

```text
business_license_validated_profile_metrics
business_license_validated_drift_metrics
```

A successful monitor refresh generated profile metrics for the validated table.

![Data Quality Monitoring](evidence/screenshots/06_data_quality_monitoring.png)

---

## 14. Lakeview Quality Scorecard

Validated dashboard results:

| Metric | Result |
|---|---:|
| Overall Quality Score | **99.56%** |
| Total rows/checks | **84,281** |
| Trusted / passed | **83,910** |
| Quarantined / failed | **371** |
| Warning rows | **1** |

Quality issues:

| Dimension | Records | Status |
|---|---:|---|
| Consistency | 323 | QUARANTINE |
| Validity | 40 | QUARANTINE |
| Uniqueness | 8 | QUARANTINE |
| Completeness | 1 | WARN |

### Overview

![Dashboard overview](evidence/screenshots/03_dashboard_overview.png)

### Daily Trends

![Dashboard daily trends](evidence/screenshots/04_dashboard_daily_trends.png)

### Dimension Analysis

![Dashboard dimension analysis](evidence/screenshots/05_dashboard_dimension_analysis.png)

---

## 15. Databricks Asset Bundle Deployment

The repository uses a shared root Databricks Asset Bundle with multiple targets.

Relevant targets:

```text
personal
personal_dev
personal_prod
azure_dev
azure_prod
```

### Azure Dev

```text
validate → deploy → execute → validate
```

Result:

```text
Bundle validation          PASS
Deployment                 PASS
Full orchestration 00–11   PASS
Monitoring                 PASS
Dashboard                  PASS
```

### Azure Prod

```text
validate → plan → deploy only
```

The production job was intentionally **not executed**.

The existing external volume was bound to the Azure Prod deployment state:

```bash
databricks bundle deployment bind   lab07_data_quality   dbr_dev.parvinbadalov.lab07_data_quality   -t azure_prod   --profile dev
```

A repeated bind returned `Resource already managed`, confirming that the same requested volume was already managed by the Azure Prod bundle state.

After changing the Azure Prod Lakeflow runtime to `CURRENT`, the final pipeline plan was:

```text
Plan: 0 to add, 0 to change, 0 to delete, 2 unchanged
```

![Azure Prod clean plan](evidence/screenshots/07_azure_prod_plan_clean.png)

The final Azure Prod selective deployment completed successfully.

> The quality monitor was created/refreshed during Azure Dev validation against the shared monitored table. The selective Azure Prod promotion covered the Lab 07 volume, pipeline, dashboard, and job. Production execution was intentionally omitted.

---

## 16. Repository Structure

```text
labs/lab_07_data_quality_testing/
├── README.md
├── dashboards/
│   └── lab07_dq_scorecard.lvdash.json
├── notebooks/
│   ├── lab07_00_source_preparation.ipynb
│   ├── lab07_02_scd2_validation.ipynb
│   ├── lab07_03_quarantine_review.ipynb
│   ├── lab07_04_delta_constraints.ipynb
│   ├── lab07_05_gx_validation.ipynb
│   ├── lab07_06_reconciliation.ipynb
│   ├── lab07_07_freshness_volume.ipynb
│   ├── lab07_08_schema_contract.ipynb
│   ├── lab07_09_monitoring_validation.ipynb
│   ├── lab07_10_quality_scorecard.ipynb
│   └── lab07_11_final_gate.ipynb
├── pipeline/
│   ├── bronze.py
│   ├── quality.py
│   ├── scd2.py
│   └── gold.py
├── src/
│   ├── lab07/
│   └── tests/
│       ├── test_expectations.py
│       ├── test_quarantine.py
│       ├── test_gold_quality_flow.py
│       └── test_scd2_snapshot_flow.py
├── tests/
│   ├── fixtures/
│   ├── integration/
│   ├── quality/
│   └── unit/
├── tools/
│   └── license_batch_loader.py
└── evidence/
    └── screenshots/
```

Root resource definitions include:

```text
resources/lab07_infrastructure.yml
resources/lab07_quality_pipeline.yml
resources/lab07_monitoring.yml
resources/lab07_data_quality_job.yml
```

---

## 17. Evidence Index

| Screenshot | What it proves |
|---|---|
| `01_azure_dev_job_timeline_success.png` | All Azure Dev job tasks succeeded |
| `02_azure_dev_job_dag_success.png` | Full DAG and task dependencies |
| `03_dashboard_overview.png` | Final quality KPIs |
| `04_dashboard_daily_trends.png` | Quality/volume trend reporting |
| `05_dashboard_dimension_analysis.png` | Issue distribution by dimension |
| `06_data_quality_monitoring.png` | Monitoring profile exists |
| `07_azure_prod_plan_clean.png` | Final Azure Prod pipeline plan is clean |
| `08_local_pytest_16_passed.png` | Local pytest suite passed |
| `09_chispa_3_passed.png` | Chispa tests passed |
| `10_ruff_black_passed.png` | Code-quality checks passed |
| `11_lakeflow_test_expectations_passed.png` | Native expectations test passed |
| `12_lakeflow_test_quarantine_passed.png` | Native quarantine test passed |
| `13_lakeflow_test_gold_quality_passed.png` | Native Gold test passed |
| `14_lakeflow_test_scd2_passed.png` | Native SCD2 test passed |

---

## 18. Final Validation Status

**Lab 07 is complete for the agreed academy scope.**

> Azure Prod is a **deploy-only production test** for this lab. The production job was intentionally not executed.

| Capability | Result |
|---|---|
| Public API ingestion | ✅ |
| Provenance metadata | ✅ |
| External Unity Catalog Volume | ✅ |
| Bronze ingestion | ✅ |
| Data normalization | ✅ |
| VALID / WARN / QUARANTINE classification | ✅ |
| Quarantine reasons | ✅ |
| Quality dimensions | ✅ |
| Canonical snapshots | ✅ |
| SCD Type 2 | ✅ |
| Delta constraints | ✅ |
| GX validation step | ✅ |
| Reconciliation | ✅ |
| Freshness / volume validation | ✅ |
| Schema contract | ✅ |
| Native Lakeflow `TestPipeline` | ✅ `4/4 passed` |
| Local pytest | ✅ `16 passed, 1 deselected` |
| Chispa | ✅ `3 passed` |
| Ruff / Black | ✅ |
| Data Quality Monitoring | ✅ |
| Lakeview dashboard | ✅ |
| Azure Dev deployment | ✅ |
| Azure Dev full run | ✅ |
| Azure Prod validation | ✅ |
| Azure Prod deployment | ✅ |
| Azure Prod execution | ⏭️ intentionally not run |

---

## 19. Key Learning Outcomes

Lab 07 demonstrates:

- public API ingestion and provenance,
- external Unity Catalog volumes on ADLS Gen2,
- reusable PySpark quality rules,
- trusted vs quarantined data design,
- warnings vs blocking errors,
- deterministic test fixtures,
- quality dimensions and reason codes,
- snapshot correctness and late-arriving data handling,
- Lakeflow AUTO CDC / SCD Type 2,
- native Lakeflow `TestPipeline`,
- pytest and chispa testing,
- reconciliation and schema contracts,
- freshness and volume checks,
- Databricks Data Quality Monitoring,
- Lakeview dashboards,
- Databricks Asset Bundle validation/deployment,
- Azure Dev execution and deploy-only Azure Prod promotion.

---

## Conclusion

Lab 07 now has end-to-end proof across source ingestion, transformation, quality controls, historical tracking, tests, monitoring, reporting, and deployment.

```text
Local tests        ✅
Native Lakeflow    ✅
Pipeline           ✅
SCD2               ✅
Monitoring         ✅
Dashboard          ✅
Azure Dev          ✅
Azure Prod deploy  ✅
```

The only intentionally omitted activity is executing the Azure Prod job.
