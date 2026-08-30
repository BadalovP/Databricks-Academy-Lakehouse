# Demo 2 Azure Dev Validation Evidence

Captured after the final successful Azure Dev execution on **2026-08-30**, with
viewer-specific governance validation completed on **2026-08-31**.

All resource identifiers below belong to `azure_dev`. No `azure_prod` execution was
performed.

## Deployment

| Resource | Identifier | Status |
|---|---|---|
| External volume | `7b564391-e467-4eb0-ba2a-b33ae102ee52` | Active external Azure storage |
| Volume name | `dbr_dev.parvinbadalov.demo2_ecommerce` | Active |
| Lakeflow pipeline | `63c982e0-4c02-4b13-a949-3c6e227718c0` | COMPLETED |
| Validation job | `302596415744074` | Active |
| Final job run | `38050791281035` | SUCCESS |
| SQL alert | `4215558586839739` | Deployed, schedule PAUSED |
| AI/BI dashboard | `01f1a4a6833a1f10967638a44a6486de` | Active and published |
| Existing notebook compute | GP2 / `0702-171207-xo9bbc0y` | Used without touching GP1 |
| Existing SQL warehouse | `3ed106620db591d9` | Assigned to alert/dashboard |

Final selective Demo 2 bundle plan:

```text
0 to add, 0 to change, 0 to delete, 4 unchanged
```

## Workflow

All 11 tasks in run `38050791281035` completed successfully.

![Successful workflow timeline](screenshots/01_job_success_timeline.png)

![Successful workflow DAG](screenshots/02_job_success_dag.png)

```text
00_setup
01_generate_reference_and_v1
02_pipeline_initial
03_write_v2_schema_evolution_and_dq
04_pipeline_refresh
05_scd2_validation
06_governance_validation
06b_governance_cleanup
07_reconciliation
08_alert_validation
09_final_gate
```

The final gate found no missing or failed required checks.

## Schema Evolution and DQ

V1 Bronze did not contain `sales_channel` or `coupon_code`. After the controlled V2
batch, both fields were present and non-null for V2 rows.

| Batch | Total | VALID | WARN | QUARANTINE | Quarantine rate |
|---|---:|---:|---:|---:|---:|
| `DEMO2_V1_INITIAL` | 24 | 24 | 0 | 0 | 0% |
| `DEMO2_V2_SCHEMA_EVOLUTION` | 100 | 92 | 2 | 6 | 6% |

![Initial pipeline state](screenshots/03_pipeline_initial_success.png)

![V2 schema evolution evidence](screenshots/04_v2_schema_evolution.png)

![Successful pipeline refresh](screenshots/05_pipeline_refresh_success.png)

V2 quarantine reasons each affected one row:

```text
DUPLICATE_ORDER_LINE_ID
CUSTOMER_ID_MISSING
UNKNOWN_PRODUCT_ID
NON_POSITIVE_QUANTITY
INVALID_DISCOUNT
FUTURE_ORDER_TIMESTAMP
```

`HIGH_DISCOUNT` produced the two warning rows.

## SCD2 and Facts

- `C001`, `C003`, and `C006`: two versions each.
- Every customer: exactly one current version.
- Tracked columns: customer name, email, country, city, loyalty tier.
- Technical ingestion metadata: excluded from SCD2 history.
- Total trusted facts: 118.
- V2 trusted facts: 94.
- Null customer/product/date keys: 0.
- Duplicate fact keys: 0.

![SCD2 and temporal-fact validation](screenshots/06_scd2_validation.png)

## Governance

The primary serving object is the fail-closed dynamic view
`demo2_sales_governed`, backed by `demo2_user_country_access`.

Initial workflow validation proved:

- 118 base business rows,
- 118 visible rows for the explicitly mapped administrative session,
- no unmapped-user access rows,
- cleanup probe successfully removed.

![Governance validation](screenshots/07_governance_validation.png)

![Governance cleanup](screenshots/08_governance_cleanup.png)

### Viewer-Specific RLS / CLS Follow-Up

A second authenticated Databricks workspace identity was used to validate actual
viewer-specific behavior. The user's real email is intentionally omitted from this
evidence package.

| Test | Expected | Result |
|---|---|---|
| Authenticated user with no mapping | 0 rows | **PASS** |
| Restricted mapping country | `PL` only | **PASS** |
| `all_access = false` | no unrestricted row access | **PASS** |
| `can_view_pii = false` | customer name masked | **PASS** |
| `can_view_pii = false` | email masked | **PASS** |
| Distinct visible country count | 1 | **PASS** |

The restricted mapping was:

```text
country          = PL
all_access       = false
can_view_pii     = false
is_cleanup_probe = false
```

![Sanitized restricted-viewer mapping](screenshots/15_governance_mapping_sanitized.png)

The SQL validation showed that an unmapped authenticated user received zero rows.
After the PL mapping was inserted, the same user received only PL rows, while
`customer_name` and `email` were returned as `***MASKED***`.

This verifies fail-closed viewer-specific RLS/CLS semantics using `SESSION_USER()`.

## Reconciliation

```text
V2 Bronze physical rows: 100
V2 trusted rows:          94
V2 quarantine rows:        6
V2 fact rows:              94
Trusted duplicates:        0
Fact duplicates:           0
Customer orphans:          0
Product orphans:           0
Date orphans:              0
```

![Reconciliation result](screenshots/09_reconciliation.png)

## Alert Validation

The alert evaluates the latest logical batch by `_batch_loaded_at DESC` with a
deterministic batch ID tiebreaker.

```text
quarantine_rate_pct = 6.0
threshold            = 5.0
condition            = TRUE
```

![Alert validation result](screenshots/10_alert_validation.png)

The recurring schedule remains paused.

## Final Gate

The final quality gate passed after all required workflow validation results were
persisted.

![Final gate passed](screenshots/11_final_gate.png)

## Dashboard

The published dashboard reads business metrics from `demo2_sales_governed` and DQ
metrics from Gold DQ aggregates.

Validated business KPIs:

```text
Trusted lines:       118
Orders:               59
Customers:             8
Items sold:          236
Gross revenue:  16,360.83
Net revenue:    15,962.40
Average order value: 270.55
```

![Business overview](screenshots/12_dashboard_overview.png)

![Business breakdown](screenshots/13_dashboard_business_breakdown.png)

The DQ page shows the controlled V2 result:

```text
Physical rows:        100
Valid rows:            92
Warnings:               2
Quarantined rows:       6
Quarantine rate:        6%
```

![Data Quality dashboard](screenshots/14_dashboard_dq.png)

## Tests

```text
Pure pytest:        14 passed, 2 deselected
Remote Chispa:       2 passed, 14 deselected
Ruff:                All checks passed
Black:               32 files unchanged
pip check:           No broken requirements found
Bundle validation:   Validation OK
```

## Resolved Execution Issues

During implementation, early Demo 2 validation exposed three issues that were fixed
before the final clean run:

1. the SCD2 snapshot callback attempted premature access to a pipeline-managed table;
   it was changed to read deterministic snapshot inputs,
2. Unity Catalog rejected `input_file_name()`; Bronze was changed to use
   `_metadata.file_path`,
3. a Gold query had an ambiguous `customer_id`; fact/customer aliases were qualified.

The final workflow completed successfully without retries.

## Git / Promotion Evidence

Demo 2 was merged into `main` through GitHub PR #2 after repository checks passed.
The local repository and Azure Databricks Workspace Git folder were then synchronized
with `main`.

No Azure Prod execution was performed.
