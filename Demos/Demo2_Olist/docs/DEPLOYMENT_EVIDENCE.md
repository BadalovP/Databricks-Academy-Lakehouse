# Demo2 Olist Deployment Evidence

This file records the observed results of the completed isolated development deployment and Job run.

## Scope

- Bundle: `demo2-olist-end-to-end`
- Target: `azure_dev`
- Bundle root: `Demos/Demo2_Olist`
- Workspace host: `https://adb-7405604503619901.1.azuredatabricks.net`
- Catalog/schema: `dbr_dev.parvinbadalov`
- Date: 2026-09-04

## Validation and safety gate

- `databricks bundle validate -t azure_dev`: passed before deployment and after the final correction.
- `databricks bundle summary -t azure_dev`: showed only `demo2_olist_end_to_end_workflow` and `demo2_olist_pipeline`; no unrelated resources were included.
- `databricks bundle deploy -t azure_dev`: completed successfully from `Demos/Demo2_Olist`.
- Deployment safety gate: passed; only the two intended Demo2 Olist Jobs were present and no destructive removals were scheduled.
- Confirmation: root bundle was not deployed.
- Confirmation: no production target was used.
- Confirmation: no resource removal was scheduled.

## Resources

- Preserved manual Job: `Demo2 Olist End-to-End Workflow_manual` (untouched).
- Preserved small bundle-managed Job: `demo2_olist_end_to_end_workflow`, Job ID `443959986174452` (intended to remain intact).
- Comprehensive Job resource: `demo2_olist_pipeline`, Job ID `135818015304158`.
- Comprehensive Job URL: https://adb-7405604503619901.1.azuredatabricks.net/jobs/135818015304158
- Existing Olist Test pipeline: `a54606ca-9067-44da-8ce9-e24c70f180f4`.
- Published dashboard: `01f1a7f2e75d17f8bbc359d20695d8e3`.
- SQL warehouse: `3ed106620db591d9`.

## Large Job run

- Job name: `[dev parvinbadov] [azure_dev] Demo2 Olist Lakehouse Pipeline`
- Job ID: `135818015304158`
- Job URL: https://adb-7405604503619901.1.azuredatabricks.net/jobs/135818015304158
- Run ID: `107916031830226`
- Run URL: https://adb-7405604503619901.1.azuredatabricks.net/?o=7405604503619901#job/135818015304158/run/107916031830226
- Overall lifecycle: `TERMINATED`
- Overall result: `SUCCESS`

This is the previously verified successful baseline run. The documentation-verification run below re-executed the same 26-task workflow after the documentation-only changes.

## Documentation verification

- Coverage checker: `tools/check_documentation_coverage.py`
- JSON notebooks: 11 notebooks, 40 documented code cells, 0 missing
- Databricks source-format notebooks: 16 notebooks, 120 documented executable cells, 0 missing
- Test files: 4 files, 5 documented test functions, 0 missing
- Overall documentation coverage: `100%`
- New verification deployment: completed successfully from `Demos/Demo2_Olist`.
- New verification run ID: `1841243362075`
- New verification run URL: https://adb-7405604503619901.1.azuredatabricks.net/?o=7405604503619901#job/135818015304158/run/1841243362075
- New verification start time: `2026-09-04 13:46:44` local workspace time

### Final verification task results

Run `1841243362075` reported `TERMINATED/SUCCESS` for every expected task:

`setup`, `source_exploration`, `validate_landing_files`, `bronze_reference_tables`, `bronze_autoloader`, `bronze_validation`, `silver_data_quality`, `silver_deduplication`, `silver_business_transformations`, `customer_scd2`, `gold_dimensions`, `gold_fact_order_items`, `gold_reconciliation`, `governance_rls_cls`, `dashboard_validation`, `alert_validation`, `dqx_quality_monitoring`, `automated_tests`, `parallel_learning_start`, `customer_check`, `order_check`, `parallel_learning_summary`, `run_olist_pipeline`, `validate_pipeline_outputs`, `refresh_olist_dashboard`, and `final_validation`.

The final verification output validator observed `order_item_rows=112650`, `distinct_orders=98666`, `total_price=13591643.70`, `total_freight=2251909.54`, `total_value=15843553.24`, `7` status rows, and `quality_status=PASS`.

### Task status

| Task | Final status | Run evidence |
| --- | --- | --- |
| `setup` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `source_exploration` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `validate_landing_files` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `bronze_reference_tables` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `bronze_autoloader` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `bronze_validation` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `silver_data_quality` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `silver_deduplication` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `silver_business_transformations` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `customer_scd2` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `gold_dimensions` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `gold_fact_order_items` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `gold_reconciliation` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `governance_rls_cls` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `dashboard_validation` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `alert_validation` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `dqx_quality_monitoring` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `automated_tests` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `parallel_learning_start` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `customer_check` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `order_check` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `parallel_learning_summary` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `run_olist_pipeline` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `validate_pipeline_outputs` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `refresh_olist_dashboard` | `TERMINATED/SUCCESS` | Run `1841243362075` |
| `final_validation` | `TERMINATED/SUCCESS` | Run `1841243362075` |

## Final metrics

| Metric | Expected | Observed |
| --- | ---: | ---: |
| `order_item_rows` | 112650 | 112650 |
| `distinct_orders` | 98666 | 98666 |
| `total_price` | 13591643.70 | 13591643.70 |
| `total_freight` | 2251909.54 | 2251909.54 |
| `total_value` | 15843553.24 | 15843553.24 |
| `quality_status` | PASS | PASS |
| status aggregation rows | 7 | 7 |

Dashboard refresh status: `TERMINATED/SUCCESS` in Run `1841243362075`.

The new verification run reported `TERMINATED SUCCESS` for every task listed above, with no skipped or failed tasks.

## Warnings

Two earlier development attempts exposed serverless incompatibilities and were not reported as successful: `customer_scd2` used unsupported DataFrame persistence, then DQX used unsupported DataFrame caching. Removing those unnecessary persistence calls preserved logic and enabled the successful run. Local Pytest execution was unavailable in the base interpreter because `pytest` was not installed; the Databricks `automated_tests` task succeeded in the final run. No root-bundle deployment occurred, no production target was used, and the manual and small managed Jobs were preserved.
