# Demo2 Olist Deployment Evidence

This file records observed results only. It must be updated after the isolated development bundle is actually deployed and the comprehensive Job completes.

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

### Task status

| Task | Final status | Run evidence |
| --- | --- | --- |
| `setup` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `source_exploration` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `validate_landing_files` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `bronze_reference_tables` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `bronze_autoloader` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `bronze_validation` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `silver_data_quality` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `silver_deduplication` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `silver_business_transformations` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `customer_scd2` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `gold_dimensions` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `gold_fact_order_items` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `gold_reconciliation` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `governance_rls_cls` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `dashboard_validation` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `alert_validation` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `dqx_quality_monitoring` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `automated_tests` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `parallel_learning_start` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `customer_check` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `order_check` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `parallel_learning_summary` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `run_olist_pipeline` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `validate_pipeline_outputs` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `refresh_olist_dashboard` | `TERMINATED/SUCCESS` | Run `107916031830226` |
| `final_validation` | `TERMINATED/SUCCESS` | Run `107916031830226` |

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

Dashboard refresh status: `TERMINATED/SUCCESS` in Run `107916031830226`.

## Warnings

Two earlier development attempts exposed serverless incompatibilities and were not reported as successful: `customer_scd2` used unsupported DataFrame persistence, then DQX used unsupported DataFrame caching. Removing those unnecessary persistence calls preserved logic and enabled the successful run. Local Pytest execution was unavailable in the base interpreter because `pytest` was not installed; the Databricks `automated_tests` task succeeded in the final run. No root-bundle deployment occurred, no production target was used, and the manual and small managed Jobs were preserved.
