# Demo 2 Azure Dev Validation Evidence

Captured on 2026-08-30 after the final successful execution. All identifiers below
belong to `azure_dev`; no `azure_prod` action was performed.

## Deployment

| Resource | Identifier | Status |
|---|---|---|
| External volume | `7b564391-e467-4eb0-ba2a-b33ae102ee52` | Reused external Azure storage location |
| Volume name | `dbr_dev.parvinbadalov.demo2_ecommerce` | Active |
| Lakeflow pipeline | `63c982e0-4c02-4b13-a949-3c6e227718c0` | Successful |
| Validation job | `302596415744074` | Active |
| Final job run | `38050791281035` | SUCCESS |
| SQL alert | `4215558586839739` | Deployed, schedule PAUSED |
| AI/BI dashboard | `01f1a4a6833a1f10967638a44a6486de` | Active and published |
| Existing notebook compute | GP2 / `0702-171207-xo9bbc0y` | Used without touching GP1 |
| Existing SQL warehouse | `3ed106620db591d9` | Assigned to alert/dashboard |

Final selective bundle plan: `0 to add, 0 to change, 0 to delete, 4 unchanged`.

## Workflow

All tasks in run `38050791281035` completed successfully:

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

## Schema Evolution And DQ

V1 Bronze did not contain `sales_channel` or `coupon_code`. After V2, both columns
were present and non-null for all 100 V2 physical rows.

| Batch | Total | VALID | WARN | QUARANTINE | Rate |
|---|---:|---:|---:|---:|---:|
| `DEMO2_V1_INITIAL` | 24 | 24 | 0 | 0 | 0% |
| `DEMO2_V2_SCHEMA_EVOLUTION` | 100 | 92 | 2 | 6 | 6% |

V2 quarantine reasons each affected one row:

```text
DUPLICATE_ORDER_LINE_ID
CUSTOMER_ID_MISSING
UNKNOWN_PRODUCT_ID
NON_POSITIVE_QUANTITY
INVALID_DISCOUNT
FUTURE_ORDER_TIMESTAMP
```

`HIGH_DISCOUNT` produced the two WARN rows.

## SCD2 And Facts

- `C001`, `C003`, and `C006`: 2 versions, exactly 1 current version each.
- `C002`, `C004`, `C005`, `C007`, and `C008`: 1 current version each.
- SCD2 tracked columns: customer name, email, country, city, loyalty tier.
- Technical ingestion columns were absent from the SCD2 target.
- Total trusted facts: 118 (24 V1 + 94 V2).
- Null customer/product/date keys: 0.
- Duplicate fact keys: 0.

## Reconciliation

```text
V2 Bronze physical rows: 100
V2 trusted rows:         94
V2 quarantine rows:       6
V2 fact rows:             94
Trusted duplicates:       0
Fact duplicates:          0
Customer orphans:         0
Product orphans:          0
Date orphans:             0
```

## Governance

The dynamic-view fallback passed with 118 base rows and 118 visible rows for the
explicitly mapped session user. There were no unmapped-user access rows. The temporary
cleanup probe was removed (`remaining_probes = 0`).

The dashboard is published with `embed_credentials=false`, so it is configured for
individual data permissions. A second authenticated identity is still required to
capture viewer-specific row/mask behavior.

## Alert And Dashboard

The alert source is `quarantine_rate_pct`, operator is `GREATER_THAN`, and threshold is
`5`. The final logical batch returned `6`, so the deterministic condition is true.
The recurring schedule remains paused.

The dashboard is active and published. Its four datasets query:

- `demo2_sales_governed`
- `demo2_dq_summary_gold` (full trend and latest batch)
- `demo2_dq_failures_by_rule_gold`

The checked-in dashboard JSON matches the normalized Azure draft exactly after export.
Live business KPI query results were 118 trusted lines, 59 orders, 8 customers, 236
items, gross revenue 16,360.83, net revenue 15,962.40, and average order value 270.55.

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

During implementation, two early job runs were canceled after their pipeline task
entered service retry loops; only the Demo 2 job/pipeline was stopped:

- `896745229978036`: the snapshot callback depended on a pipeline-managed table before
  graph materialization; it was changed to read the deterministic snapshot files.
- `1014376273684324`: Unity Catalog rejected `input_file_name()`; Bronze now uses
  `_metadata.file_path`.

Validate-only update `8de30732-72fa-470b-acde-d7b6e09157bc` then exposed an ambiguous
`customer_id` reference in a Gold aggregate. Qualifying the fact/customer aliases
resolved it. Later validate-only updates completed successfully, and the final clean
workflow passed without retries.

No commit, push, pull, merge, force operation, or Git history rewrite was performed.
