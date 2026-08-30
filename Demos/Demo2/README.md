# Demo 2: RetailPulse E-Commerce Lakehouse

RetailPulse is a deterministic Azure Databricks reference implementation for an
e-commerce medallion lakehouse. It demonstrates incremental ingestion, controlled
schema evolution, explicit data-quality routing, SCD Type 2 customer history,
temporally correct facts, governed analytics, reconciliation, alerting, and an
AI/BI dashboard.

## Architecture

```text
Azure-backed Unity Catalog external volume
  -> Auto Loader Bronze
  -> classified Silver (VALID / WARN / QUARANTINE)
  -> trusted Silver + quarantine
  -> customer SCD2 + product/date dimensions + order-line fact
  -> business and DQ Gold aggregates
  -> fail-closed governed view
  -> AI/BI dashboard and SQL alert
```

The project reuses `dbr_dev.parvinbadalov` and the external volume
`dbr_dev.parvinbadalov.demo2_ecommerce`. Runtime input and Auto Loader metadata are
scoped below `/Volumes/dbr_dev/parvinbadalov/demo2_ecommerce/runtime`.

## Source Design

The generators in `src/demo2/data_generation.py` produce repeatable JSON/CSV inputs:

- V1 contains 24 valid order lines and omits `sales_channel` and `coupon_code`.
- V2 contains exactly 100 physical rows and introduces non-null values for both fields.
- Customer snapshots are dated `20260801` and `20260830`.
- `C001`, `C003`, and `C006` change between snapshots.

Bronze adds source metadata, rescued data, ingestion timestamps, and a canonical
SHA-256 row hash. Auto Loader evolves the V1 schema when V2 arrives.

## Data Quality

Silver applies deterministic duplicate ranking and classifies every physical row.
The controlled V2 result is:

| Status | Rows |
|---|---:|
| VALID | 92 |
| WARN | 2 |
| QUARANTINE | 6 |

WARN rows remain trusted. QUARANTINE rows are excluded from business facts and retain
rule reasons and quality dimensions. The six quarantined rows demonstrate uniqueness,
completeness, referential integrity, validity, and timeliness failures.

## SCD2 And Gold

`dim_customer_scd2` uses `dp.create_auto_cdc_from_snapshot_flow`. Only the five
business attributes are tracked; ingestion metadata is excluded. Changed customers
have two versions and every customer has exactly one current version.

`fact_order_lines` resolves the customer version valid on the order date, uses stable
customer/product/date surrogate keys, and contains no orphan or duplicate fact keys.
Gold also publishes daily sales, category, country, loyalty-tier, batch DQ, and
per-rule DQ aggregates.

## Governance

`demo2_sales_governed` is the primary analytical source. It is a fail-closed dynamic
view backed by `demo2_user_country_access`:

- No identity mapping means no rows.
- Country mappings restrict rows unless `all_access` is explicitly true.
- Customer name and email are masked unless `can_view_pii` is explicitly true.
- Temporary validation mappings are removed by an `ALL_DONE` cleanup task.

This dynamic-view implementation is the supported fallback used by this workspace.
Viewer-specific enforcement still requires an authenticated second identity; local
pytest cannot prove Unity Catalog identity behavior.

## Dashboard And Alert

The published AI/BI dashboard uses individual data permissions
(`embed_credentials=false`). Business visuals read `demo2_sales_governed`; quarantine
rate and batch metrics read `demo2_dq_summary_gold`; per-rule visuals read the derived
Gold rule aggregate. It includes six KPIs, seven business charts, five global filters,
and a dedicated DQ page.

The SQL alert reads the latest batch by `_batch_loaded_at DESC` with a deterministic
batch-id tiebreaker. It triggers when `quarantine_rate_pct > 5`; V2 evaluates to
`6% > 5%`. The deployed schedule is paused to avoid unattended recurring executions.

## Reconciliation

For V2, the physical-row identity and trusted-layer identity both hold:

```text
100 Bronze = 92 VALID + 2 WARN + 6 QUARANTINE
94 trusted Silver = 94 fact rows
duplicate trusted keys = 0
duplicate fact keys = 0
customer/product/date orphans = 0
```

## Validation

From the repository root:

```powershell
.\.venv-demo2\Scripts\python.exe -m pytest Demos/Demo2/tests -m "not spark" -q
$env:DEMO2_REMOTE_SPARK = "1"
$env:DATABRICKS_CONFIG_PROFILE = "AZURE_DEV"
$env:DEMO2_CLUSTER_ID = "0702-171207-xo9bbc0y"
.\.venv-demo2\Scripts\python.exe -m pytest Demos/Demo2/tests -m spark -q
.\.venv-demo2\Scripts\python.exe -m ruff check Demos/Demo2
.\.venv-demo2\Scripts\python.exe -m black --check Demos/Demo2
databricks bundle validate -t azure_dev --profile AZURE_DEV
```

Pure tests prove deterministic generation, hashes, classification, temporal lookup,
reconciliation, and governance predicates. Chispa runs through Databricks Connect in
this environment. Auto Loader evolution, Lakeflow execution, snapshot CDC, Unity
Catalog objects, dashboard publication, and workflow behavior are proven only by the
Azure run documented in `docs/evidence/AZURE_DEV_VALIDATION.md`.

## Deployment

The `azure_dev` bundle deploys one serverless Lakeflow pipeline, one multi-task job,
one paused SQL alert, and one AI/BI dashboard. Notebook tasks use the existing GP2
cluster; pipeline tasks use serverless compute. No `azure_prod` deployment is part of
this implementation.

Development remains on `feature/demo2-ecommerce`. Review and commit/push are manual
follow-up steps; this implementation does not alter Git history or publish code.

## Known Limitations

- Governance uses a dynamic-view fallback rather than native row filters and masks.
- A second authenticated user is required for viewer-specific RLS/CLS evidence.
- The alert is deployed paused; the workflow proves its condition deterministically.
- Dashboard filters apply to business datasets, not to independent DQ batch datasets.
