# AI/BI dashboard

[← Main README](../README.md) · [Architecture](ARCHITECTURE.md)

**Status:** the dashboards are generated and tested offline. The `personal_dev` dashboard `01f1c647728e1ceeb2b5bd10a8700793` was deployed by the bundle in step 1, with the earlier 6-column layout. The current generator (12-column `GRID_V1` layout, table v2, theme) has not been redeployed. **No dataset query has been run against the live tables and no rendering has been checked**, so the dashboard is not validated live.

## How it is built

Every dataset is one numbered, read-only statement in `sql/10`–`sql/14`. The SQL uses `${catalog}`, `${schema}` and `${lakeflow_schema}`. [`scripts/build_dashboard.py`](../scripts/build_dashboard.py) substitutes each bundle target's values from `databricks.yml` and writes one file per target:

| Target | File |
|---|---|
| `personal_dev` | [`dashboards/urbanflow_final.personal_dev.lvdash.json`](../dashboards/urbanflow_final.personal_dev.lvdash.json) |
| `azure_prod` | [`dashboards/urbanflow_final.azure_prod.lvdash.json`](../dashboards/urbanflow_final.azure_prod.lvdash.json) |

The bundle resource deploys `urbanflow_final.${bundle.target}.lvdash.json`, so each dashboard can only read its own target's tables. Historical and weather datasets are bound to one execution each, the one the platform Job writes with its default parameters (the labelled development samples), so samples and a future full month can never be added together.

```bash
python scripts/build_dashboard.py            # regenerate both files
python scripts/build_dashboard.py --check    # CI: fail if a file is stale
```

## Pages

| Page | Datasets | Honesty label on the page |
|---|---|---|
| Current Operations | Snapshot headline, availability breakdown, top 25 priorities, actions | **Point-in-time GBFS snapshot — not a trend**; nothing is charted over time |
| Historical Demand | Trips per day, by hour, durations, rideable type, top stations, weekday vs weekend | Real January 2024 trips; with Job defaults this is the **40-row development sample** (`data_scope` column) |
| Weather & Demand | Daily trips vs temperature, precipitation and wind; bands; coverage | One NYC coordinate, **not station-level weather**; **association, not causation** |
| Data Quality & Reliability | Station reconciliation, trip quarantine reasons, Lakeflow expectations, historical reconciliation | A low reference coverage is **not** a data-quality failure (the 40-station development dimension) |
| Maintenance & Assistant | Tickets by status, priorities at stations under maintenance, CDC audit trail and checkpoint, latest live assistant evaluation | Maintenance tickets are **fictional, synthetic data**; assistant scores exist only after a live evaluation |

The **Maintenance & Assistant** page is where the capabilities meet. The "priorities at stations under maintenance" table joins Gold rebalancing priorities with the CDC-maintained maintenance Silver table, listing the stations where a van should not be sent without a dispatcher's decision.

## Safety

- Every statement is a `SELECT`. `tests/test_sql_assets.py` fails on any write, DDL or `GRANT`.
- No dataset may name Demo 3's schemas or another target's schemas (`tests/test_dashboard_and_governance_demo.py`).
- The dashboard uses an existing SQL warehouse. In `personal_dev` it is looked up by name (`Serverless Starter Warehouse`); `azure_prod` takes `BUNDLE_VAR_warehouse_id`. The project creates none.
