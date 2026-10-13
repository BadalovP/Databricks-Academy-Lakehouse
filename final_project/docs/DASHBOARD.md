# AI/BI dashboard

[← Main README](../README.md) · [Architecture](ARCHITECTURE.md)

**Status:** the dashboards are generated and tested offline. The `personal_dev` dashboard `01f1c647728e1ceeb2b5bd10a8700793` received the 12-column `GRID_V1` layout in step 4A. It has not been published.

**Live validation (2026-10-13):**
- The maintenance review was deployed (one update, post-plan all `skip`).
- **All 25 datasets of the deployed definition ran without error** in 40 seconds on the Starter warehouse, and every widget field matches its dataset.
- The expected values appear: 4 open tickets with a HOLD for MT-1004, checkpoint 4 / last data change 3 / 11 events, 22 Lakeflow expectations with 0 failures, and the live assistant evaluation ([evidence](../evidence/2026-10-13_personal_dev_dashboard_validation.md)).
- The structural rendering check found one defect. Lakeview concatenates a text widget's lines, so the page titles merged with their explanations. The generator now adds blank-line breaks, with a regression test. That fix is **not deployed yet**, and the dashboard is **not published**.

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

The **Maintenance & Assistant** page is where the capabilities meet. The "stations with an open maintenance ticket" table lists every `OPEN` or `IN_PROGRESS` ticket from the CDC-maintained Silver table, with:
- the station's latest availability;
- any pending Gold rebalancing action;
- a `HOLD` note when a van would otherwise be sent to a station under repair.

Open tickets without a pending action stay visible. The **Current Operations** priority list carries the same `maintenance_hold` column. Out-of-service stations never appear there, because Gold excludes them.

The CDC checkpoint table shows `processed_version` (the newest scanned and reconciled source version) next to `last_data_change_version` and the history event count. After step 4C these read 4, 3 and 11: the checkpoint passed the automatic `OPTIMIZE` version.

### Review changes (2026-10-13)

| Dataset | Before | After | Why |
|---|---|---|---|
| `maintenance_conflicts` | Inner join: only stations that had both an open ticket and a priority | Every open ticket, LEFT JOINed to the latest availability and priority, with `dispatch_note` | Open tickets at stations without a pending action were hidden |
| `ops_priorities` | Priorities only | Plus `maintenance_hold` | The operator's list must flag stations closed for repair |
| `maintenance_checkpoint` | `processed_version` only | Plus `last_data_change_version`, `history_events` | The checkpoint now passes non-data versions, so the two must be distinguishable |

`tests/test_dashboard_sql_analysis.py` runs these datasets on a small local example (closed ticket excluded, open ticket without action kept, HOLD when an action is pending, checkpoint 4 vs last data change 3).

### Empty and missing tables

| Dataset | Behaviour until the source has data |
|---|---|
| Maintenance, CDC | Live since step 4 (5 tickets, 11 events) |
| `quality_lakeflow` | Live since step 5 (update `a26b4385`) |
| `assistant_evaluation` | Live since E5: the latest evaluation, baseline vs RAG. Before E1, the table did not exist and the widget would have shown "table not found". The ingest creates it empty, and an empty table then renders an empty state. |

## Offline SQL verification

`tests/test_dashboard_sql_analysis.py` creates empty local Spark tables with the **live** column schemas of the 18 DEV tables, read from Unity Catalog (`tests/fixtures/dashboard_table_schemas.json`), plus the code-defined `rag_evaluation_results`. It then analyses and executes every dataset exactly as the generator renders it.
- 24 of the 25 datasets resolve every table, column and function.
- Every widget field exists in its dataset's output.
- `quality_lakeflow` is excluded, because `EVENT_LOG` and the `:` JSON path exist only on Databricks.

This proves the SQL matches the tables. It does not prove the values or the rendering; that needs the live query check (approval item D in [RELEASE_AND_VALIDATION.md](RELEASE_AND_VALIDATION.md)).

## Safety

- Every statement is a `SELECT`. `tests/test_sql_assets.py` fails on any write, DDL or `GRANT`.
- No dataset may name Demo 3's schemas or another target's schemas (`tests/test_dashboard_and_governance_demo.py`).
- The dashboard uses an existing SQL warehouse. In `personal_dev` it is looked up by name (`Serverless Starter Warehouse`); `azure_prod` takes `BUNDLE_VAR_warehouse_id`. The project creates none.
