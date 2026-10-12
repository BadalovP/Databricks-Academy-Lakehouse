# Step 5: Lakeflow Declarative Pipeline, one triggered update (2026-10-12): PASS

**Outcome:** one Lakeflow-only run of the platform Job started exactly one triggered serverless pipeline update, and the update completed.

- All ten datasets have the expected counts.
- All 22 expectations recorded zero failed records.
- The event log holds no error or warning.
- Lakeflow Silver, fact, dimension, shortage and priority match the imperative tables row for row, in both directions.

The pipeline is idle again, and no table in the main schema changed.

All data is the bounded 40-row **development sample**. It proves that the declarative and imperative paths agree, not that they scale.

## Pipeline (read from the workspace before and after)

| Property | Value |
|---|---|
| Pipeline | `d5e0e599-c195-4c08-8217-d554ebf2b301`, `[personal_dev] UrbanFlow 2.0 Medallion Pipeline` |
| Compute | Serverless, no cluster definition, no schedule or trigger |
| Mode | Triggered (`continuous: false`), development mode, channel `CURRENT`, edition `ADVANCED` |
| Target | `workspace.urbanflow_final_lakeflow_dev` (separate from the imperative schema) |
| Sources (read only) | `workspace.urbanflow_final_dev.bronze_station_status` (append-only: versions 0 `CREATE TABLE`, 1 `STREAMING UPDATE`) and `workspace.urbanflow_final_dev.dim_station_development_sample` |
| Code | `pipeline/bronze.py` (streaming table), `silver.py` and `gold.py` (materialized views). Business rules imported from `src/urbanflow/silver.py` and `gold.py`, the same functions the notebooks use. |

## Run

| Item | Value |
|---|---|
| Job / run | `158911626270744` / `441981394008476`, token `urbanflow-final-step5-lakeflow-20261013-001` |
| Window | 2026-10-12 22:51:41 → 22:56:27 UTC (286 s), `SUCCESS`, no retry |
| Parameters | `run_lakeflow=true`; station, historical, weather, CDC and full month `false` |
| `preflight` | PASS with only `run_lakeflow` enabled (a Lakeflow-only run was refused before commit `e4b256d`) |
| Other tasks | `NOTHING_TO_LAND`, `SKIPPED` or `DRY_RUN`; `validate_medallion` PASS with no checks; `federation_cdc` `SKIPPED` |
| `refresh_lakeflow` | SUCCESS, 142 s (task timeout 2,400 s, `full_refresh: false`) |

## Update

| Item | Value |
|---|---|
| Update ID | `a26b4385-f0b8-454a-9aad-d9a3424151c7` (the pipeline's first update) |
| Progress | `WAITING_FOR_RESOURCES` 22:54:07 → `INITIALIZING` 22:54:41 → `SETTING_UP_TABLES` 22:55:23 → `RUNNING` 22:55:36 → `COMPLETED` 22:56:16 UTC |
| Duration | 129 s from request to completion, of which 40 s computing flows |
| Errors / warnings in the event log | none |
| After the run | Pipeline `IDLE`; no update running |

## Datasets: Lakeflow vs imperative

| Dataset | Lakeflow | Imperative (main schema) | Rows only in one side (both directions) |
|---|---|---|---|
| `bronze_station_status` | 40 | 40 | — |
| `silver_station_status` | 40 | 40 | 0 / 0 |
| `quarantine_station_status` | 0 | 0 | — |
| `duplicate_station_status` | 0 | 0 | — |
| `station_information_raw` | 40 | (reference, main-schema dimension) | — |
| `dim_station_development_sample` | 40 | 40 | 0 / 0 |
| `fact_station_availability` | 40 | 40 | 0 / 0 |
| `gold_daily_station_summary` | 40 | 40 | — |
| `gold_station_shortage` | 13 | 13 | 0 / 0 |
| `gold_rebalancing_priority` | 13 | 13 | 0 / 0 (incl. `priority_score`, `action`) |

The comparisons use `EXCEPT ALL` over the business columns:
- Silver: event, station, bikes, docks, status and both low flags;
- fact: event, station, status;
- shortage: station, status;
- priority: station, status, score, action;
- dimension: station, short name, capacity.

## Expectations (event log, this update)

| Dataset | Expectations | Passed | Failed |
|---|---|---|---|
| `silver_station_status` | `event_id_present`, `station_id_present`, `observed_at_present`, `bikes_not_negative`, `docks_not_negative`, `status_is_known`, `out_of_service_is_not_actionable` | 40 each | 0 |
| `quarantine_station_status` | `has_failure_reason` | 0 (no rows) | 0 |
| `duplicate_station_status` | `duplicate_has_event_id` | 0 (no rows) | 0 |
| `dim_station_development_sample` | `station_id_present`, `short_name_present` | 40 each | 0 |
| `fact_station_availability` | `event_id_present`, `observation_date_present` | 40 each | 0 |
| `gold_daily_station_summary` | `counts_are_positive`, `trend_claim_matches_observations` | 40 each | 0 |
| `gold_station_shortage` | `shortage_is_actionable`, `shortage_status_is_known`, `execution_is_attributed` | 13 each | 0 |
| `gold_rebalancing_priority` | `priority_is_actionable`, `score_is_explainable`, `action_is_known`, `execution_is_attributed` | 13 each | 0 |

The expectations are warnings by design: dropping rows would break Bronze = Silver + Quarantine + Duplicates. The Bronze streaming flow reports no `num_output_rows` metric in the event log; its 40 rows were counted with SQL.

## Side effects

| Check | Result |
|---|---|
| The 20 main-schema Final Project tables | No new commit (compared against a pre-run audit) |
| Lakeflow schema | The 10 pipeline datasets, created by this update |
| Cost | Not read from billing. Serverless pipeline compute ran for about 2 minutes; see `docs/COST_AND_PERFORMANCE.md`. |

Machine-readable record: [`2026-10-12_personal_dev_step5_lakeflow.json`](2026-10-12_personal_dev_step5_lakeflow.json).
