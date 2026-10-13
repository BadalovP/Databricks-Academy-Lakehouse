# Phase D: dashboard update, live query validation and rendering check (Personal DEV, 2026-10-13): PASS with one rendering defect

**Outcome:**
- The reviewed dashboard update was deployed (exactly one update) and the post-deploy plan is all `skip`.
- **All 25 datasets of the deployed dashboard ran without error** on the existing Starter warehouse in 40 seconds, and every widget field matches its dataset's columns.
- The maintenance, CDC, Lakeflow and assistant figures equal the independently verified state.

Reading the deployed definition back revealed a **rendering defect**: Lakeview concatenates a text widget's lines without separators, so each page title renders its heading and its explanation as one line. It is fixed in code with a regression test, but **not deployed**, because one deployment was approved. The dashboard was **not published**.

## Deployment (dashboard only)

| Check | Result |
|---|---|
| Commit | `d93f2f4` (working tree clean) |
| `build_dashboard.py --check`, `bundle validate -t personal_dev` | Pass |
| Pre-deploy plan | `urbanflow_final_dashboard`: **update** (`serialized_dashboard` only). The other 6 resources `skip`. 0 creates, 0 deletes. Validator PASS; no active runs |
| Deploy | `bundle deploy --fail-on-active-runs`: complete (dashboard `update_time` 00:40:35 UTC) |
| Post-deploy plan | All 7 `skip`; validator PASS |
| Deployed datasets | Byte-identical to the generated `urbanflow_final.personal_dev.lvdash.json` |

## Live query validation (deployed definition, `SELECT * FROM (<dataset>) LIMIT 1000`)

Warehouse `410e5f3b4784e981` (Serverless Starter, 2X-Small; no resize, no new warehouse). Window 00:40:57 → 00:41:41 UTC (40 s of queries). **25 / 25 succeeded. 0 widget-field mismatches.**

| Dataset | Rows | Check |
|---|---|---|
| `ops_headline` | 1 | 40 stations, 25 available, 2 out of service, 4 low bikes, 9 low docks, **13 actionable**. Note: "one GBFS snapshot – a point-in-time count, not a trend" |
| `ops_breakdown` / `ops_actions` | 4 / 2 | 62.5 % available; COLLECT 9, DELIVER 4 |
| `ops_priorities` | 13 | **No `OUT_OF_SERVICE` station**. `maintenance_hold` = `HOLD - open maintenance ticket` for Church Ave & Story St (MT-1004); 12 `clear` |
| `demand_*` (7) | 1–37 | `data_scope` = `40-ROW DEVELOPMENT SAMPLE`; 40 trips, 35 member / 5 casual, 30 start stations |
| `weather_series` | 1 | `open-meteo@40.7128,-74.0060`, 48 hours, 0 missing |
| `weather_gaps` | 1 | `48-HOUR WEATHER / 40-TRIP DEVELOPMENT SAMPLE`: 4 of 40 trips with weather (10 %) |
| `weather_daily/buckets/precipitation` | 17 / 2 / 2 | Run |
| `quality_station` | 1 | Bronze 40 = Silver 40 + 0 + 0; fact 40; shortage 13 = priority 13; all three identities `true` |
| `quality_trip_rules` | 0 | No quarantined trips (empty result is correct) |
| `quality_lakeflow` | 22 | Expectations of update `a26b4385`; **0 failed records** |
| `quality_monthly` | 1 | 40 landed = 40 valid; reference coverage 100 % of the 40-station development dimension |
| `maintenance_status` | 5 | 1 CLOSED (HIGH), 1 IN_PROGRESS, 3 OPEN; "FICTIONAL, SYNTHETIC MAINTENANCE DATA" |
| `maintenance_conflicts` | 4 | All **4 open tickets** listed. MT-1004 (OPEN, HIGH, LOW_DOCKS, COLLECT_BIKES, priority 2): **HOLD - dispatcher decision before sending a van**. MT-1002, MT-1006, MT-1005: AVAILABLE, *no rebalancing action pending*, still visible |
| `maintenance_cdc` | 5 | v1 insert 5; v3 insert 1, update pre 2, post 2, delete 1 (11 events) |
| `maintenance_checkpoint` | 1 | **`processed_version` 4, `last_data_change_version` 3, `history_events` 11** |
| `assistant_evaluation` | 2 | Latest live evaluation (`d9e264ca…`): baseline correctness 0.148, 1 error; RAG 0.685, recall 1.0, coverage 0.938, 0 errors |

## Rendering (structural check of the deployed definition)

No browser was available to the operator, so no screenshot exists. The deployed JSON was checked instead:

| Page | Widgets | Types (version) | Layout |
|---|---|---|---|
| Current Operations | 12 | 7 counters (v2), 2 bar (v3), 1 table (v2), 2 text | Inside 12 columns, no overlap, counters ≥ 3 high |
| Historical Demand | 11 | 4 counters, 4 bar, 1 line, 1 table, 1 text | Same |
| Weather & Demand | 12 | 4 counters, 2 line, 3 scatter, 2 bar, 1 text | Same |
| Data Quality & Reliability | 11 | 6 counters, 1 bar, 2 tables, 2 text | Same |
| Maintenance & Assistant | 6 | 1 bar, 4 tables, 1 text | Same |

**Defect:** Lakeview stored each page title's `lines` as one string, for example `"## Current Operations**POINT-IN-TIME GBFS SNAPSHOT…"`, so the heading swallows its explanation on all five pages. The generator now ends every line but the last with a blank-line break (`build_dashboard.text`), with a regression test. Deploying that fix is one more dashboard-only update and needs approval. There are no filters or parameters in the definition, so there was nothing to test there.

## Publication and warehouse

| Item | Result |
|---|---|
| Published | **No.** The draft is visible to the owner, and publishing with merged titles would present a known defect. Publish (owner only, embedded credentials off) after the text fix is deployed. |
| Warehouse auto-stop | See the record of the auto-stop check in `RELEASE_AND_VALIDATION.md` |

Machine-readable record: [`2026-10-13_personal_dev_dashboard_validation.json`](2026-10-13_personal_dev_dashboard_validation.json), with every dataset's columns and up to 30 rows.
