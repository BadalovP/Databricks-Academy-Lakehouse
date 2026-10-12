# UrbanFlow AI/BI dashboard

Status: **PUBLISHED 2026-10-03.** "UrbanFlow — NYC Mobility Operations & Demand", dashboard
`01f1bf66a828102f9167c26cdd833277`, published with embedded credentials on the shared academy
warehouse `3ed106620db591d9` (Serverless Starter Warehouse, 2X-Small, 5-minute auto-stop, owned by
the academy). It has **no schedule**, so it queries only when someone opens it.

| Fact | Value |
|---|---|
| Workspace path | `/Users/parvinbadalov@softserve.academy/UrbanFlow — NYC Mobility Operations & Demand.lvdash.json` |
| Pages | Current Operations · January 2024 Historical Demand · Weather & Demand · Data Quality & Reliability |
| Datasets | 20, generated from `sql/10`-`sql/13` by [`scripts/build_dashboard.py`](../scripts/build_dashboard.py) |
| Committed definition | [`dashboards/urbanflow.lvdash.json`](../dashboards/urbanflow.lvdash.json); a test fails if it drifts from the SQL |
| Historical execution | `urbanflow-hist-202401-full-gh-37140618864-1` (unified run `96337578882467`) |
| Weather execution | `urbanflow-weather-202401-full-gh-37140618864-1` |
| Station execution | `urbanflow-20260929T195132Z-r3` (the one real snapshot) |

**Execution binding.** The two execution IDs are resolved from
`evidence/2026-10-03_unified_release.json`, not typed, and substituted as literals into every
historical and weather dataset. A viewer cannot widen the scope, which is also why there is no
interactive execution filter: it would reintroduce the risk of silently summing the 40-row sample
with the month.

**Validated before publishing.** All 20 datasets were executed read-only on the warehouse first.
Key figures, each matching the stored evidence: 2,520 stations / 1,774 available / 278 low bikes /
374 low docks / 5 both / 89 out of service / 657 actionable; 1,886,318 valid trips, 1,678,496
member, 207,822 casual, 2,223 start stations; 744 weather hours, 99.98% trip coverage, 374 trips
without weather; 1,888,085 landed reconciling with 1,767 quarantined (1,160 missing start station,
607 too long) and 64,635 trips (3.43%) at a referenced station; 22 Lakeflow expectations.

**What could not be verified from here:** the widgets were not inspected visually in a browser.
The datasets are proven; the rendering of each widget relies on the Lakeview definition format.

**A defect found while building it.** `sql/13` query 6 counted historical trips across every
execution - on the real data it would have shown 1,886,358 valid trips and `ride_ids_unique =
false`, because the sample comes from the same archive. It is now scoped, and a per-statement test
requires every read of an execution-keyed table to select one execution.

**Alert.** The Academy alert was validated live on 2026-10-03 and deleted afterwards
([ALERT_VALIDATION.md](../evidence/ALERT_VALIDATION.md)). It was deliberately **not** recreated:
the station data is one frozen snapshot, so a recurring `actionable_shortages > 0` alert would fire
identically forever and spend warehouse time while nobody is watching.

## What is real today, and what is not

This is the first thing a reader needs, because a dashboard makes every number look equally
solid.

| Section | Backed by | Trend-capable? |
|---|---|---|
| Current operations | One real GBFS snapshot, 2,520 stations, validated live | **No.** A point-in-time count |
| Historical demand | Official Citi Bike trip archive | **Yes.** Weeks of real rides |
| Weather | Real Open-Meteo archive observations, one city coordinate | Yes, but a comparison only |
| Data quality | The persisted tables themselves | n/a |

Two rules follow from that table and they are enforced in the SQL, not left to goodwill:

1. **Never chart the availability tables over time.** There is one snapshot. Query 1 in
   `sql/10_dashboard_current_operations.sql` returns a `reading_note` column saying so, so the
   caption cannot drift away from the data.
2. **Never present the weather section as prediction.** It is a grouped average over coarse,
   labelled buckets. It controls for nothing: not day of week, not holidays, not closures.

## Datasets

Each dataset is one numbered, read-only statement. `tests/test_sql_assets.py` enforces that they only read, stay inside the UrbanFlow schemas, are fully qualified, and that every read of an execution-keyed table selects one execution.

| Dataset | Source statement | Page(s) |
|---|---|---|
| `ops_headline` | `sql/10_dashboard_current_operations.sql` query 1 - Snapshot headline | Current Operations |
| `ops_breakdown` | `sql/10_dashboard_current_operations.sql` query 2 - Availability breakdown | Current Operations |
| `ops_priorities` | `sql/10_dashboard_current_operations.sql` query 3 - Top 25 priorities | Current Operations |
| `ops_actions` | `sql/10_dashboard_current_operations.sql` query 5 - Action summary | Current Operations |
| `demand_daily` | `sql/11_dashboard_historical_demand.sql` query 1 - Trips per day | January 2024 Historical Demand |
| `demand_hourly` | `sql/11_dashboard_historical_demand.sql` query 4 - Trips by hour and weekday | January 2024 Historical Demand |
| `demand_durations` | `sql/11_dashboard_historical_demand.sql` query 5 - Duration distribution | January 2024 Historical Demand |
| `demand_headline` | `sql/11_dashboard_historical_demand.sql` query 7 - Monthly headline | January 2024 Historical Demand |
| `demand_rideable` | `sql/11_dashboard_historical_demand.sql` query 8 - Rideable type | January 2024 Historical Demand |
| `demand_top_stations` | `sql/11_dashboard_historical_demand.sql` query 9 - Top stations | January 2024 Historical Demand |
| `demand_day_type` | `sql/11_dashboard_historical_demand.sql` query 10 - Weekday vs weekend | January 2024 Historical Demand |
| `weather_daily` | `sql/12_dashboard_weather.sql` query 1 - Daily trips and weather | Weather & Demand |
| `weather_buckets` | `sql/12_dashboard_weather.sql` query 2 - Trips by temperature | Weather & Demand |
| `weather_precipitation` | `sql/12_dashboard_weather.sql` query 3 - Trips by rainfall | Weather & Demand |
| `weather_series` | `sql/12_dashboard_weather.sql` query 4 - Weather series provenance | Weather & Demand |
| `weather_gaps` | `sql/12_dashboard_weather.sql` query 5 - Weather coverage | Weather & Demand |
| `quality_station` | `sql/13_dashboard_data_quality.sql` query 1 - Station reconciliation | Data Quality & Reliability |
| `quality_trip_rules` | `sql/13_dashboard_data_quality.sql` query 7 - Trip quarantine reasons | Data Quality & Reliability |
| `quality_lakeflow` | `sql/13_dashboard_data_quality.sql` query 8 - Lakeflow expectations | Data Quality & Reliability |
| `quality_monthly` | `sql/13_dashboard_data_quality.sql` query 9 - Monthly reconciliation | Data Quality & Reliability |

## Layout as published

Generated from the committed definition. Text tiles carry the caveats inside the page, not in a footnote, so a screenshot cannot crop them away.

**Current Operations**

- text: **POINT-IN-TIME GBFS SNAPSHOT — NOT A HISTORICAL AVAILABILITY TREND.** One real Citi Bike station-status snapshot (station execution `urbanflow-202609…
- counter: Stations observed
- counter: AVAILABLE
- counter: LOW_BIKES
- counter: LOW_DOCKS
- counter: LOW_BIKES_AND_DOCKS
- counter: OUT_OF_SERVICE
- counter: Actionable shortages
- text: **Actionable = LOW_BIKES + LOW_DOCKS + LOW_BIKES_AND_DOCKS = 657.** An OUT_OF_SERVICE station is excluded: no rebalancing fixes it.
- bar: Stations by availability status
- bar: Recommended action
- table: Top 25 rebalancing priorities (score = severity + deficit + size)

**January 2024 Historical Demand**

- text: **REAL JANUARY 2024 CITI BIKE MONTHLY ARCHIVE** — every valid trip of execution `urbanflow-hist-202401-full-gh-37140618864-1` (unified run `9633757888…
- counter: Valid trips
- counter: Member trips
- counter: Casual trips
- counter: Start stations
- bar: Average trips per day: weekday vs weekend
- line: Trips per day
- bar: Rideable type
- bar: Trips by hour of day (UTC timestamps as stored)
- bar: Trip duration distribution
- table: Top 15 origin and destination stations

**Weather & Demand**

- text: Hourly Open-Meteo archive for **one NYC reference coordinate** (40.7128, -74.0060) — **not station-level weather**. Charts show **association, not cau…
- counter: Weather hours
- counter: Trip-weather coverage %
- counter: Trips without weather
- counter: Hours missing temperature
- line: Trips per day
- line: Average temperature per day (°C)
- scatter: Daily trips vs temperature
- scatter: Daily trips vs precipitation (mm)
- scatter: Daily trips vs wind (km/h)
- bar: Average trips per day by temperature band
- bar: Average trips per day by rainfall band

**Data Quality & Reliability**

- text: Latest successful unified Job run: **`96337578882467`** (`[azure] UrbanFlow End-to-End`, 7/7 tasks, final validation PASS). Lakeflow update **`ba6710e…
- counter: Landed
- counter: Valid
- counter: Quarantine
- counter: Duplicates
- counter: Reconciles
- counter: Reference coverage %
- text: **Reference coverage 3.43% is NOT a data-quality failure.** It is the share of rides starting at one of the 40 stations in the committed **DEVELOPMENT…
- bar: Why trips were quarantined (every row has a named reason)
- table: Station snapshot reconciliation (Bronze = Silver + Quarantine + Duplicates)
- table: Lakeflow expectations from the pipeline event log (latest completed update)

## Rebuilding or republishing

```powershell
python scripts/build_dashboard.py            # regenerate dashboards/urbanflow.lvdash.json
python scripts/build_dashboard.py --check    # fails if the file is stale (pytest runs the same check in CI)
databricks lakeview update 01f1bf66a828102f9167c26cdd833277 --json @payload.json --profile dev
databricks lakeview publish 01f1bf66a828102f9167c26cdd833277 --embed-credentials `
  --warehouse-id 3ed106620db591d9 --profile dev
```

`payload.json` carries `serialized_dashboard` (the committed file's contents). Do not add a
schedule: the station page is one snapshot, and a refresh schedule would only spend warehouse time.
