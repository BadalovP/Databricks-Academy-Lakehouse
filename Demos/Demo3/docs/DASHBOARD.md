# UrbanFlow AI/BI dashboard - datasets, layout and creation steps

Status: **prepared locally, not created.** Every query exists and is read-only; the dashboard
object itself has not been created in the workspace. The reason is in [Why it is not created
yet](#why-it-is-not-created-yet).

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

Each numbered query in the SQL files becomes one dashboard dataset. They are plain `SELECT`
statements against persisted tables - no writes, no DDL - and a test
(`tests/test_sql_assets.py`) enforces that, along with the rule that every table reference is
fully qualified and inside the UrbanFlow schema.

| Dataset | File | Query | Feeds |
|---|---|---|---|
| `ops_headline` | `sql/10_dashboard_current_operations.sql` | 1 | Counter tiles |
| `ops_breakdown` | `sql/10_...` | 2 | Availability bar/pie |
| `ops_priorities` | `sql/10_...` | 3 | Top-25 priority table |
| `ops_station_detail` | `sql/10_...` | 4 | Station table and map |
| `ops_actions` | `sql/10_...` | 5 | Action summary |
| `demand_daily` | `sql/11_dashboard_historical_demand.sql` | 1 | Trips-per-day line |
| `demand_stations` | `sql/11_...` | 2 | Busiest-stations bar |
| `demand_rider_mix` | `sql/11_...` | 3 | Member share area |
| `demand_hourly` | `sql/11_...` | 4 | Hour-by-weekday heatmap |
| `demand_durations` | `sql/11_...` | 5 | Duration histogram |
| `demand_match_rate` | `sql/11_...` | 6 | Join-health counter |
| `weather_daily` | `sql/12_dashboard_weather.sql` | 1 | Trips vs temperature combo |
| `weather_buckets` | `sql/12_...` | 2 | Trips per temperature bucket |
| `weather_precipitation` | `sql/12_...` | 3 | Trips per rainfall band |
| `weather_series` | `sql/12_...` | 4 | Coverage and provenance tile |
| `weather_gaps` | `sql/12_...` | 5 | Uncovered-trips counter |
| `quality_counts` | `sql/13_dashboard_data_quality.sql` | 1 | Reconciliation tiles |
| `quality_rules` | `sql/13_...` | 2 | Quarantine-by-rule bar |
| `quality_freshness` | `sql/13_...` | 3 | Freshness tile |
| `quality_reference` | `sql/13_...` | 4 | Reference match tile |
| `quality_keys` | `sql/13_...` | 5 | Uniqueness tile |
| `quality_trips` | `sql/13_...` | 6 | Trip outcome tiles |

## Layout

Four pages, in the order an operator would actually read them: what is wrong now, what is
normal, what explains the difference, and whether to trust any of it.

```
PAGE 1 - CURRENT OPERATIONS                 (one snapshot; no time axis anywhere)
+-------------------------------------------------------------------------+
| [ stations ] [ available ] [ out of service ] [ actionable shortages ]  |  counters
| caption: "point-in-time count from one GBFS snapshot at <observed_at>"  |
+----------------------------------+--------------------------------------+
| Availability breakdown (bar)     | Action summary (bar)                 |
+----------------------------------+--------------------------------------+
| Top 25 rebalancing priorities (table, score and its 3 components)      |
+-------------------------------------------------------------------------+
| Station map, sized by bikes available, coloured by status               |
+-------------------------------------------------------------------------+

PAGE 2 - HISTORICAL DEMAND                  (real history; a time axis is valid here)
+-------------------------------------------------------------------------+
| Trips per day (line)                                                    |
+----------------------------------+--------------------------------------+
| Busiest stations (bar)           | Member vs casual share (area)        |
+----------------------------------+--------------------------------------+
| Demand by hour and weekday (heatmap)                                    |
+----------------------------------+--------------------------------------+
| Trip duration buckets (histogram)| [ match rate ] counter               |
+----------------------------------+--------------------------------------+

PAGE 3 - WEATHER                            (comparison, not prediction)
+-------------------------------------------------------------------------+
| banner: "one city coordinate, not per-station weather; comparisons only"|
+-------------------------------------------------------------------------+
| Trips and temperature by day (dual axis)                                |
+----------------------------------+--------------------------------------+
| Trips per temperature bucket     | Trips per rainfall band              |
+----------------------------------+--------------------------------------+
| Weather coverage and provenance (table: grid label, hours, gaps, source)|
+-------------------------------------------------------------------------+

PAGE 4 - DATA QUALITY
+-------------------------------------------------------------------------+
| [ bronze reconciles ] [ fact = silver ] [ unassigned rows ] [ fresh? ]  |
+----------------------------------+--------------------------------------+
| Quarantined rows by rule (bar)   | Trip outcomes (bar)                  |
+----------------------------------+--------------------------------------+
| Reference match rate, with the "40-station sample" note shown           |
+-------------------------------------------------------------------------+
```

### Tiles that must carry their caveat in the tile

Not in a footnote, because footnotes get cropped in a screenshot:

- Every Page 1 tile: "one snapshot, point-in-time".
- Page 3 banner: one coordinate, comparison only.
- Reference match rate: "the dimension is a committed 40-station sample, so a low rate is
  expected".
- Daily availability summary, wherever shown: `is_trend_capable = false`.

## Why it is not created yet

Creating the dashboard object is not in itself destructive, but publishing one requires a SQL
warehouse to execute the datasets, and **serverless SQL warehouse time is billable**. This
project does not assume serverless is free, so the warehouse is not started and the dashboard
is not published without separate approval.

There is also a dependency ordering problem that would make a dashboard created today
misleading rather than merely empty:

| Section | Status as of 2026-10-02 |
|---|---|
| Current operations | **Unblocked.** The Phase 2 correction ran; shortage and priority hold exactly 657 correct rows with zero out-of-service entries |
| Historical demand | **Data exists, but from a 40-ROW DEVELOPMENT SAMPLE only.** Every tile would show 40 trips over 17 days. Honest, and far too thin to present as demand analysis - this page needs the full monthly archive |
| Weather | **Data exists, 48 hours.** Only 4 of 40 trips fall inside it, so coverage is 0.1. The charts would be technically correct and practically empty |
| Data quality | **Fully available.** Every reconciliation tile has real numbers behind it |

So the remaining blocker is no longer correctness, it is the SQL warehouse plus the thinness of
the sample data. Pages 1 and 4 would be genuinely informative today. Pages 2 and 3 should wait
for the monthly archive, or carry a prominent 40-row label - showing a 40-trip "demand trend" to a
supervisor would invite exactly the wrong conclusion.

## Exact creation steps, once approved

1. **Run the prerequisites in order:** the Phase 2 correction run, then notebook 06 against a
   landed archive, then notebook 07.
2. **Confirm the data is right before building anything on it.** Run query 1 of
   `sql/13_dashboard_data_quality.sql` and check `bronze_reconciles`, `fact_matches_silver` and
   `derived_tables_agree` are all true and `unassigned_rows` is 0.
3. **Start a SQL warehouse** - the smallest available - and note that this begins billing.
4. **Create the dashboard:** in the workspace, *New → Dashboard*, name it
   `[dev] UrbanFlow Operations`.
5. **Add each dataset** from the table above: *Data → Create from SQL*, paste the query, name
   it with the dataset name given.
6. **Build the four pages** per the layout, adding each caveat caption as you add its tile
   rather than afterwards.
7. **Re-read every tile against the "what is real" table** at the top of this document. Any
   tile with a time axis must be on Page 2, or sourced from the weather daily series.
8. **Stop the SQL warehouse** when finished. It does not stop itself immediately, and an idle
   warehouse still bills until its auto-stop elapses.
9. **Record the evidence:** dashboard URL, the date, and the row counts each tile showed, then
   move the Lab 1 and Lab 6 dashboard rows in
   [LABS_1_TO_9_COVERAGE.md](LABS_1_TO_9_COVERAGE.md) from `Pending live` to `Validated live`.
   Not before: a created dashboard with no recorded figures is not evidence.

## Alternative that needs no warehouse

Every query here also runs in a notebook cell on the already-running GP1 cluster with
`display()`. That produces the same figures and the same charts for a presentation, with no SQL
warehouse and no extra cost. It is not an AI/BI dashboard object, so it does not close the
Academy dashboard requirement - but it does let the numbers be shown and discussed, which is
the part that matters for a supervisor conversation.
