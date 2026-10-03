"""Build the UrbanFlow AI/BI (Lakeview) dashboard definition from the committed SQL files.

The datasets are the numbered, read-only statements in `sql/10`-`sql/13`, so the dashboard and
the SQL that the tests guard can never drift apart. Every historical and weather statement is
bound to ONE explicit execution: the IDs below are the full January 2024 executions recorded in
`evidence/2026-10-03_unified_release.json` for unified run 96337578882467, substituted as
literals so no viewer can silently widen the scope.

    python scripts/build_dashboard.py            # writes dashboards/urbanflow.lvdash.json
    python scripts/build_dashboard.py --check    # fails if the committed file is stale
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_ROOT / "sql"
OUTPUT = PROJECT_ROOT / "dashboards" / "urbanflow.lvdash.json"
EVIDENCE = PROJECT_ROOT / "evidence" / "2026-10-03_unified_release.json"

DISPLAY_NAME = "UrbanFlow — NYC Mobility Operations & Demand"
FULL_MONTH_RUN_ID = 96337578882467
LAKEFLOW_PIPELINE_ID = "fb8a0b8a-cdf8-45c4-bff6-2d117a516fb9"
LAKEFLOW_UPDATE_ID = "ba6710ed-bd97-46e0-a0c0-616050e3c9b9"


def execution_ids() -> dict[str, str]:
    """Resolve the two execution IDs from evidence rather than typing them by hand."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))
    run = evidence["job_runs"]["full_month"]
    if run["run_id"] != FULL_MONTH_RUN_ID or run["result_state"] != "SUCCESS":
        raise ValueError("The evidence does not record a successful full-month run.")
    return {
        "historical_execution_id": run["parameters"]["historical_execution_id"],
        "weather_execution_id": run["parameters"]["weather_execution_id"],
    }


def numbered_statements(path: Path) -> dict[int, str]:
    """Map each `-- N.` header to the active statement that follows it."""
    statements: dict[int, str] = {}
    number = None
    buffer: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        header = re.match(r"--\s*(\d+)\.\s", line)
        if header:
            number = int(header.group(1))
            buffer = []
            continue
        if line.strip().startswith("--") or number is None:
            continue
        buffer.append(line)
        if line.rstrip().endswith(";"):
            statements[number] = "\n".join(buffer).rstrip().rstrip(";")
            number = None
    return statements


def bind(sql: str, ids: dict[str, str]) -> str:
    for name, value in ids.items():
        if not re.fullmatch(r"[A-Za-z0-9._+-]+", value):
            raise ValueError(f"{name} {value!r} is not a safe literal.")
        sql = sql.replace(f":{name}", f"'{value}'")
    if re.search(r"(?<![:\w]):[a-z_]+_id\b", sql):
        raise ValueError("An execution parameter was left unbound.")
    return sql


DATASETS = {
    # name: (file, statement number, display name)
    "ops_headline": ("10_dashboard_current_operations.sql", 1, "Snapshot headline"),
    "ops_breakdown": ("10_dashboard_current_operations.sql", 2, "Availability breakdown"),
    "ops_priorities": ("10_dashboard_current_operations.sql", 3, "Top 25 priorities"),
    "ops_actions": ("10_dashboard_current_operations.sql", 5, "Action summary"),
    "demand_daily": ("11_dashboard_historical_demand.sql", 1, "Trips per day"),
    "demand_hourly": ("11_dashboard_historical_demand.sql", 4, "Trips by hour and weekday"),
    "demand_durations": ("11_dashboard_historical_demand.sql", 5, "Duration distribution"),
    "demand_headline": ("11_dashboard_historical_demand.sql", 7, "Monthly headline"),
    "demand_rideable": ("11_dashboard_historical_demand.sql", 8, "Rideable type"),
    "demand_top_stations": ("11_dashboard_historical_demand.sql", 9, "Top stations"),
    "demand_day_type": ("11_dashboard_historical_demand.sql", 10, "Weekday vs weekend"),
    "weather_daily": ("12_dashboard_weather.sql", 1, "Daily trips and weather"),
    "weather_buckets": ("12_dashboard_weather.sql", 2, "Trips by temperature"),
    "weather_precipitation": ("12_dashboard_weather.sql", 3, "Trips by rainfall"),
    "weather_series": ("12_dashboard_weather.sql", 4, "Weather series provenance"),
    "weather_gaps": ("12_dashboard_weather.sql", 5, "Weather coverage"),
    "quality_station": ("13_dashboard_data_quality.sql", 1, "Station reconciliation"),
    "quality_trip_rules": ("13_dashboard_data_quality.sql", 7, "Trip quarantine reasons"),
    "quality_lakeflow": ("13_dashboard_data_quality.sql", 8, "Lakeflow expectations"),
    "quality_monthly": ("13_dashboard_data_quality.sql", 9, "Monthly reconciliation"),
}


def datasets(ids: dict[str, str]) -> list[dict]:
    cache: dict[str, dict[int, str]] = {}
    out = []
    for name, (file, number, display) in DATASETS.items():
        statements = cache.setdefault(file, numbered_statements(SQL_DIR / file))
        sql = bind(statements[number], ids)
        out.append({"name": name, "displayName": display, "queryLines": sql.splitlines(True)})
    return out


def _field(name: str, expression: str | None = None) -> dict:
    return {"name": name, "expression": expression or f"`{name}`"}


def _query(dataset: str, fields: list[dict], disaggregated: bool = True) -> list[dict]:
    return [
        {
            "name": "main_query",
            "query": {"datasetName": dataset, "fields": fields, "disaggregated": disaggregated},
        }
    ]


def text(name: str, lines: list[str], x: int, y: int, w: int, h: int) -> dict:
    return {
        "widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def counter(name: str, dataset: str, field: str, title: str, x: int, y: int, w: int = 1) -> dict:
    return {
        "widget": {
            "name": name,
            "queries": _query(dataset, [_field(field)]),
            "spec": {
                "version": 2,
                "widgetType": "counter",
                "encodings": {"value": {"fieldName": field, "displayName": title}},
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": 2},
    }


def chart(
    name: str,
    kind: str,
    dataset: str,
    x_field: str,
    y_field: str,
    title: str,
    pos: tuple[int, int, int, int],
    *,
    x_scale: str = "categorical",
    y_aggregate: str | None = None,
) -> dict:
    """Bar, line or scatter. `y_aggregate` sums a field per x value when rows are finer."""
    y_name = f"{y_aggregate.lower()}({y_field})" if y_aggregate else y_field
    fields = [
        _field(x_field),
        _field(y_name, f"{y_aggregate}(`{y_field}`)") if y_aggregate else _field(y_field),
    ]
    x, y, w, h = pos
    return {
        "widget": {
            "name": name,
            "queries": _query(dataset, fields, disaggregated=y_aggregate is None),
            "spec": {
                "version": 3,
                "widgetType": kind,
                "encodings": {
                    "x": {"fieldName": x_field, "scale": {"type": x_scale}, "displayName": x_field},
                    "y": {
                        "fieldName": y_name,
                        "scale": {"type": "quantitative"},
                        "displayName": y_field,
                    },
                },
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def table(name: str, dataset: str, columns: list[str], title: str, pos: tuple) -> dict:
    x, y, w, h = pos
    return {
        "widget": {
            "name": name,
            "queries": _query(dataset, [_field(c) for c in columns]),
            "spec": {
                "version": 1,
                "widgetType": "table",
                "encodings": {
                    "columns": [
                        {
                            "fieldName": c,
                            "displayName": c,
                            "title": c,
                            "type": "string",
                            "displayAs": "string",
                            "visible": True,
                            "order": i,
                        }
                        for i, c in enumerate(columns)
                    ]
                },
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def pages(ids: dict[str, str]) -> list[dict]:
    hist, weather = ids["historical_execution_id"], ids["weather_execution_id"]
    operations = [
        text(
            "ops_title",
            [
                "## Current Operations",
                "**POINT-IN-TIME GBFS SNAPSHOT — NOT A HISTORICAL AVAILABILITY TREND.** One real "
                "Citi Bike station-status snapshot (station execution "
                "`urbanflow-20260929T195132Z-r3`). Nothing on this page is charted over time.",
            ],
            0,
            0,
            6,
            2,
        ),
        counter("ops_total", "ops_headline", "total_stations_observed", "Stations observed", 0, 2),
        counter("ops_available", "ops_headline", "available_stations", "AVAILABLE", 1, 2),
        counter("ops_low_bikes", "ops_headline", "low_bike_stations", "LOW_BIKES", 2, 2),
        counter("ops_low_docks", "ops_headline", "low_dock_stations", "LOW_DOCKS", 3, 2),
        counter("ops_both", "ops_headline", "both_low_stations", "LOW_BIKES_AND_DOCKS", 4, 2),
        counter("ops_oos", "ops_headline", "out_of_service_stations", "OUT_OF_SERVICE", 5, 2),
        counter(
            "ops_actionable",
            "ops_headline",
            "actionable_shortages",
            "Actionable shortages",
            0,
            4,
            2,
        ),
        text(
            "ops_rule",
            [
                "**Actionable = LOW_BIKES + LOW_DOCKS + LOW_BIKES_AND_DOCKS = 657.** An "
                "OUT_OF_SERVICE station is excluded: no rebalancing fixes it."
            ],
            2,
            4,
            4,
            2,
        ),
        chart(
            "ops_breakdown_bar",
            "bar",
            "ops_breakdown",
            "availability_status",
            "stations",
            "Stations by availability status",
            (0, 6, 3, 5),
        ),
        chart(
            "ops_actions_bar",
            "bar",
            "ops_actions",
            "action",
            "stations",
            "Recommended action",
            (3, 6, 3, 5),
        ),
        table(
            "ops_priorities_table",
            "ops_priorities",
            [
                "priority_score",
                "action",
                "station_name",
                "availability_status",
                "num_bikes_available",
                "num_docks_available",
                "severity_points",
                "deficit_points",
                "size_points",
            ],
            "Top 25 rebalancing priorities (score = severity + deficit + size)",
            (0, 11, 6, 7),
        ),
    ]
    demand = [
        text(
            "demand_title",
            [
                "## January 2024 Historical Demand",
                "**REAL JANUARY 2024 CITI BIKE MONTHLY ARCHIVE** — every valid trip of execution "
                f"`{hist}` (unified run `{FULL_MONTH_RUN_ID}`). The 40-row development sample is "
                "not on this page. The archive is defined by ride END time, so 374 rides started "
                "on 31 December.",
            ],
            0,
            0,
            6,
            2,
        ),
        counter("demand_valid", "demand_headline", "valid_trips", "Valid trips", 0, 2),
        counter("demand_member", "demand_headline", "member_trips", "Member trips", 1, 2),
        counter("demand_casual", "demand_headline", "casual_trips", "Casual trips", 2, 2),
        counter(
            "demand_stations", "demand_headline", "distinct_start_stations", "Start stations", 3, 2
        ),
        chart(
            "demand_day_type_bar",
            "bar",
            "demand_day_type",
            "day_type",
            "avg_trips_per_day",
            "Average trips per day: weekday vs weekend",
            (4, 2, 2, 4),
        ),
        chart(
            "demand_daily_line",
            "line",
            "demand_daily",
            "trip_date",
            "trips",
            "Trips per day",
            (0, 4, 4, 5),
            x_scale="temporal",
        ),
        chart(
            "demand_rideable_bar",
            "bar",
            "demand_rideable",
            "rideable_type",
            "trips",
            "Rideable type",
            (4, 6, 2, 3),
        ),
        chart(
            "demand_hourly_bar",
            "bar",
            "demand_hourly",
            "hour_of_day",
            "trips",
            "Trips by hour of day (UTC timestamps as stored)",
            (0, 9, 3, 5),
            y_aggregate="SUM",
        ),
        chart(
            "demand_duration_bar",
            "bar",
            "demand_durations",
            "duration_bucket",
            "trips",
            "Trip duration distribution",
            (3, 9, 3, 5),
        ),
        table(
            "demand_top_table",
            "demand_top_stations",
            ["direction", "station_name", "station_short_name", "trips"],
            "Top 15 origin and destination stations",
            (0, 14, 6, 8),
        ),
    ]
    weather_page = [
        text(
            "weather_title",
            [
                "## Weather & Demand",
                "Hourly Open-Meteo archive for **one NYC reference coordinate** "
                "(40.7128, -74.0060) — **not station-level weather**. Charts show "
                "**association, not causation**: nothing controls for weekday, holidays or "
                f"closures. Weather execution `{weather}`. The 374 trips without weather "
                "started on 31 December, outside the January weather window.",
            ],
            0,
            0,
            6,
            3,
        ),
        counter("weather_hours", "weather_series", "hours_retrieved", "Weather hours", 0, 3),
        counter(
            "weather_coverage",
            "weather_gaps",
            "weather_coverage_percent",
            "Trip-weather coverage %",
            1,
            3,
        ),
        counter(
            "weather_missing",
            "weather_gaps",
            "trips_without_weather",
            "Trips without weather",
            2,
            3,
        ),
        counter(
            "weather_missing_temp",
            "weather_series",
            "missing_temperature_hours",
            "Hours missing temperature",
            3,
            3,
        ),
        chart(
            "weather_daily_trips",
            "line",
            "weather_daily",
            "trip_date",
            "trips",
            "Trips per day",
            (0, 5, 3, 4),
            x_scale="temporal",
        ),
        chart(
            "weather_daily_temp",
            "line",
            "weather_daily",
            "trip_date",
            "avg_temperature_celsius",
            "Average temperature per day (°C)",
            (3, 5, 3, 4),
            x_scale="temporal",
        ),
        chart(
            "weather_temp_scatter",
            "scatter",
            "weather_daily",
            "avg_temperature_celsius",
            "trips",
            "Daily trips vs temperature",
            (0, 9, 2, 5),
            x_scale="quantitative",
        ),
        chart(
            "weather_precip_scatter",
            "scatter",
            "weather_daily",
            "total_precipitation_mm",
            "trips",
            "Daily trips vs precipitation (mm)",
            (2, 9, 2, 5),
            x_scale="quantitative",
        ),
        chart(
            "weather_wind_scatter",
            "scatter",
            "weather_daily",
            "avg_wind_speed_kmh",
            "trips",
            "Daily trips vs wind (km/h)",
            (4, 9, 2, 5),
            x_scale="quantitative",
        ),
        chart(
            "weather_bucket_bar",
            "bar",
            "weather_buckets",
            "temperature_bucket",
            "avg_trips_per_day",
            "Average trips per day by temperature band",
            (0, 14, 3, 4),
        ),
        chart(
            "weather_precip_bar",
            "bar",
            "weather_precipitation",
            "precipitation_band",
            "avg_trips_per_day",
            "Average trips per day by rainfall band",
            (3, 14, 3, 4),
        ),
    ]
    quality = [
        text(
            "quality_title",
            [
                "## Data Quality & Reliability",
                f"Latest successful unified Job run: **`{FULL_MONTH_RUN_ID}`** "
                "(`[azure] UrbanFlow End-to-End`, 7/7 tasks, final validation PASS). Lakeflow "
                f"update **`{LAKEFLOW_UPDATE_ID}`** on pipeline `{LAKEFLOW_PIPELINE_ID}`: "
                "COMPLETED, isolated schema, business results identical to the imperative path.",
            ],
            0,
            0,
            6,
            2,
        ),
        counter("quality_landed", "quality_monthly", "landed_rows", "Landed", 0, 2),
        counter("quality_valid", "quality_monthly", "valid_trips", "Valid", 1, 2),
        counter("quality_quarantine", "quality_monthly", "quarantined_trips", "Quarantine", 2, 2),
        counter("quality_duplicates", "quality_monthly", "duplicate_trips", "Duplicates", 3, 2),
        counter("quality_reconciles", "quality_monthly", "reconciles", "Reconciles", 4, 2),
        counter(
            "quality_reference",
            "quality_monthly",
            "reference_coverage_percent",
            "Reference coverage %",
            5,
            2,
        ),
        text(
            "quality_reference_note",
            [
                "**Reference coverage 3.43% is NOT a data-quality failure.** It is the share of "
                "rides starting at one of the 40 stations in the committed **DEVELOPMENT "
                "REFERENCE DIMENSION**. 39 of those 40 stations appear in January 2024, out of "
                "2,223 stations in the real month. Landed = valid + quarantine + duplicates "
                "holds exactly."
            ],
            0,
            4,
            6,
            2,
        ),
        chart(
            "quality_rules_bar",
            "bar",
            "quality_trip_rules",
            "failed_rule",
            "trips_failing",
            "Why trips were quarantined (every row has a named reason)",
            (0, 6, 3, 5),
        ),
        table(
            "quality_station_table",
            "quality_station",
            [
                "bronze_rows",
                "silver_rows",
                "quarantine_rows",
                "duplicate_rows",
                "fact_rows",
                "shortage_rows",
                "priority_rows",
                "bronze_reconciles",
                "fact_matches_silver",
                "derived_tables_agree",
            ],
            "Station snapshot reconciliation (Bronze = Silver + Quarantine + Duplicates)",
            (3, 6, 3, 5),
        ),
        table(
            "quality_lakeflow_table",
            "quality_lakeflow",
            ["dataset", "expectation", "passed_records", "failed_records"],
            "Lakeflow expectations from the pipeline event log (latest completed update)",
            (0, 11, 6, 8),
        ),
    ]
    return [
        {"name": "operations", "displayName": "Current Operations", "layout": operations},
        {"name": "demand", "displayName": "January 2024 Historical Demand", "layout": demand},
        {"name": "weather", "displayName": "Weather & Demand", "layout": weather_page},
        {"name": "quality", "displayName": "Data Quality & Reliability", "layout": quality},
    ]


def build() -> dict:
    ids = execution_ids()
    definition = {"datasets": datasets(ids), "pages": pages(ids)}
    for page in definition["pages"]:
        page["pageType"] = "PAGE_TYPE_CANVAS"
    return definition


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = json.dumps(build(), indent=2, ensure_ascii=False) + "\n"
    if args.check:
        current = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
        if current != rendered:
            print(f"{OUTPUT} is stale; run scripts/build_dashboard.py", file=sys.stderr)
            return 1
        print("dashboard definition is current")
        return 0
    OUTPUT.parent.mkdir(exist_ok=True)
    OUTPUT.write_text(rendered, encoding="utf-8", newline="\n")
    print(f"wrote {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
