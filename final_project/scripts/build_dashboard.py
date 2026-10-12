"""Build the UrbanFlow 2.0 AI/BI (Lakeview) dashboards, one per bundle target, from committed SQL.

The datasets are the numbered, read-only statements in `sql/10`-`sql/14`, so the dashboards and
the SQL the tests guard cannot drift apart. Each target gets its own file with fully qualified
names for that target's catalog and schemas (from `databricks.yml`), so a DEV dashboard can never
read PROD tables, and neither can read Demo 3's or another project's.

Historical and weather statements are bound to ONE explicit execution each: the executions the
platform Job produces with its default parameters (the committed development samples), read from
`resources/urbanflow_final.job.yml`. Binding a full-archive execution is a reviewed change to the
Job defaults, committed together with its evidence.

    python scripts/build_dashboard.py            # writes dashboards/urbanflow_final.<target>.lvdash.json
    python scripts/build_dashboard.py --check    # fails if a committed file is stale
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_ROOT / "sql"
DASHBOARD_DIR = PROJECT_ROOT / "dashboards"
BUNDLE = PROJECT_ROOT / "databricks.yml"
JOB = PROJECT_ROOT / "resources" / "urbanflow_final.job.yml"
# The Job's default historical landing namespace is empty, which names this Bronze table.
HISTORICAL_BRONZE_TABLE = "bronze_historical_trips"
PLACEHOLDERS = ("catalog", "schema", "lakeflow_schema")
PLATFORM_JOB = "urbanflow_final_platform"


def output_path(target: str) -> Path:
    return DASHBOARD_DIR / f"urbanflow_final.{target}.lvdash.json"


def targets() -> dict[str, dict[str, str]]:
    """Catalog and schemas of every bundle target, exactly as databricks.yml declares them."""
    bundle = yaml.safe_load(BUNDLE.read_text(encoding="utf-8"))
    return {
        name: {key: str(target["variables"][key]) for key in PLACEHOLDERS}
        for name, target in bundle["targets"].items()
    }


def execution_ids() -> dict[str, str]:
    """The executions the platform Job writes with its default parameters."""
    jobs = yaml.safe_load(JOB.read_text(encoding="utf-8"))["resources"]["jobs"]
    parameters = {p["name"]: p["default"] for p in jobs[PLATFORM_JOB]["parameters"]}
    return {
        "historical_execution_id": str(parameters["historical_execution_id"]),
        "weather_execution_id": str(parameters["weather_execution_id"]),
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


def bind(sql: str, ids: dict[str, str], names: dict[str, str]) -> str:
    """Substitute the target's names and the execution IDs as validated literals."""
    for name, value in {**names, "historical_bronze_table": HISTORICAL_BRONZE_TABLE}.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"{name} {value!r} is not a plain identifier.")
        sql = sql.replace("${" + name + "}", value)
    if "${" in sql:
        raise ValueError("A target placeholder was left unbound.")
    for name, value in ids.items():
        if not re.fullmatch(r"[A-Za-z0-9._+-]+", value):
            raise ValueError(f"{name} {value!r} is not a safe literal.")
        sql = sql.replace(f":{name}", f"'{value}'")
    if re.search(r"(?<![:\w]):[a-z_]+_id\b", sql):
        raise ValueError("An execution parameter was left unbound.")
    return sql


MAINTENANCE_SQL = "14_dashboard_maintenance_and_assistant.sql"
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
    "quality_monthly": ("13_dashboard_data_quality.sql", 9, "Historical reconciliation"),
    "maintenance_status": (MAINTENANCE_SQL, 1, "Maintenance tickets"),
    "maintenance_conflicts": (MAINTENANCE_SQL, 2, "Stations with an open maintenance ticket"),
    "maintenance_cdc": (MAINTENANCE_SQL, 3, "CDC audit trail"),
    "maintenance_checkpoint": (MAINTENANCE_SQL, 4, "CDC checkpoint"),
    "assistant_evaluation": (MAINTENANCE_SQL, 5, "Assistant evaluation"),
}


def datasets(ids: dict[str, str], names: dict[str, str]) -> list[dict]:
    cache: dict[str, dict[int, str]] = {}
    out = []
    for name, (file, number, display) in DATASETS.items():
        statements = cache.setdefault(file, numbered_statements(SQL_DIR / file))
        sql = bind(statements[number], ids, names)
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
        # Lakeview counters need enough vertical room for the value and title. Height 2 renders
        # cramped in the current dashboard runtime; 3 is the supported compact KPI size.
        "position": {"x": x, "y": y, "width": w, "height": 3},
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
                "version": 2,
                "widgetType": "table",
                "encodings": {
                    "columns": [
                        {
                            "fieldName": c,
                            "displayName": c,
                        }
                        for c in columns
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
                "**POINT-IN-TIME GBFS SNAPSHOT — NOT A HISTORICAL AVAILABILITY TREND.** The "
                "station executions loaded into this target (by default the committed, dated "
                "Citi Bike sample snapshot). Nothing on this page is charted over time.",
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
            5,
            2,
        ),
        text(
            "ops_rule",
            [
                "**Actionable = LOW_BIKES + LOW_DOCKS + LOW_BIKES_AND_DOCKS.** An "
                "OUT_OF_SERVICE station is excluded: no rebalancing fixes it."
            ],
            2,
            5,
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
            (0, 8, 3, 5),
        ),
        chart(
            "ops_actions_bar",
            "bar",
            "ops_actions",
            "action",
            "stations",
            "Recommended action",
            (3, 8, 3, 5),
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
                "maintenance_hold",
            ],
            "Top 25 rebalancing priorities (score = severity + deficit + size; HOLD = open "
            "maintenance ticket)",
            (0, 13, 6, 7),
        ),
    ]
    demand = [
        text(
            "demand_title",
            [
                "## Historical Demand",
                "**REAL JANUARY 2024 CITI BIKE TRIPS** — every valid trip of execution "
                f"`{hist}` only. With the Job defaults this is the **40-ROW DEVELOPMENT SAMPLE**, "
                "so the counts illustrate the pipeline, not the month; each dataset carries its "
                "`data_scope` label. The archive is defined by ride END time.",
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
            (0, 5, 4, 5),
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
            (0, 10, 3, 5),
            y_aggregate="SUM",
        ),
        chart(
            "demand_duration_bar",
            "bar",
            "demand_durations",
            "duration_bucket",
            "trips",
            "Trip duration distribution",
            (3, 10, 3, 5),
        ),
        table(
            "demand_top_table",
            "demand_top_stations",
            ["direction", "station_name", "station_short_name", "trips"],
            "Top 15 origin and destination stations",
            (0, 15, 6, 8),
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
                f"closures. Weather execution `{weather}`; trips outside its window have no "
                "weather and are counted, not hidden.",
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
            (0, 6, 3, 4),
            x_scale="temporal",
        ),
        chart(
            "weather_daily_temp",
            "line",
            "weather_daily",
            "trip_date",
            "avg_temperature_celsius",
            "Average temperature per day (°C)",
            (3, 6, 3, 4),
            x_scale="temporal",
        ),
        chart(
            "weather_temp_scatter",
            "scatter",
            "weather_daily",
            "avg_temperature_celsius",
            "trips",
            "Daily trips vs temperature",
            (0, 10, 2, 5),
            x_scale="quantitative",
        ),
        chart(
            "weather_precip_scatter",
            "scatter",
            "weather_daily",
            "total_precipitation_mm",
            "trips",
            "Daily trips vs precipitation (mm)",
            (2, 10, 2, 5),
            x_scale="quantitative",
        ),
        chart(
            "weather_wind_scatter",
            "scatter",
            "weather_daily",
            "avg_wind_speed_kmh",
            "trips",
            "Daily trips vs wind (km/h)",
            (4, 10, 2, 5),
            x_scale="quantitative",
        ),
        chart(
            "weather_bucket_bar",
            "bar",
            "weather_buckets",
            "temperature_bucket",
            "avg_trips_per_day",
            "Average trips per day by temperature band",
            (0, 15, 3, 4),
        ),
        chart(
            "weather_precip_bar",
            "bar",
            "weather_precipitation",
            "precipitation_band",
            "avg_trips_per_day",
            "Average trips per day by rainfall band",
            (3, 15, 3, 4),
        ),
    ]
    quality = [
        text(
            "quality_title",
            [
                "## Data Quality & Reliability",
                "Reconciliation identities for this target's executions and the Lakeflow "
                "expectations of the latest COMPLETED update of its isolated pipeline. Run IDs "
                "and results are recorded as evidence only after a live run.",
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
                "**A low reference coverage is NOT a data-quality failure.** It is the share of "
                "rides starting at one of the 40 stations in the committed **DEVELOPMENT "
                "REFERENCE DIMENSION**, while the real network has thousands of stations. "
                "Landed = valid + quarantine + duplicates must hold exactly."
            ],
            0,
            5,
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
            (0, 7, 3, 5),
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
            (3, 7, 3, 5),
        ),
        table(
            "quality_lakeflow_table",
            "quality_lakeflow",
            ["dataset", "expectation", "passed_records", "failed_records"],
            "Lakeflow expectations from the pipeline event log (latest completed update)",
            (0, 12, 6, 8),
        ),
    ]
    maintenance = [
        text(
            "maintenance_title",
            [
                "## Maintenance & Assistant",
                "Station maintenance arrives through **Delta Change Data Feed**; the tickets are "
                "**FICTIONAL, SYNTHETIC DATA** on real sample station IDs. Assistant scores come "
                "only from approved **live** evaluations; offline fixture scores are never "
                "stored, so empty tables mean no live evaluation has run.",
            ],
            0,
            0,
            6,
            2,
        ),
        chart(
            "maintenance_status_bar",
            "bar",
            "maintenance_status",
            "status",
            "tickets",
            "Maintenance tickets by status",
            (0, 2, 3, 4),
            y_aggregate="SUM",
        ),
        table(
            "maintenance_checkpoint_table",
            "maintenance_checkpoint",
            [
                "processed_version",
                "last_data_change_version",
                "history_events",
                "processed_at",
            ],
            "CDC checkpoint: newest scanned and reconciled source version (after reconciliation "
            "only)",
            (3, 2, 3, 2),
        ),
        table(
            "maintenance_cdc_table",
            "maintenance_cdc",
            ["commit_version", "change_type", "events", "committed_at"],
            "CDC audit trail by source version",
            (3, 4, 3, 4),
        ),
        table(
            "maintenance_conflicts_table",
            "maintenance_conflicts",
            [
                "maintenance_id",
                "station_name",
                "maintenance_status",
                "maintenance_severity",
                "availability_status",
                "priority_score",
                "action",
                "dispatch_note",
            ],
            "Stations with an open maintenance ticket: current status and pending action (HOLD)",
            (0, 8, 6, 5),
        ),
        table(
            "assistant_evaluation_table",
            "assistant_evaluation",
            [
                "variant",
                "questions",
                "answer_correctness",
                "retrieval_recall",
                "source_coverage",
                "errors",
                "evaluated_at",
            ],
            "Assistant: baseline vs RAG, latest live evaluation",
            (0, 13, 6, 4),
        ),
    ]
    return [
        {"name": "operations", "displayName": "Current Operations", "layout": operations},
        {"name": "demand", "displayName": "Historical Demand", "layout": demand},
        {"name": "weather", "displayName": "Weather & Demand", "layout": weather_page},
        {"name": "quality", "displayName": "Data Quality & Reliability", "layout": quality},
        {"name": "maintenance", "displayName": "Maintenance & Assistant", "layout": maintenance},
    ]


def build(target: str) -> dict:
    ids = execution_ids()
    definition = {
        "datasets": datasets(ids, targets()[target]),
        "pages": pages(ids),
        "uiSettings": {
            "theme": {
                "canvasBackgroundColor": {"light": "#F7FAFC", "dark": "#111827"},
                "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#1F2937"},
                "widgetBorderColor": {"light": "#FFFFFF", "dark": "#1F2937"},
                "fontColor": {"light": "#172554", "dark": "#E5E7EB"},
                "selectionColor": {"light": "#0072B2", "dark": "#5ADBFF"},
                # Okabe-Ito inspired: readable categories without relying on red/green alone.
                "visualizationColors": [
                    "#0072B2",
                    "#E69F00",
                    "#009E73",
                    "#CC79A7",
                    "#D55E00",
                    "#56B4E9",
                    "#F0E442",
                ],
                "widgetHeaderAlignment": "LEFT",
                "widgetCornerRadius": 8,
            }
        },
    }
    for page in definition["pages"]:
        page["pageType"] = "PAGE_TYPE_CANVAS"
        page["layoutVersion"] = "GRID_V1"
        # The original generator used a six-column design vocabulary. Lakeview's documented
        # canvas is twelve columns, so scale horizontal positions once at serialization time.
        # Keeping the page definitions in six logical columns makes the dense operations layout
        # readable while the emitted dashboard fills the supported grid exactly.
        for item in page["layout"]:
            item["position"]["x"] *= 2
            item["position"]["width"] *= 2
    return definition


def render(target: str) -> str:
    return json.dumps(build(target), indent=2, ensure_ascii=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    stale = []
    for target in targets():
        path, rendered = output_path(target), render(target)
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != rendered:
                stale.append(str(path))
            continue
        DASHBOARD_DIR.mkdir(exist_ok=True)
        path.write_text(rendered, encoding="utf-8", newline="\n")
        print(f"wrote {path}")
    if stale:
        print(f"stale: {stale}; run scripts/build_dashboard.py", file=sys.stderr)
        return 1
    if args.check:
        print("dashboard definitions are current")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
