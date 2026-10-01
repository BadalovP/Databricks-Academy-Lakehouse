"""Open-Meteo weather retrieval and enrichment for UrbanFlow trip demand.

Weather is the one external dimension that makes bike-share demand explicable: a wet, cold
morning and a dry, mild one produce very different trip counts at the same station. This
module retrieves REAL observations from Open-Meteo's archive API and joins them to trip
demand. Nothing here generates, interpolates or back-fills a measurement.

Three design decisions are worth stating plainly, because each one prevents a specific way
of accidentally lying with this data.

1. **Hourly grain, explicitly.** Open-Meteo returns one value per hour per coordinate. A trip
   starting at 09:47 is joined to the 09:00 observation, which is a documented rounding
   decision rather than an interpolation. `truncate_to_hour` is the single place that happens.

2. **A missing hour stays missing.** The API legitimately returns null for an hour a station
   did not report. Those arrive as null and are counted, never replaced with a zero or a
   neighbouring hour's value. Zero degrees and "unknown" are different facts, and a zero
   substituted for a null would quietly become a real-looking cold reading.

3. **One coordinate, named as such.** The configured latitude and longitude are a single point
   for New York City, not per-station weather. Every row carries that coordinate and
   `weather_grid_label` so a reader can see the resolution they are getting. Claiming
   station-level weather from a city-level series would be the easiest mistake to make here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# The three variables the dashboard actually uses. Keeping the list short and explicit means
# the request, the schema and the tests cannot drift apart.
HOURLY_VARIABLES: tuple[str, ...] = (
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
)

WEATHER_COLUMNS: tuple[str, ...] = (
    "weather_hour",
    "latitude",
    "longitude",
    "weather_grid_label",
    "temperature_celsius",
    "precipitation_mm",
    "wind_speed_kmh",
    "has_temperature",
    "has_precipitation",
    "has_wind",
    "source",
    "retrieved_at",
)


@dataclass(frozen=True)
class WeatherRequest:
    """One bounded Open-Meteo archive request.

    The date range is required and validated rather than defaulted. An accidental multi-year
    request is a slow, rude call against a free public API, so the bound is explicit and
    `max_days` refuses an unreasonably wide window outright.
    """

    latitude: float
    longitude: float
    start_date: str
    end_date: str
    timezone: str = "UTC"
    hourly: tuple[str, ...] = field(default=HOURLY_VARIABLES)

    def __post_init__(self) -> None:
        if not -90.0 <= self.latitude <= 90.0:
            raise ValueError(f"latitude {self.latitude} is outside -90..90.")
        if not -180.0 <= self.longitude <= 180.0:
            raise ValueError(f"longitude {self.longitude} is outside -180..180.")
        for label, value in (("start_date", self.start_date), ("end_date", self.end_date)):
            if not _is_iso_date(value):
                raise ValueError(f"{label} must be an ISO date (YYYY-MM-DD), received {value!r}.")
        if self.end_date < self.start_date:
            raise ValueError(f"end_date {self.end_date} precedes start_date {self.start_date}.")
        if not self.hourly:
            raise ValueError("At least one hourly variable is required.")

    @property
    def grid_label(self) -> str:
        """A short, human-readable name for the single coordinate these readings describe."""
        return f"open-meteo@{self.latitude:.4f},{self.longitude:.4f}"

    def query_parameters(self) -> dict[str, str]:
        return {
            "latitude": f"{self.latitude}",
            "longitude": f"{self.longitude}",
            "start_date": self.start_date,
            "end_date": self.end_date,
            "hourly": ",".join(self.hourly),
            "timezone": self.timezone,
        }


def _is_iso_date(value: str) -> bool:
    from datetime import date

    try:
        date.fromisoformat(value)
    except (TypeError, ValueError):
        return False
    return True


def span_days(request: WeatherRequest) -> int:
    """Inclusive number of days the request covers."""
    from datetime import date

    start = date.fromisoformat(request.start_date)
    end = date.fromisoformat(request.end_date)
    return (end - start).days + 1


def validate_request_size(request: WeatherRequest, *, max_days: int = 62) -> int:
    """Refuse an unbounded window before any network call is made.

    62 days covers the two-month archive window this project needs with room to spare, and
    stops a typo in a year from turning into a very large request against a free API.
    """
    days = span_days(request)
    if days > max_days:
        raise ValueError(
            f"Weather request covers {days} days, above the {max_days}-day bound. "
            "Narrow the range or raise max_days deliberately."
        )
    return days


def build_archive_url(request: WeatherRequest, *, base_url: str = ARCHIVE_URL) -> str:
    """Render the full archive URL, so a run log shows exactly what was requested."""
    from urllib.parse import urlencode

    return f"{base_url}?{urlencode(request.query_parameters())}"


def parse_hourly_payload(payload: dict[str, Any], request: WeatherRequest) -> list[dict[str, Any]]:
    """Turn Open-Meteo's column-oriented response into one record per hour.

    The API returns parallel arrays - `time`, `temperature_2m`, `precipitation` - rather than
    a list of objects. Lengths are checked against `time` instead of trusted, because a
    silently short array would otherwise shift every later reading onto the wrong hour, which
    is the kind of error that produces a plausible-looking but wrong chart.
    """
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        raise ValueError("Open-Meteo payload is missing its 'hourly' object.")
    times = hourly.get("time")
    if not isinstance(times, list):
        raise ValueError("Open-Meteo payload is missing its 'hourly.time' array.")

    series: dict[str, list[Any]] = {}
    for variable in request.hourly:
        values = hourly.get(variable)
        if values is None:
            raise ValueError(f"Open-Meteo payload is missing the requested variable {variable!r}.")
        if not isinstance(values, list) or len(values) != len(times):
            raise ValueError(
                f"Open-Meteo variable {variable!r} has {len(values) if isinstance(values, list) else 'no'} "
                f"values for {len(times)} hours; refusing to align them positionally."
            )
        series[variable] = values

    retrieved_at = _utc_now_iso()
    records: list[dict[str, Any]] = []
    for index, timestamp in enumerate(times):
        temperature = series.get("temperature_2m", [None] * len(times))[index]
        precipitation = series.get("precipitation", [None] * len(times))[index]
        wind = series.get("wind_speed_10m", [None] * len(times))[index]
        records.append(
            {
                "weather_hour": str(timestamp),
                "latitude": request.latitude,
                "longitude": request.longitude,
                "weather_grid_label": request.grid_label,
                "temperature_celsius": _as_float(temperature),
                "precipitation_mm": _as_float(precipitation),
                "wind_speed_kmh": _as_float(wind),
                # A null is a real answer from the API and is recorded as one, never zeroed.
                "has_temperature": temperature is not None,
                "has_precipitation": precipitation is not None,
                "has_wind": wind is not None,
                "source": "open-meteo-archive",
                "retrieved_at": retrieved_at,
            }
        )
    return records


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _utc_now_iso() -> str:
    from urbanflow.transformations import utc_now_iso

    return utc_now_iso()


def weather_coverage(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Report how complete the retrieved series is, before anything is joined to it.

    A join against a half-empty weather series produces charts that look fine and mean
    little, so the gaps are counted up front and carried into the evidence report.
    """
    total = len(records)
    with_temperature = sum(1 for row in records if row["has_temperature"])
    with_precipitation = sum(1 for row in records if row["has_precipitation"])
    with_wind = sum(1 for row in records if row["has_wind"])
    return {
        "hours": total,
        "hours_with_temperature": with_temperature,
        "hours_with_precipitation": with_precipitation,
        "hours_with_wind": with_wind,
        "missing_temperature": total - with_temperature,
        "missing_precipitation": total - with_precipitation,
        "missing_wind": total - with_wind,
        "temperature_completeness": round(with_temperature / total, 4) if total else 0.0,
        "complete": total > 0 and with_temperature == total,
    }


def fetch_hourly_weather(
    request: WeatherRequest,
    *,
    base_url: str = ARCHIVE_URL,
    timeout_seconds: float = 30.0,
    max_days: int = 62,
    opener: Any | None = None,
) -> list[dict[str, Any]]:
    """Retrieve real hourly observations. Bounded, and never invents a reading.

    `opener` is injected so tests exercise the parsing and validation without a network call.
    The default path uses urllib rather than adding a dependency for one GET.
    """
    import json
    from urllib.request import urlopen

    validate_request_size(request, max_days=max_days)
    url = build_archive_url(request, base_url=base_url)
    fetch = opener or urlopen
    with fetch(url, timeout=timeout_seconds) as response:  # noqa: S310 - fixed https base URL
        payload = json.loads(response.read().decode("utf-8"))
    return parse_hourly_payload(payload, request)


def weather_frame(spark: Any, records: list[dict[str, Any]]) -> Any:
    """Build the typed weather dimension frame from retrieved records."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("weather_hour", T.StringType()),
            T.StructField("latitude", T.DoubleType()),
            T.StructField("longitude", T.DoubleType()),
            T.StructField("weather_grid_label", T.StringType()),
            T.StructField("temperature_celsius", T.DoubleType()),
            T.StructField("precipitation_mm", T.DoubleType()),
            T.StructField("wind_speed_kmh", T.DoubleType()),
            T.StructField("has_temperature", T.BooleanType()),
            T.StructField("has_precipitation", T.BooleanType()),
            T.StructField("has_wind", T.BooleanType()),
            T.StructField("source", T.StringType()),
            T.StructField("retrieved_at", T.StringType()),
        ]
    )
    frame = spark.createDataFrame(records, schema)
    return frame.withColumn("weather_hour", F.to_timestamp("weather_hour")).select(*WEATHER_COLUMNS)


def truncate_to_hour(frame: Any, column: str, *, alias: str = "weather_hour") -> Any:
    """The single place a timestamp is reduced to its hour, so the rule is auditable.

    A trip starting at 09:47 joins to the 09:00 observation. That is a documented rounding
    decision, not an interpolation, and keeping it in one function stops a second, slightly
    different version appearing elsewhere.
    """
    from pyspark.sql import functions as F

    return frame.withColumn(alias, F.date_trunc("hour", F.col(column)))


def enrich_trips_with_weather(trips: Any, weather: Any, *, started_at: str = "started_at") -> Any:
    """Left-join trips to the hourly weather series on the truncated start hour.

    The join is LEFT on purpose. An hour the archive does not cover must not delete real
    trips - losing rides to a weather gap would corrupt the demand figures to improve the
    weather figures. `has_weather` makes the coverage measurable instead of invisible.
    """
    from pyspark.sql import functions as F

    stamped = truncate_to_hour(trips, started_at, alias="trip_hour")
    joined = stamped.join(
        weather.select(
            F.col("weather_hour"),
            F.col("temperature_celsius"),
            F.col("precipitation_mm"),
            F.col("wind_speed_kmh"),
            F.col("weather_grid_label"),
        ),
        stamped["trip_hour"] == weather["weather_hour"],
        "left",
    )
    return joined.withColumn("has_weather", F.col("weather_hour").isNotNull())


DEMAND_SUMMARY_COLUMNS: tuple[str, ...] = (
    "execution_id",
    "trip_date",
    "temperature_bucket",
    "trips",
    "avg_temperature_celsius",
    "total_precipitation_mm",
    "avg_wind_speed_kmh",
    "trips_with_weather",
    "weather_coverage",
)


def weather_demand_summary(enriched: Any, *, execution_id: str | None = None) -> Any:
    """Daily trip counts beside that day's weather: the dashboard's weather comparison.

    Temperature is averaged and precipitation summed, which matches how each is normally read
    - a day has an average temperature but a total rainfall. Buckets are coarse and stated in
    the column itself so nobody mistakes this for a model: it is a grouped average, and the
    comparison it supports is "more rain, fewer trips", not a prediction.

    `execution_id` is always present so the schema is stable whether or not a caller supplies
    one, and so the execution-scoped replacement that writes this table has a column to scope
    by. An hour with no reading buckets as UNKNOWN rather than being dropped or counted as cold.
    """
    from pyspark.sql import functions as F

    bucket = (
        F.when(F.col("temperature_celsius").isNull(), F.lit("UNKNOWN"))
        .when(F.col("temperature_celsius") < F.lit(0), F.lit("BELOW_FREEZING"))
        .when(F.col("temperature_celsius") < F.lit(10), F.lit("COLD_0_10C"))
        .when(F.col("temperature_celsius") < F.lit(20), F.lit("MILD_10_20C"))
        .otherwise(F.lit("WARM_20C_PLUS"))
    )
    return (
        enriched.withColumn("trip_date", F.to_date("started_at"))
        .withColumn("temperature_bucket", bucket)
        .groupBy("trip_date", "temperature_bucket")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("temperature_celsius"), 2).alias("avg_temperature_celsius"),
            F.round(F.sum("precipitation_mm"), 2).alias("total_precipitation_mm"),
            F.round(F.avg("wind_speed_kmh"), 2).alias("avg_wind_speed_kmh"),
            F.sum(F.col("has_weather").cast("int")).alias("trips_with_weather"),
        )
        .withColumn(
            "weather_coverage",
            F.when(F.col("trips") > 0, F.round(F.col("trips_with_weather") / F.col("trips"), 4)),
        )
        .withColumn(
            "execution_id",
            F.lit(execution_id) if execution_id else F.lit(None).cast("string"),
        )
        .select(*DEMAND_SUMMARY_COLUMNS)
    )


def persist_weather_outputs(
    spark: Any,
    *,
    weather: Any,
    demand_summary: Any,
    table_names: dict[str, str],
    execution_id: str,
) -> dict[str, dict[str, Any]]:
    """Persist the weather dimension and the demand comparison.

    The two need different strategies for the same reason Gold's do. The hourly dimension is
    append-or-update: an hour's observed temperature does not stop existing, so a MERGE keyed by
    grid label and hour is correct. The demand summary is a DERIVED aggregate whose rows
    legitimately disappear - a day whose trips all move to quarantine should lose its row - and
    UPDATE with INSERT can never remove one, so it uses the execution-scoped replacement.
    """
    from urbanflow.persistence import (
        evolve_delta_schema,
        merge_delta_table,
        replace_execution_scope,
    )

    required = {"weather", "demand_summary"}
    missing = required - set(table_names)
    if missing:
        raise ValueError(f"Missing weather table names: {sorted(missing)}")

    migrations = {
        name: evolve_delta_schema(spark, frame, table_names[name])
        for name, frame in (("weather", weather), ("demand_summary", demand_summary))
    }
    results: dict[str, dict[str, Any]] = {
        "weather": merge_delta_table(
            spark,
            weather,
            table_names["weather"],
            key_columns=("weather_grid_label", "weather_hour"),
        ),
        "demand_summary": replace_execution_scope(
            spark,
            demand_summary,
            table_names["demand_summary"],
            key_columns=("trip_date", "temperature_bucket"),
            execution_column="execution_id",
            execution_id=execution_id,
        ),
    }
    for name, migration in migrations.items():
        results[name]["schema_migration"] = migration
    return results


def reconcile_weather_join(enriched_rows: int, trip_rows: int, matched_rows: int) -> dict[str, Any]:
    """Prove the weather join neither dropped nor multiplied a single trip.

    A left join to a series with duplicate hours would MULTIPLY trips, which inflates demand
    while every individual number still looks reasonable. Checking that the enriched count
    equals the trip count catches that; a coverage shortfall is reported but is not a failure,
    because a genuinely uncovered hour is a fact about the archive.
    """
    preserved = enriched_rows == trip_rows
    return {
        "trip_rows": trip_rows,
        "enriched_rows": enriched_rows,
        "trips_with_weather": matched_rows,
        "trips_without_weather": trip_rows - matched_rows,
        "weather_coverage": round(matched_rows / trip_rows, 4) if trip_rows else 0.0,
        "rows_preserved": preserved,
        "status": "PASS" if preserved else "FAIL",
    }
