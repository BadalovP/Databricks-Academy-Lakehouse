"""Reusable UrbanFlow business transformations for Python and Spark."""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, Iterable


def classify_availability(
    observation: dict[str, Any],
    *,
    low_bike_threshold: int = 2,
    low_dock_threshold: int = 2,
) -> dict[str, Any]:
    """Add simple, configurable operational flags to one station observation."""
    result = dict(observation)
    bikes = int(result["num_bikes_available"])
    docks = int(result["num_docks_available"])
    operational = all(bool(result.get(field)) for field in ("is_installed", "is_renting"))
    returnable = bool(result.get("is_returning"))

    # The threshold is inclusive on purpose: a station sitting exactly at the configured number
    # is already the shortage an operator wants to see, so 2 bikes with a threshold of 2 is LOW.
    result["is_low_bikes"] = operational and bikes <= low_bike_threshold
    # A station that is not accepting returns cannot have a dock shortage worth reporting.
    result["is_low_docks"] = operational and returnable and docks <= low_dock_threshold
    if not operational:
        result["availability_status"] = "OUT_OF_SERVICE"
    elif result["is_low_bikes"] and result["is_low_docks"]:
        result["availability_status"] = "LOW_BIKES_AND_DOCKS"
    elif result["is_low_bikes"]:
        result["availability_status"] = "LOW_BIKES"
    elif result["is_low_docks"]:
        result["availability_status"] = "LOW_DOCKS"
    else:
        result["availability_status"] = "AVAILABLE"
    return result


def deduplicate_latest(observations: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the newest station observation using source then collection time."""
    latest: dict[str, dict[str, Any]] = {}
    for row in observations:
        station_id = str(row["station_id"])
        key = (int(row.get("last_reported", 0)), str(row.get("collected_at", "")))
        existing = latest.get(station_id)
        existing_key = (
            (
                int(existing.get("last_reported", 0)),
                str(existing.get("collected_at", "")),
            )
            if existing
            else None
        )
        if existing_key is None or key > existing_key:
            latest[station_id] = dict(row)
    return [latest[key] for key in sorted(latest)]


def join_station_information(
    observations: Iterable[dict[str, Any]],
    station_information: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Left join status observations to current station reference records."""
    reference = {str(row["station_id"]): row for row in station_information}
    joined: list[dict[str, Any]] = []
    for observation in observations:
        station_id = str(observation["station_id"])
        result = dict(observation)
        station = reference.get(station_id)
        result["station_reference_matched"] = station is not None
        if station:
            for field in ("name", "lat", "lon", "capacity", "region_id", "station_type"):
                result[field] = station.get(field)
        joined.append(result)
    return joined


def summarize_availability(observations: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Count stations by the availability status added above."""
    counts = Counter(str(row["availability_status"]) for row in observations)
    return dict(sorted(counts.items()))


def apply_scd2_station_change(
    history: list[dict[str, Any]],
    incoming: dict[str, Any],
    *,
    effective_at: str,
    tracked_fields: tuple[str, ...] = ("name", "capacity", "lat", "lon"),
) -> list[dict[str, Any]]:
    """Educational pure-Python SCD2 transition used before the Delta MERGE version."""
    result = deepcopy(history)
    station_id = str(incoming["station_id"])
    current = next(
        (
            row
            for row in result
            if str(row["station_id"]) == station_id and bool(row.get("is_current"))
        ),
        None,
    )
    if current and all(current.get(field) == incoming.get(field) for field in tracked_fields):
        return result
    if current:
        current["valid_to"] = effective_at
        current["is_current"] = False
    new_version = dict(incoming)
    new_version["valid_from"] = effective_at
    new_version["valid_to"] = None
    new_version["is_current"] = True
    result.append(new_version)
    return result


def with_shortage_flags(
    dataframe: Any,
    *,
    low_bike_threshold: int = 2,
    low_dock_threshold: int = 2,
) -> Any:
    """Spark equivalent of classify_availability, imported lazily for local tests."""
    from pyspark.sql import functions as F

    operational = F.col("is_installed").cast("boolean") & F.col("is_renting").cast("boolean")
    # Same inclusive <= rule and same returning requirement as classify_availability above.
    low_bikes = operational & (F.col("num_bikes_available") <= F.lit(low_bike_threshold))
    low_docks = (
        operational
        & F.col("is_returning").cast("boolean")
        & (F.col("num_docks_available") <= F.lit(low_dock_threshold))
    )
    status = (
        F.when(~operational, F.lit("OUT_OF_SERVICE"))
        .when(low_bikes & low_docks, F.lit("LOW_BIKES_AND_DOCKS"))
        .when(low_bikes, F.lit("LOW_BIKES"))
        .when(low_docks, F.lit("LOW_DOCKS"))
        .otherwise(F.lit("AVAILABLE"))
    )
    return (
        dataframe.withColumn("is_low_bikes", low_bikes)
        .withColumn("is_low_docks", low_docks)
        .withColumn("availability_status", status)
    )


def latest_station_observations(dataframe: Any) -> Any:
    """Deduplicate a Spark DataFrame deterministically by station and timestamps."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    window = Window.partitionBy("station_id").orderBy(
        F.col("last_reported").desc(), F.col("collected_at").desc()
    )
    return (
        dataframe.withColumn("_urbanflow_rank", F.row_number().over(window))
        .filter(F.col("_urbanflow_rank") == 1)
        .drop("_urbanflow_rank")
    )


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
