"""UrbanFlow Gold Lakeflow declarations.

Same discipline as the Silver module: every figure comes from `urbanflow.gold`, which the
notebook path and the unit tests also use. The declarative layer contributes the table
definitions, the expectations and the dependency graph, not the arithmetic.

Two honesty constraints are enforced here rather than left to a reader's goodwill.

1. **One snapshot is not a trend.** `gold_daily_station_summary` carries `is_trend_capable`,
   and with a single observation per station every row is false. The expectation below asserts
   the flag is consistent with the observation count, so a future change that quietly starts
   claiming trend capability from one reading fails the pipeline instead of reaching a chart.

2. **An out-of-service station is not an actionable shortage.** The shortage and priority
   tables carry an expectation that no row is `OUT_OF_SERVICE`. That is the exact defect the
   first live Phase 2 run produced - 746 rows where the reference rule yields 657 - so it is
   asserted declaratively as well as in the tests.

The station dimension is deliberately named `dim_station_development_sample`. It is built from
the committed 40-row reference sample, not from a complete feed, and the name says so wherever
it appears.
"""

from pyspark import pipelines as dp

from urbanflow.gold import (
    daily_station_summary,
    rebalancing_priority,
    shortage_indicators,
    station_availability_fact,
    station_dimension,
)

SHORTAGE_EXPECTATIONS = {
    "shortage_is_actionable": "availability_status <> 'OUT_OF_SERVICE'",
    "shortage_status_is_known": (
        "availability_status IN ('LOW_BIKES','LOW_DOCKS','LOW_BIKES_AND_DOCKS')"
    ),
    "execution_is_attributed": "execution_id IS NOT NULL",
}

PRIORITY_EXPECTATIONS = {
    "priority_is_actionable": "availability_status <> 'OUT_OF_SERVICE'",
    "score_is_explainable": "priority_score = severity_points + deficit_points + size_points",
    "action_is_known": "action IN ('INSPECT_STATION','DELIVER_BIKES','COLLECT_BIKES')",
    "execution_is_attributed": "execution_id IS NOT NULL",
}

FACT_EXPECTATIONS = {
    "event_id_present": "event_id IS NOT NULL",
    "observation_date_present": "observation_date IS NOT NULL",
}

SUMMARY_EXPECTATIONS = {
    # One snapshot per station means exactly one observation, so trend capability must be
    # false. This fails loudly if anything ever starts claiming a trend from one reading.
    "trend_claim_matches_observations": (
        "(observations_per_station > 1 AND is_trend_capable = true) "
        "OR (observations_per_station <= 1 AND is_trend_capable = false)"
    ),
    "counts_are_positive": "observations_per_station > 0",
}

DIMENSION_EXPECTATIONS = {
    "station_id_present": "station_id IS NOT NULL",
    "short_name_present": "station_short_name IS NOT NULL",
}


@dp.materialized_view(
    name="station_information_raw",
    comment=(
        "GBFS station_information records landed in the UrbanFlow Volume. The reference feed "
        "for station names, capacities and the short_name that historical trips join on."
    ),
    table_properties={"quality": "bronze", "project": "urbanflow"},
)
def station_information_raw():
    """Read the approved main-schema station reference and restore its raw feed column names."""
    source = spark.conf.get("urbanflow.station_reference_source_table")  # noqa: F821
    return spark.read.table(source).selectExpr(  # noqa: F821
        "station_id",
        "station_short_name AS short_name",
        "station_name AS name",
        "latitude AS lat",
        "longitude AS lon",
        "capacity",
        "region_id",
    )


@dp.materialized_view(
    name="dim_station_development_sample",
    comment=(
        "Station dimension built from the committed 40-row GBFS reference sample. Carries BOTH "
        "the GBFS UUID and short_name, because historical trip archives join on short_name."
    ),
    table_properties={"quality": "gold", "project": "urbanflow"},
)
@dp.expect_all(DIMENSION_EXPECTATIONS)
def dim_station_development_sample():
    """Named a sample because it is one: 40 stations, not the full feed."""
    return station_dimension(spark.read.table("station_information_raw"))  # noqa: F821


@dp.materialized_view(
    name="fact_station_availability",
    comment="One row per station observation, left-enriched with station reference data.",
    table_properties={"quality": "gold", "project": "urbanflow"},
)
@dp.expect_all(FACT_EXPECTATIONS)
def fact_station_availability():
    """Shares Silver's grain exactly, so its row count must match Silver's."""
    return station_availability_fact(
        spark.read.table("silver_station_status"),  # noqa: F821
        spark.read.table("dim_station_development_sample"),  # noqa: F821
    )


@dp.materialized_view(
    name="gold_daily_station_summary",
    comment=(
        "Per station per day aggregate. `is_trend_capable` is false whenever a station has a "
        "single observation, which is the honest answer for one snapshot."
    ),
    table_properties={"quality": "gold", "project": "urbanflow"},
)
@dp.expect_all(SUMMARY_EXPECTATIONS)
def gold_daily_station_summary():
    """Min, max and average over one observation all equal it, and the flag says so."""
    return daily_station_summary(spark.read.table("fact_station_availability"))  # noqa: F821


@dp.materialized_view(
    name="gold_station_shortage",
    comment=(
        "Stations an operator would act on right now. An out-of-service station is excluded "
        "because no amount of rebalancing fixes it."
    ),
    table_properties={"quality": "gold", "project": "urbanflow"},
)
@dp.expect_all(SHORTAGE_EXPECTATIONS)
def gold_station_shortage():
    """Derived filter: its row set legitimately shrinks when a station stops qualifying."""
    return shortage_indicators(spark.read.table("fact_station_availability"))  # noqa: F821


@dp.materialized_view(
    name="gold_rebalancing_priority",
    comment=(
        "Ranked rebalancing list. The score is a plain sum of severity, deficit and size "
        "points, so it can be recomputed by hand from the columns beside it."
    ),
    table_properties={"quality": "gold", "project": "urbanflow"},
)
@dp.expect_all(PRIORITY_EXPECTATIONS)
def gold_rebalancing_priority():
    """No model and no tuned weighting; every ranking is defensible out loud."""
    low_bikes = int(spark.conf.get("urbanflow.low_bike_threshold", "2"))  # noqa: F821
    low_docks = int(spark.conf.get("urbanflow.low_dock_threshold", "2"))  # noqa: F821
    return rebalancing_priority(
        spark.read.table("fact_station_availability"),  # noqa: F821
        low_bike_threshold=low_bikes,
        low_dock_threshold=low_docks,
    )


__all__ = [
    "DIMENSION_EXPECTATIONS",
    "FACT_EXPECTATIONS",
    "PRIORITY_EXPECTATIONS",
    "SHORTAGE_EXPECTATIONS",
    "SUMMARY_EXPECTATIONS",
    "station_information_raw",
    "dim_station_development_sample",
    "fact_station_availability",
    "gold_daily_station_summary",
    "gold_rebalancing_priority",
    "gold_station_shortage",
]
