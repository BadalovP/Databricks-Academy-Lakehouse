"""Gold analytics: station dimension, availability fact, summaries and priorities.

Every calculation here is deliberately simple arithmetic a student can defend out loud.
Nothing is modelled, predicted or weighted by anything that cannot be explained in one
sentence.

One honesty constraint runs through this module. The Bronze table currently holds ONE
real snapshot, so any "daily" or "trend" output covers a single observation per station.
`daily_station_summary` therefore reports `observations_per_station` and
`is_trend_capable` so a reader can see immediately that one snapshot is not a trend.
Nothing here fabricates history to make a chart look fuller.
"""

from __future__ import annotations

from typing import Any

SHORTAGE_STATUSES = ("LOW_BIKES", "LOW_DOCKS", "LOW_BIKES_AND_DOCKS")
FACT_COLUMNS: tuple[str, ...] = (
    "event_id",
    "execution_id",
    "station_id",
    "observed_at",
    "observation_date",
    "num_bikes_available",
    "num_docks_available",
    "num_ebikes_available",
    "capacity_estimate",
    "availability_status",
    "is_low_bikes",
    "is_low_docks",
    "station_short_name",
    "station_name",
    "capacity",
)


def station_dimension(station_information: Any) -> Any:
    """Build the station dimension from the GBFS station_information feed.

    Carries BOTH identifiers on purpose. `station_id` is the current GBFS UUID that
    station_status uses, while `short_name` (for example "7407.13") is what the
    historical trip CSVs put in their start_station_id column. Keeping both in the
    dimension is what lets historical trips join to current stations without anyone
    guessing which key to use.
    """
    from pyspark.sql import functions as F

    dimension = station_information.select(
        F.col("station_id").alias("station_id"),
        F.col("short_name").alias("station_short_name"),
        F.col("name").alias("station_name"),
        F.col("lat").cast("double").alias("latitude"),
        F.col("lon").cast("double").alias("longitude"),
        F.col("capacity").cast("int").alias("capacity"),
        F.coalesce(F.col("region_id"), F.lit("UNKNOWN")).alias("region_id"),
    )
    # The public feed should contain one row per UUID.  Ordering before deduplication
    # keeps the result stable even if a malformed snapshot repeats an identifier.
    from pyspark.sql import Window

    ordering = Window.partitionBy("station_id").orderBy(
        F.col("station_short_name").asc_nulls_last(),
        F.col("station_name").asc_nulls_last(),
        F.col("latitude").asc_nulls_last(),
        F.col("longitude").asc_nulls_last(),
    )
    return (
        dimension.withColumn("_choice", F.row_number().over(ordering))
        .where(F.col("_choice") == 1)
        .drop("_choice")
    )


def station_availability_fact(silver: Any, dimension: Any | None = None) -> Any:
    """One row per station observation, the grain of the Silver layer itself.

    A left join keeps observations for stations missing from the dimension rather than
    dropping them, because losing a real reading to a reference-data gap would be worse
    than reporting it with an unknown name.
    """
    from pyspark.sql import functions as F

    fact = silver.select(
        "event_id",
        "execution_id",
        "station_id",
        "observed_at",
        F.to_date("observed_at").alias("observation_date"),
        "num_bikes_available",
        "num_docks_available",
        "num_ebikes_available",
        "capacity_estimate",
        "availability_status",
        "is_low_bikes",
        "is_low_docks",
    )
    if dimension is None:
        # Keep the fact schema stable whether or not a dimension was supplied, so
        # downstream Gold functions never have to check which columns exist.
        enriched = (
            fact.withColumn("station_short_name", F.lit(None).cast("string"))
            .withColumn("station_name", F.lit(None).cast("string"))
            .withColumn("capacity", F.lit(None).cast("int"))
        )
    else:
        enriched = fact.join(
            dimension.select("station_id", "station_short_name", "station_name", "capacity"),
            on="station_id",
            how="left",
        )
    return enriched.select(*FACT_COLUMNS)


def daily_station_summary(fact: Any) -> Any:
    """Aggregate per station per day, and state plainly how many observations back it.

    `is_trend_capable` is false when a station has a single observation for the day. With
    one snapshot every row is false, which is the honest answer: min, max and average all
    equal the single reading, so the numbers are real but carry no trend information.
    """
    from pyspark.sql import functions as F

    return (
        fact.groupBy("station_id", "observation_date")
        .agg(
            F.count("*").alias("observations_per_station"),
            F.min("num_bikes_available").alias("min_bikes_available"),
            F.max("num_bikes_available").alias("max_bikes_available"),
            F.round(F.avg("num_bikes_available"), 2).alias("avg_bikes_available"),
            F.min("num_docks_available").alias("min_docks_available"),
            F.max("num_docks_available").alias("max_docks_available"),
            F.round(F.avg("num_docks_available"), 2).alias("avg_docks_available"),
            F.sum(F.col("is_low_bikes").cast("int")).alias("low_bike_observations"),
            F.sum(F.col("is_low_docks").cast("int")).alias("low_dock_observations"),
        )
        .withColumn("is_trend_capable", F.col("observations_per_station") > 1)
    )


def shortage_indicators(fact: Any) -> Any:
    """Current shortage list: the stations an operator would act on right now."""
    from pyspark.sql import functions as F

    return (
        fact.where(F.col("availability_status").isin(*SHORTAGE_STATUSES))
        .select(
            "event_id",
            "execution_id",
            "station_id",
            "station_name",
            "station_short_name",
            "observed_at",
            "num_bikes_available",
            "num_docks_available",
            "capacity_estimate",
            "availability_status",
        )
        .orderBy("availability_status", "num_bikes_available")
    )


def rebalancing_priority(
    fact: Any, *, low_bike_threshold: int = 2, low_dock_threshold: int = 2
) -> Any:
    """Rank stations needing attention, using arithmetic that is trivial to defend.

    The score is a deliberately plain sum of three whole-number parts:

      severity   2 when both bikes and docks are short, else 1 for either, else 0
      deficit    how many units below the threshold the short side is
      size       1 when the station's capacity estimate is at or above 20, else 0

    A bigger station running dry inconveniences more riders, which is the only reason
    size counts at all. There is no tuned weighting and no model, so the ranking can be
    recomputed by hand from the columns shown next to it. `action` says which way the van
    should move bikes, which is the part an operator actually needs.
    """
    from pyspark.sql import functions as F

    both_short = F.col("is_low_bikes") & F.col("is_low_docks")
    severity = (
        F.when(both_short, F.lit(2))
        .when(F.col("is_low_bikes") | F.col("is_low_docks"), F.lit(1))
        .otherwise(F.lit(0))
    )
    bike_deficit = F.greatest(F.lit(low_bike_threshold) - F.col("num_bikes_available"), F.lit(0))
    dock_deficit = F.greatest(F.lit(low_dock_threshold) - F.col("num_docks_available"), F.lit(0))
    deficit = F.greatest(bike_deficit, dock_deficit)
    size_bonus = F.when(F.col("capacity_estimate") >= F.lit(20), F.lit(1)).otherwise(F.lit(0))

    scored = (
        fact.where(F.col("availability_status").isin(*SHORTAGE_STATUSES))
        .withColumn("severity_points", severity)
        .withColumn("deficit_points", deficit)
        .withColumn("size_points", size_bonus)
        .withColumn(
            "priority_score",
            F.col("severity_points") + F.col("deficit_points") + F.col("size_points"),
        )
        .withColumn(
            "action",
            F.when(both_short, F.lit("INSPECT_STATION"))
            .when(F.col("is_low_bikes"), F.lit("DELIVER_BIKES"))
            .otherwise(F.lit("COLLECT_BIKES")),
        )
    )
    return scored.select(
        # execution_id is carried so this table can be corrected one execution at a time.
        # Without it a stale-row cleanup would have to overwrite the whole table, which
        # would destroy any other execution's rows.
        "execution_id",
        "station_id",
        "station_name",
        "observed_at",
        "num_bikes_available",
        "num_docks_available",
        "capacity_estimate",
        "availability_status",
        "severity_points",
        "deficit_points",
        "size_points",
        "priority_score",
        "action",
    ).orderBy(F.col("priority_score").desc(), F.col("num_bikes_available").asc())


def persist_gold_outputs(
    spark: Any,
    *,
    dimension: Any,
    fact: Any,
    daily_summary: Any,
    shortages: Any,
    priorities: Any,
    table_names: dict[str, str],
    execution_id: str,
) -> dict[str, dict[str, Any]]:
    """Persist every Gold output, migrating schemas and removing stale derived rows.

    The five outputs split into two kinds, and they need different write strategies:

    - `dimension`, `fact` and `daily_summary` are append-or-update by nature. A station
      observation never stops existing, so a plain MERGE is correct.
    - `shortages` and `priorities` are DERIVED filters whose row set legitimately shrinks
      when a station stops qualifying. MERGE alone would leave the old row behind forever,
      so these use an execution-scoped atomic replacement that also deletes rows the
      corrected source no longer produces.
    """
    from urbanflow.persistence import (
        evolve_delta_schema,
        merge_delta_table,
        replace_execution_scope,
    )

    required = {"dimension", "fact", "daily_summary", "shortages", "priorities"}
    missing = required - set(table_names)
    if missing:
        raise ValueError(f"Missing Gold table names: {sorted(missing)}")

    # Migrate first. A table created by an earlier release can be narrower than the
    # current projection, and Delta MERGE does not evolve the target on its own.
    migrations = {
        name: evolve_delta_schema(spark, frame, table_names[name])
        for name, frame in (
            ("dimension", dimension),
            ("fact", fact),
            ("daily_summary", daily_summary),
            ("shortages", shortages),
            ("priorities", priorities),
        )
    }

    results: dict[str, dict[str, Any]] = {
        "dimension": merge_delta_table(
            spark, dimension, table_names["dimension"], key_columns=("station_id",)
        ),
        "fact": merge_delta_table(spark, fact, table_names["fact"], key_columns=("event_id",)),
        "daily_summary": merge_delta_table(
            spark,
            daily_summary,
            table_names["daily_summary"],
            key_columns=("station_id", "observation_date"),
        ),
        "shortages": replace_execution_scope(
            spark,
            shortages,
            table_names["shortages"],
            key_columns=("event_id",),
            execution_column="execution_id",
            execution_id=execution_id,
        ),
        "priorities": replace_execution_scope(
            spark,
            priorities,
            table_names["priorities"],
            key_columns=("station_id", "observed_at"),
            execution_column="execution_id",
            execution_id=execution_id,
        ),
    }
    for name, migration in migrations.items():
        results[name]["schema_migration"] = migration
    return results


def reconcile_gold(silver: Any, fact: Any, summary: Any) -> dict[str, Any]:
    """Prove Gold neither invented nor lost rows relative to Silver.

    The fact table shares Silver's grain, so its count must match exactly and its event
    IDs must stay distinct. The daily summary is an aggregate, so the check there is that
    its observation counts add back up to the fact row count.
    """
    silver_rows = silver.count()
    fact_rows = fact.count()
    distinct_fact_ids = fact.select("event_id").distinct().count()
    summary_observations = summary.groupBy().sum("observations_per_station").collect()[0][0] or 0
    return {
        "silver_rows": silver_rows,
        "fact_rows": fact_rows,
        "distinct_fact_event_ids": distinct_fact_ids,
        "summary_observation_total": int(summary_observations),
        "fact_matches_silver": fact_rows == silver_rows,
        "fact_ids_unique": distinct_fact_ids == fact_rows,
        "summary_totals_match_fact": int(summary_observations) == fact_rows,
        "status": (
            "PASS"
            if fact_rows == silver_rows
            and distinct_fact_ids == fact_rows
            and int(summary_observations) == fact_rows
            else "FAIL"
        ),
    }
