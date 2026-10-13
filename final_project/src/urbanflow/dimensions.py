"""SCD Type 1 and Type 2 handling for the station dimension.

Why the station dimension and not the observations: availability readings are immutable
facts, so there is nothing to slowly change about them. Station *attributes* do change —
capacity is adjusted, stations are renamed, stations move — and those are exactly the
changes SCD is for.

On test data, stated plainly: this project has one real GBFS snapshot, so it has not yet
observed a real station attribute change over time. The SCD functions here are therefore
exercised against explicitly labelled synthetic scenarios in `tests/test_dimensions.py`,
and `SYNTHETIC_CHANGE_NOTE` is written into any demonstration output so nobody can mistake
a teaching scenario for observed history. When a second real snapshot exists, the same
functions run against it unchanged.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

SYNTHETIC_CHANGE_NOTE = (
    "SYNTHETIC TEST SCENARIO: this change was authored to demonstrate SCD behaviour. "
    "It is not an observed Citi Bike station change."
)

# Attributes worth tracking history for. Coordinates change when a station is physically
# relocated, and capacity changes when docks are added or removed; both matter when
# interpreting an old availability reading.
SCD2_TRACKED_COLUMNS: tuple[str, ...] = (
    "station_name",
    "capacity",
    "latitude",
    "longitude",
)

SCD1_OVERWRITE_COLUMNS: tuple[str, ...] = ("station_name", "capacity", "region_id")


def scd1_overwrite(existing: Any, incoming: Any, *, key: str = "station_id") -> Any:
    """SCD Type 1: the newest attribute value replaces the old one, keeping no history.

    Correct when the old value was simply wrong — a corrected spelling has no analytical
    value worth storing. Wrong when the old value was right at the time, which is what
    Type 2 below is for.
    """
    from pyspark.sql import functions as F

    right = incoming.alias("incoming")
    joined = existing.alias("existing").join(right, on=key, how="full_outer")
    selected = [F.col(key)]
    for column in SCD1_OVERWRITE_COLUMNS:
        # Incoming wins where present; the existing value survives only where the
        # incoming feed says nothing about this station.
        selected.append(
            F.coalesce(F.col(f"incoming.{column}"), F.col(f"existing.{column}")).alias(column)
        )
    return joined.select(*selected)


def scd2_apply(
    existing: Any,
    incoming: Any,
    *,
    effective_from: str,
    key: str = "station_id",
) -> Any:
    """SCD Type 2: close the old row and open a new one when a tracked attribute changes.

    Produces the classic shape — `valid_from`, `valid_to`, `is_current` — so a reading from
    last month can be interpreted against the station as it was then rather than as it is
    now.

    Rules applied, in plain terms: an unchanged station keeps its single current row; a
    changed station gets its current row closed at `effective_from` and a new current row
    opened; a brand-new station simply gets a current row. Rows already closed are carried
    through untouched, because history is append-only.
    """
    from pyspark.sql import functions as F

    if not effective_from.strip():
        raise ValueError("effective_from must be a non-empty timestamp string.")
    try:
        datetime.fromisoformat(effective_from.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("effective_from must be an ISO-8601 timestamp string.") from exc

    if incoming.where(F.col(key).isNull()).limit(1).count():
        raise ValueError(f"Incoming SCD2 snapshot contains a null {key}.")
    if incoming.groupBy(key).count().where(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Incoming SCD2 snapshot contains duplicate {key} values.")

    history = existing.where(~F.col("is_current"))
    current = existing.where(F.col("is_current"))
    if current.groupBy(key).count().where(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Existing SCD2 dimension has multiple current rows for {key}.")

    incoming_current = incoming.select(key, *SCD2_TRACKED_COLUMNS).withColumn(
        "_incoming", F.lit(True)
    )

    # An explicit join condition, not `on=key`. Joining on the key name collapses both
    # sides into one column, after which `c.station_id` and `n.station_id` cannot be
    # resolved -- and telling "absent from the incoming feed" apart from "new station"
    # depends on being able to see each side's key independently.
    joined = current.alias("c").join(
        incoming_current.alias("n"),
        F.col(f"c.{key}") == F.col(f"n.{key}"),
        "full_outer",
    )

    changed = None
    for column in SCD2_TRACKED_COLUMNS:
        # Null-safe inequality: a value appearing or disappearing is a change too.
        condition = ~F.col(f"c.{column}").eqNullSafe(F.col(f"n.{column}"))
        changed = condition if changed is None else (changed | condition)

    present_in_both = F.col(f"c.{key}").isNotNull() & F.col(f"n.{key}").isNotNull()
    is_new_station = F.col(f"c.{key}").isNull() & F.col(f"n.{key}").isNotNull()
    is_gone = F.col(f"n.{key}").isNull()

    # A changed version cannot start at or before the version it closes. Unchanged
    # idempotent reruns at the same effective timestamp remain valid.
    invalid_time = (
        present_in_both
        & changed
        & (F.lit(effective_from).cast("timestamp") <= F.col("c.valid_from"))
    )
    if joined.where(invalid_time).limit(1).count():
        raise ValueError("effective_from must be later than the current version's valid_from.")

    # 1. Unchanged, or no longer in the incoming feed: keep the existing current row.
    keep = joined.where((present_in_both & ~changed) | is_gone).select(
        F.col(f"c.{key}").alias(key),
        *[F.col(f"c.{c}").alias(c) for c in SCD2_TRACKED_COLUMNS],
        F.col("c.valid_from").alias("valid_from"),
        F.col("c.valid_to").alias("valid_to"),
        F.col("c.is_current").alias("is_current"),
    )

    # 2. Changed: close the old version at effective_from.
    closed = joined.where(present_in_both & changed).select(
        F.col(f"c.{key}").alias(key),
        *[F.col(f"c.{c}").alias(c) for c in SCD2_TRACKED_COLUMNS],
        F.col("c.valid_from").alias("valid_from"),
        F.lit(effective_from).cast("timestamp").alias("valid_to"),
        F.lit(False).alias("is_current"),
    )

    # 3. Changed or new: open the new current version.
    opened = joined.where((present_in_both & changed) | is_new_station).select(
        F.coalesce(F.col(f"n.{key}"), F.col(f"c.{key}")).alias(key),
        *[F.col(f"n.{c}").alias(c) for c in SCD2_TRACKED_COLUMNS],
        F.lit(effective_from).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
    )

    return history.unionByName(keep).unionByName(closed).unionByName(opened)


def initial_scd2_dimension(dimension: Any, *, effective_from: str) -> Any:
    """Seed an empty SCD2 dimension from the first observed snapshot."""
    from pyspark.sql import functions as F

    try:
        datetime.fromisoformat(effective_from.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("effective_from must be an ISO-8601 timestamp string.") from exc
    if dimension.where(F.col("station_id").isNull()).limit(1).count():
        raise ValueError("Initial SCD2 snapshot contains a null station_id.")
    if dimension.groupBy("station_id").count().where(F.col("count") > 1).limit(1).count():
        raise ValueError("Initial SCD2 snapshot contains duplicate station_id values.")
    return dimension.select("station_id", *SCD2_TRACKED_COLUMNS).select(
        "station_id",
        *SCD2_TRACKED_COLUMNS,
        F.lit(effective_from).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
    )


def scd2_audit(dimension: Any, *, key: str = "station_id") -> dict[str, Any]:
    """Check the invariants that make an SCD2 table trustworthy.

    Exactly one current row per key, and every closed row carrying a `valid_to`. Both are
    easy to break with a careless merge and hard to notice afterwards, because queries
    filtering on `is_current` still return plausible-looking data.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    total = dimension.count()
    distinct_keys = dimension.select(key).distinct().count()
    current = dimension.where(F.col("is_current"))
    current_rows = current.count()
    distinct_current_keys = current.select(key).distinct().count()
    closed_without_valid_to = dimension.where(
        ~F.col("is_current") & F.col("valid_to").isNull()
    ).count()
    current_with_valid_to = dimension.where(
        F.col("is_current") & F.col("valid_to").isNotNull()
    ).count()
    invalid_closed_intervals = dimension.where(
        ~F.col("is_current")
        & F.col("valid_to").isNotNull()
        & (F.col("valid_to") <= F.col("valid_from"))
    ).count()
    ordered = dimension.withColumn(
        "_next_valid_from",
        F.lead("valid_from").over(Window.partitionBy(key).orderBy("valid_from")),
    )
    overlapping_intervals = ordered.where(
        F.col("valid_to").isNotNull()
        & F.col("_next_valid_from").isNotNull()
        & (F.col("valid_to") > F.col("_next_valid_from"))
    ).count()
    one_current = current_rows == distinct_current_keys == distinct_keys
    return {
        "total_rows": total,
        "distinct_keys": distinct_keys,
        "current_rows": current_rows,
        "distinct_current_keys": distinct_current_keys,
        "one_current_row_per_key": one_current,
        "closed_rows_missing_valid_to": closed_without_valid_to,
        "current_rows_with_valid_to": current_with_valid_to,
        "invalid_closed_intervals": invalid_closed_intervals,
        "overlapping_intervals": overlapping_intervals,
        "status": (
            "PASS"
            if one_current
            and closed_without_valid_to == 0
            and current_with_valid_to == 0
            and invalid_closed_intervals == 0
            and overlapping_intervals == 0
            else "FAIL"
        ),
    }
