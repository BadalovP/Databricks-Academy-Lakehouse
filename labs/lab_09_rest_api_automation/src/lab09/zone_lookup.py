"""Pure-Python mirror of pipeline/silver.py's zone-known classification logic.

pipeline/silver.py's `_join_zone` runs inside a Lakeflow pipeline: it
references `spark`/`pyspark.sql.functions` at runtime and, consistent with
how this repository already treats every other file under `pipeline/`,
cannot be imported or unit tested outside a live Spark session. This
module mirrors the exact same per-row decision in plain Python so it can
be unit tested directly. Keep `UNKNOWN_ZONE_MARKERS` and the
matched/semantic-unknown logic here in sync by hand with
`pipeline/silver.py`'s Spark column expressions.
"""

from __future__ import annotations

# TLC's own semantic "not a real zone" placeholder values, observed
# verbatim in the official taxi_zone_lookup.csv:
#   LocationID 264 -> Borough "Unknown", Zone "N/A"
#   LocationID 265 -> Borough "N/A",     Zone "Outside of NYC"
# Matched case-insensitively against both Borough and Zone.
UNKNOWN_ZONE_MARKERS = {"unknown", "n/a", "na"}


def is_semantic_unknown(borough: str | None, zone: str | None) -> bool:
    """True if the lookup's own Borough/Zone values are themselves a placeholder.

    A location can be genuinely present in the lookup file (a "matched"
    join) and still not represent a real, known zone -- 264 and 265 always
    join successfully but are not real zones.
    """
    normalized_borough = (borough or "").strip().lower()
    normalized_zone = (zone or "").strip().lower()
    return normalized_borough in UNKNOWN_ZONE_MARKERS or normalized_zone in UNKNOWN_ZONE_MARKERS


def classify_zone(matched: bool, borough: str | None, zone: str | None) -> tuple[bool, str, str]:
    """Return (zone_known, display_borough, display_zone) for one pickup/dropoff location.

    `zone_known` requires both a successful lookup match AND that the
    matched entry is not itself a TLC semantic-unknown placeholder. Either
    failure mode normalizes both display values to the literal string
    "UNKNOWN" -- the row is always retained by the caller, never dropped.
    """
    zone_known = matched and not is_semantic_unknown(borough, zone)
    if zone_known:
        return True, borough, zone
    return False, "UNKNOWN", "UNKNOWN"
