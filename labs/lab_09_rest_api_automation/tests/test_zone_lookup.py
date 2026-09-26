"""Tests for the zone-known classification mirrored from pipeline/silver.py.

The defect this guards against: `_zone_known` was previously true for any
successful lookup join, so TLC's own semantic placeholder rows (264, 265)
were reported as "known" zones merely because they exist in
taxi_zone_lookup.csv.
"""

from lab09.zone_lookup import classify_zone, is_semantic_unknown

# --- normal, real, matched zone ---------------------------------------------


def test_classify_zone_normal_matched_location_is_known():
    zone_known, borough, zone = classify_zone(
        matched=True, borough="Manhattan", zone="Yorkville West"
    )

    assert zone_known is True
    assert borough == "Manhattan"
    assert zone == "Yorkville West"


# --- unmatched location (no row in the lookup at all) -----------------------


def test_classify_zone_unmatched_location_is_unknown():
    zone_known, borough, zone = classify_zone(matched=False, borough=None, zone=None)

    assert zone_known is False
    assert borough == "UNKNOWN"
    assert zone == "UNKNOWN"


# --- TLC's own special Unknown/N/A lookup rows (264, 265) -------------------
# Values below are the real rows from the official taxi_zone_lookup.csv,
# confirmed by downloading it directly rather than assumed.


def test_classify_zone_tlc_location_264_is_flagged_unknown_though_matched():
    zone_known, borough, zone = classify_zone(matched=True, borough="Unknown", zone="N/A")

    assert zone_known is False
    assert borough == "UNKNOWN"
    assert zone == "UNKNOWN"


def test_classify_zone_tlc_location_265_is_flagged_unknown_though_matched():
    zone_known, borough, zone = classify_zone(matched=True, borough="N/A", zone="Outside of NYC")

    assert zone_known is False
    assert borough == "UNKNOWN"
    assert zone == "UNKNOWN"


def test_classify_zone_semantic_unknown_check_is_case_insensitive_and_trims_whitespace():
    zone_known, borough, zone = classify_zone(matched=True, borough=" UNKNOWN ", zone="some zone")

    assert zone_known is False
    assert borough == "UNKNOWN"
    assert zone == "UNKNOWN"


def test_classify_zone_does_not_flag_a_real_borough_named_similarly():
    # Guard against an overly broad substring match: "Unknown" as a whole
    # value is a marker, but it must not accidentally match unrelated text.
    zone_known, borough, zone = classify_zone(matched=True, borough="Queens", zone="Unknownville")

    assert zone_known is True
    assert borough == "Queens"
    assert zone == "Unknownville"


def test_is_semantic_unknown_matches_known_tlc_markers():
    assert is_semantic_unknown("Unknown", "N/A") is True
    assert is_semantic_unknown("N/A", "Outside of NYC") is True
    assert is_semantic_unknown("Manhattan", "Yorkville West") is False


def test_is_semantic_unknown_handles_missing_values_without_raising():
    # A null borough/zone normalizes to "" via `(value or "").strip().lower()`,
    # which is not itself one of the TLC marker strings -- this must not
    # raise, and real reference data never actually produces this case
    # (a matched row always has non-null borough/zone).
    assert is_semantic_unknown(None, None) is False
