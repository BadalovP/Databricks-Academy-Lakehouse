"""SCD1 and SCD2 tests for the station dimension.

Every change scenario below is SYNTHETIC and labelled as such. UrbanFlow has one real
GBFS snapshot so far, so it has not yet observed a station attribute change over time.
These scenarios exercise the same functions that will run against real snapshots once a
second one exists; they are teaching fixtures, not observed history.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from urbanflow.dimensions import (
    SCD2_TRACKED_COLUMNS,
    SYNTHETIC_CHANGE_NOTE,
    initial_scd2_dimension,
    scd1_overwrite,
    scd2_apply,
    scd2_audit,
)

pytestmark = pytest.mark.spark

UUID_A = "66dd9fa6-0aca-11e7-82f6-3863bb44ef7c"
UUID_B = "0138452a-b9f4-4aee-80b0-7fae9a122ffe"
UUID_C = "11111111-2222-3333-4444-555555555555"
DAY1 = "2026-09-29 00:00:00"
DAY2 = "2026-09-30 00:00:00"


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-dimension-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _dimension(spark, rows: str):
    """SYNTHETIC dimension snapshot. See module docstring."""
    return spark.sql(
        f"SELECT * FROM VALUES {rows} "
        "AS t(station_id, station_name, capacity, latitude, longitude, region_id)"
    )


def test_the_synthetic_label_exists_so_demos_cannot_be_mistaken_for_history() -> None:
    assert "SYNTHETIC" in SYNTHETIC_CHANGE_NOTE
    assert "not an observed" in SYNTHETIC_CHANGE_NOTE


def test_scd1_overwrites_in_place_and_keeps_no_history(spark_session) -> None:
    """SYNTHETIC: a station is renamed. Type 1 keeps only the corrected value."""
    existing = _dimension(spark_session, f"('{UUID_A}','Old Name',55,40.76,-73.97,'71')")
    incoming = _dimension(spark_session, f"('{UUID_A}','Corrected Name',60,40.76,-73.97,'71')")

    result = scd1_overwrite(existing, incoming).collect()

    assert len(result) == 1
    assert result[0]["station_name"] == "Corrected Name"
    assert result[0]["capacity"] == 60


def test_scd1_keeps_stations_absent_from_the_incoming_snapshot(spark_session) -> None:
    """A station missing from one feed read must not vanish from the dimension."""
    existing = _dimension(
        spark_session,
        f"('{UUID_A}','A',10,40.1,-73.1,'71'), ('{UUID_B}','B',20,40.2,-73.2,'71')",
    )
    incoming = _dimension(spark_session, f"('{UUID_A}','A renamed',10,40.1,-73.1,'71')")

    result = {r["station_id"]: r for r in scd1_overwrite(existing, incoming).collect()}

    assert result[UUID_A]["station_name"] == "A renamed"
    assert result[UUID_B]["station_name"] == "B"


def test_scd2_seeds_a_current_row_per_station(spark_session) -> None:
    dim = _dimension(spark_session, f"('{UUID_A}','A',55,40.76,-73.97,'71')")

    seeded = initial_scd2_dimension(dim, effective_from=DAY1).collect()

    assert len(seeded) == 1
    row = seeded[0]
    assert row["is_current"] is True
    assert row["valid_to"] is None
    assert str(row["valid_from"]).startswith("2026-09-29")
    for column in SCD2_TRACKED_COLUMNS:
        assert column in row.asDict()


def test_scd2_closes_the_old_row_and_opens_a_new_one_on_change(spark_session) -> None:
    """SYNTHETIC: capacity grows from 55 to 60 on day 2."""
    day1 = initial_scd2_dimension(
        _dimension(spark_session, f"('{UUID_A}','A',55,40.76,-73.97,'71')"),
        effective_from=DAY1,
    )
    day2 = _dimension(spark_session, f"('{UUID_A}','A',60,40.76,-73.97,'71')")

    result = scd2_apply(day1, day2, effective_from=DAY2).collect()

    assert len(result) == 2
    closed = [r for r in result if not r["is_current"]][0]
    current = [r for r in result if r["is_current"]][0]
    assert closed["capacity"] == 55
    assert str(closed["valid_to"]).startswith("2026-09-30")
    assert current["capacity"] == 60
    assert current["valid_to"] is None
    assert str(current["valid_from"]).startswith("2026-09-30")


def test_scd2_leaves_an_unchanged_station_with_exactly_one_current_row(spark_session) -> None:
    """Rerun safety: applying the same snapshot twice must not create versions."""
    day1 = initial_scd2_dimension(
        _dimension(spark_session, f"('{UUID_A}','A',55,40.76,-73.97,'71')"),
        effective_from=DAY1,
    )
    unchanged = _dimension(spark_session, f"('{UUID_A}','A',55,40.76,-73.97,'71')")

    once = scd2_apply(day1, unchanged, effective_from=DAY2)
    twice = scd2_apply(once, unchanged, effective_from=DAY2)

    assert once.count() == 1
    assert twice.count() == 1
    assert once.collect()[0]["valid_from"] == day1.collect()[0]["valid_from"]


def test_scd2_adds_a_brand_new_station_as_current(spark_session) -> None:
    """SYNTHETIC: a new station appears in the feed on day 2."""
    day1 = initial_scd2_dimension(
        _dimension(spark_session, f"('{UUID_A}','A',55,40.76,-73.97,'71')"),
        effective_from=DAY1,
    )
    day2 = _dimension(
        spark_session,
        f"('{UUID_A}','A',55,40.76,-73.97,'71'), ('{UUID_C}','New Station',12,40.5,-73.5,'71')",
    )

    result = {r["station_id"]: r for r in scd2_apply(day1, day2, effective_from=DAY2).collect()}

    assert len(result) == 2
    assert result[UUID_C]["is_current"] is True
    assert result[UUID_C]["station_name"] == "New Station"
    assert str(result[UUID_C]["valid_from"]).startswith("2026-09-30")


def test_scd2_keeps_a_station_that_disappears_from_the_feed(spark_session) -> None:
    """A station missing from one read is not evidence it was removed, so history stands."""
    day1 = initial_scd2_dimension(
        _dimension(
            spark_session,
            f"('{UUID_A}','A',55,40.76,-73.97,'71'), ('{UUID_B}','B',20,40.2,-73.2,'71')",
        ),
        effective_from=DAY1,
    )
    day2 = _dimension(spark_session, f"('{UUID_A}','A',55,40.76,-73.97,'71')")

    result = {r["station_id"]: r for r in scd2_apply(day1, day2, effective_from=DAY2).collect()}

    assert result[UUID_B]["is_current"] is True
    assert result[UUID_B]["valid_to"] is None


def test_scd2_tracks_a_relocation(spark_session) -> None:
    """SYNTHETIC: a station is physically moved, so coordinates change."""
    day1 = initial_scd2_dimension(
        _dimension(spark_session, f"('{UUID_A}','A',55,40.76,-73.97,'71')"),
        effective_from=DAY1,
    )
    moved = _dimension(spark_session, f"('{UUID_A}','A',55,40.80,-73.90,'71')")

    result = scd2_apply(day1, moved, effective_from=DAY2).collect()

    assert len(result) == 2
    current = [r for r in result if r["is_current"]][0]
    # Spark preserves the SQL literal as Decimal. Compare like with like so this
    # regression test does not force production coordinates through a lossy float cast.
    assert current["latitude"] == pytest.approx(Decimal("40.80"))


def test_scd2_null_safe_comparison_tracks_null_to_value_change(spark_session) -> None:
    day1 = initial_scd2_dimension(
        _dimension(
            spark_session,
            f"('{UUID_A}',CAST(NULL AS STRING),55,40.76,-73.97,'71')",
        ),
        effective_from=DAY1,
    )
    corrected = _dimension(spark_session, f"('{UUID_A}','A',55,40.76,-73.97,'71')")

    result = scd2_apply(day1, corrected, effective_from=DAY2).collect()

    assert len(result) == 2
    assert [row for row in result if row["is_current"]][0]["station_name"] == "A"


def test_scd2_rejects_duplicate_incoming_station_keys(spark_session) -> None:
    day1 = initial_scd2_dimension(
        _dimension(spark_session, f"('{UUID_A}','A',55,40.76,-73.97,'71')"),
        effective_from=DAY1,
    )
    duplicate = _dimension(
        spark_session,
        f"('{UUID_A}','A',55,40.76,-73.97,'71'), " f"('{UUID_A}','Different',60,40.80,-73.90,'71')",
    )

    with pytest.raises(ValueError, match="duplicate station_id"):
        scd2_apply(day1, duplicate, effective_from=DAY2)


def test_scd2_rejects_out_of_order_change(spark_session) -> None:
    day2 = initial_scd2_dimension(
        _dimension(spark_session, f"('{UUID_A}','A',55,40.76,-73.97,'71')"),
        effective_from=DAY2,
    )
    changed = _dimension(spark_session, f"('{UUID_A}','A',60,40.76,-73.97,'71')")

    with pytest.raises(ValueError, match="later than"):
        scd2_apply(day2, changed, effective_from=DAY1)


def test_scd2_audit_passes_on_a_well_formed_dimension(spark_session) -> None:
    day1 = initial_scd2_dimension(
        _dimension(spark_session, f"('{UUID_A}','A',55,40.76,-73.97,'71')"),
        effective_from=DAY1,
    )
    changed = _dimension(spark_session, f"('{UUID_A}','A',60,40.76,-73.97,'71')")

    audit = scd2_audit(scd2_apply(day1, changed, effective_from=DAY2))

    assert audit["total_rows"] == 2
    assert audit["current_rows"] == 1
    assert audit["one_current_row_per_key"] is True
    assert audit["closed_rows_missing_valid_to"] == 0
    assert audit["status"] == "PASS"


def test_scd2_audit_detects_two_current_rows_for_one_station(spark_session) -> None:
    """The audit must be able to FAIL, otherwise it proves nothing.

    Two current rows is the classic broken-merge symptom: queries filtering on
    is_current still return plausible data, so the corruption is easy to miss.
    """
    broken = spark_session.sql(
        f"SELECT * FROM VALUES "
        f"('{UUID_A}','A',55,40.7,-73.9,TIMESTAMP'{DAY1}',CAST(NULL AS TIMESTAMP),true), "
        f"('{UUID_A}','A',60,40.7,-73.9,TIMESTAMP'{DAY2}',CAST(NULL AS TIMESTAMP),true) "
        "AS t(station_id, station_name, capacity, latitude, longitude, "
        "valid_from, valid_to, is_current)"
    )

    audit = scd2_audit(broken)

    assert audit["current_rows"] == 2
    assert audit["distinct_current_keys"] == 1
    assert audit["one_current_row_per_key"] is False
    assert audit["status"] == "FAIL"


def test_scd2_audit_detects_a_key_with_no_current_version(spark_session) -> None:
    broken = spark_session.sql(
        f"SELECT * FROM VALUES "
        f"('{UUID_A}','A',55,40.7,-73.9,TIMESTAMP'{DAY1}',TIMESTAMP'{DAY2}',false) "
        "AS t(station_id, station_name, capacity, latitude, longitude, "
        "valid_from, valid_to, is_current)"
    )

    audit = scd2_audit(broken)

    assert audit["distinct_keys"] == 1
    assert audit["current_rows"] == 0
    assert audit["one_current_row_per_key"] is False
    assert audit["status"] == "FAIL"


def test_scd2_audit_detects_an_invalid_closed_interval(spark_session) -> None:
    broken = spark_session.sql(
        f"SELECT * FROM VALUES "
        f"('{UUID_A}','A',55,40.7,-73.9,TIMESTAMP'{DAY2}',TIMESTAMP'{DAY1}',false), "
        f"('{UUID_A}','A',60,40.7,-73.9,TIMESTAMP'{DAY2}',CAST(NULL AS TIMESTAMP),true) "
        "AS t(station_id, station_name, capacity, latitude, longitude, "
        "valid_from, valid_to, is_current)"
    )

    audit = scd2_audit(broken)

    assert audit["invalid_closed_intervals"] == 1
    assert audit["status"] == "FAIL"
