"""Tests for the reusable Delta MERGE helpers."""

from datetime import datetime
import uuid

import pytest
from pyspark.sql import SparkSession

from src.merge_utils import (
    add_record_hash,
    classify_merge_actions,
    deduplicate_latest,
    merge_upsert,
)


@pytest.fixture(scope="session")
def spark():
    """Reuse the active Databricks Spark session."""

    active_session = SparkSession.getActiveSession()

    if active_session is not None:
        return active_session

    return (
        SparkSession.builder
        .appName("lab04-merge-tests")
        .getOrCreate()
    )


@pytest.fixture
def target_table(spark):
    """Create a unique Delta table name and remove it after each test."""

    table_name = (
        f"lab04_merge_test_{uuid.uuid4().hex[:12]}"
    )

    spark.sql(
        f"DROP TABLE IF EXISTS `{table_name}`"
    )

    yield table_name

    spark.sql(
        f"DROP TABLE IF EXISTS `{table_name}`"
    )


def build_product_events(spark):
    """Build duplicate product events for deterministic tests."""

    rows = [
        (
            "85123A",
            "WHITE HANGING HEART",
            2.55,
            datetime(2026, 8, 10, 10, 0, 0),
            1,
        ),
        (
            "85123A",
            "UPDATED HANGING HEART",
            3.25,
            datetime(2026, 8, 10, 11, 0, 0),
            2,
        ),
        (
            "71053",
            "WHITE METAL LANTERN",
            3.39,
            datetime(2026, 8, 10, 10, 30, 0),
            1,
        ),
    ]

    schema = """
        stock_code STRING,
        description STRING,
        unit_price DOUBLE,
        event_timestamp TIMESTAMP,
        sequence_number LONG
    """

    return spark.createDataFrame(rows, schema)


def test_add_record_hash_is_deterministic(spark):
    source_df = spark.createDataFrame(
        [
            ("85123A", "WHITE HANGING HEART", 2.55),
        ],
        """
        stock_code STRING,
        description STRING,
        unit_price DOUBLE
        """,
    )

    first_hash = (
        add_record_hash(
            source_df,
            ["stock_code", "description", "unit_price"],
        )
        .select("source_record_hash")
        .first()[0]
    )

    second_hash = (
        add_record_hash(
            source_df,
            ["stock_code", "description", "unit_price"],
        )
        .select("source_record_hash")
        .first()[0]
    )

    assert first_hash is not None
    assert first_hash == second_hash


def test_record_hash_changes_with_tracked_value(spark):
    source_df = spark.createDataFrame(
        [
            ("85123A", "WHITE HANGING HEART", 2.55),
            ("85123A", "WHITE HANGING HEART", 3.25),
        ],
        """
        stock_code STRING,
        description STRING,
        unit_price DOUBLE
        """,
    )

    hashed_df = add_record_hash(
        source_df,
        ["stock_code", "description", "unit_price"],
    )

    distinct_hashes = (
        hashed_df
        .select("source_record_hash")
        .distinct()
        .count()
    )

    assert distinct_hashes == 2


def test_deduplicate_latest_keeps_newest_record(spark):
    source_df = build_product_events(spark)

    deduplicated_df = deduplicate_latest(
        source_df,
        business_keys=["stock_code"],
        order_columns=[
            "event_timestamp",
            "sequence_number",
        ],
    )

    assert deduplicated_df.count() == 2

    latest_product = (
        deduplicated_df
        .filter("stock_code = '85123A'")
        .select(
            "description",
            "unit_price",
            "sequence_number",
        )
        .first()
    )

    assert latest_product["description"] == (
        "UPDATED HANGING HEART"
    )
    assert latest_product["unit_price"] == 3.25
    assert latest_product["sequence_number"] == 2
def build_initial_target(spark):
    """Create the starting target state."""

    target_df = spark.createDataFrame(
        [
            (
                "85123A",
                "WHITE HANGING HEART",
                2.55,
                datetime(2026, 8, 10, 10, 0, 0),
                1,
            ),
            (
                "71053",
                "WHITE METAL LANTERN",
                3.39,
                datetime(2026, 8, 10, 10, 30, 0),
                1,
            ),
        ],
        """
        stock_code STRING,
        description STRING,
        unit_price DOUBLE,
        event_timestamp TIMESTAMP,
        sequence_number LONG
        """,
    )

    return add_record_hash(
        target_df,
        tracked_columns=[
            "description",
            "unit_price",
        ],
    )


def build_change_batch(spark):
    """Create one update, one unchanged row and one insert."""

    change_df = spark.createDataFrame(
        [
            (
                "85123A",
                "UPDATED HANGING HEART",
                3.25,
                datetime(2026, 8, 10, 11, 0, 0),
                2,
            ),
            (
                "71053",
                "WHITE METAL LANTERN",
                3.39,
                datetime(2026, 8, 10, 10, 30, 0),
                1,
            ),
            (
                "84029G",
                "KNITTED UNION FLAG",
                3.75,
                datetime(2026, 8, 10, 11, 30, 0),
                1,
            ),
        ],
        """
        stock_code STRING,
        description STRING,
        unit_price DOUBLE,
        event_timestamp TIMESTAMP,
        sequence_number LONG
        """,
    )

    return add_record_hash(
        change_df,
        tracked_columns=[
            "description",
            "unit_price",
        ],
    )


def collect_merge_actions(classified_df):
    """Return action counts as a standard Python dictionary."""

    return {
        row["merge_action"]: row["count"]
        for row in (
            classified_df
            .groupBy("merge_action")
            .count()
            .collect()
        )
    }


def collect_target_state(spark, target_table):
    """Return a deterministic representation of the target table."""

    rows = (
        spark.table(target_table)
        .select(
            "stock_code",
            "description",
            "unit_price",
            "event_timestamp",
            "sequence_number",
            "source_record_hash",
        )
        .orderBy("stock_code")
        .collect()
    )

    return [
        tuple(row[column] for column in row.__fields__)
        for row in rows
    ]


def test_classify_merge_actions(spark, target_table):
    initial_df = build_initial_target(spark)

    (
        initial_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

    change_df = build_change_batch(spark)

    classified_df = classify_merge_actions(
        change_df,
        target_table,
        business_keys=["stock_code"],
    )

    action_counts = collect_merge_actions(classified_df)

    assert action_counts == {
        "INSERT": 1,
        "UPDATE": 1,
        "UNCHANGED": 1,
    }


def test_merge_upsert_applies_expected_changes(
    spark,
    target_table,
):
    initial_df = build_initial_target(spark)

    (
        initial_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

    change_df = build_change_batch(spark)

    merge_upsert(
        change_df,
        target_table,
        business_keys=["stock_code"],
    )

    result_df = spark.table(target_table)

    assert result_df.count() == 3

    updated_product = (
        result_df
        .filter("stock_code = '85123A'")
        .select(
            "description",
            "unit_price",
            "sequence_number",
        )
        .first()
    )

    assert updated_product["description"] == (
        "UPDATED HANGING HEART"
    )
    assert updated_product["unit_price"] == 3.25
    assert updated_product["sequence_number"] == 2

    inserted_product_count = (
        result_df
        .filter("stock_code = '84029G'")
        .count()
    )

    assert inserted_product_count == 1


def test_merge_replay_is_idempotent(
    spark,
    target_table,
):
    initial_df = build_initial_target(spark)

    (
        initial_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

    change_df = build_change_batch(spark)

    # First execution applies one update and one insert.
    merge_upsert(
        change_df,
        target_table,
        business_keys=["stock_code"],
    )

    state_after_first_run = collect_target_state(
        spark,
        target_table,
    )

    count_after_first_run = spark.table(
        target_table
    ).count()

    # Replay exactly the same source batch.
    merge_upsert(
        change_df,
        target_table,
        business_keys=["stock_code"],
    )

    state_after_replay = collect_target_state(
        spark,
        target_table,
    )

    count_after_replay = spark.table(
        target_table
    ).count()

    assert count_after_first_run == 3
    assert count_after_replay == 3
    assert state_after_replay == state_after_first_run

    replay_classification_df = classify_merge_actions(
        change_df,
        target_table,
        business_keys=["stock_code"],
    )

    replay_action_counts = collect_merge_actions(
        replay_classification_df
    )

    assert replay_action_counts == {
        "UNCHANGED": 3,
    }    
