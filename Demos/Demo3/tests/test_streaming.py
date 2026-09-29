from __future__ import annotations

from unittest.mock import Mock

import pytest

from urbanflow.streaming import (
    REQUIRED_EVENT_FIELDS,
    await_bounded_completion,
    event_hubs_kafka_options,
    isolated_stream_paths,
    redacted_kafka_options,
    station_status_schema,
)


def test_kafka_options_use_event_hubs_protocol_and_bounded_rate() -> None:
    fake_connection_string = ";".join(
        ["Endpoint=sb://example/", "SharedAccessKeyName=test", "SharedAccess" + "Key=value"]
    )
    options = event_hubs_kafka_options(
        namespace="namespace",
        connection_string=fake_connection_string,
        event_hub_name="parvinbadalov_evh",
        consumer_group="parvinbadalov",
        max_offsets_per_trigger=250,
    )
    assert options["kafka.bootstrap.servers"] == "namespace.servicebus.windows.net:9093"
    assert options["kafka.security.protocol"] == "SASL_SSL"
    assert options["kafka.sasl.mechanism"] == "PLAIN"
    assert options["maxOffsetsPerTrigger"] == "250"
    assert options["startingOffsets"] == "latest"
    # These two options are what keep a run inside our OWN Event Hub and our OWN
    # consumer group on a namespace shared with other students. Without asserting
    # them, switching to "$Default" or another hub would not fail any test.
    assert options["subscribe"] == "parvinbadalov_evh"
    assert options["kafka.group.id"] == "parvinbadalov"

    safe = redacted_kafka_options(options)
    assert safe["kafka.sasl.jaas.config"] == "[REDACTED]"
    assert fake_connection_string not in str(safe)


def test_kafka_options_reject_missing_secret() -> None:
    with pytest.raises(ValueError, match="connection string"):
        event_hubs_kafka_options(
            namespace="namespace",
            connection_string="",
            event_hub_name="hub",
            consumer_group="group",
        )


def test_station_status_schema_is_explicit() -> None:
    fields = {field.name: field for field in station_status_schema().fields}
    assert set(REQUIRED_EVENT_FIELDS).issubset(fields)
    assert all(fields[name].nullable is False for name in REQUIRED_EVENT_FIELDS)


def test_stream_paths_are_isolated_to_the_configured_volume() -> None:
    paths = isolated_stream_paths(
        volume_root="/Volumes/dbr_dev/parvinbadalov_urbanflow/urbanflow_landing",
        checkpoint_subpath="checkpoints/station_status",
        report_subpath="reports/first_streaming_test",
        execution_id="urbanflow-20260928T210000Z-abcd1234",
    )
    assert paths.checkpoint.endswith("/checkpoints/station_status")
    assert paths.report.endswith(".bronze.json")
    assert paths.report.startswith("/Volumes/dbr_dev/parvinbadalov_urbanflow/")


def test_stream_paths_reject_directory_escape() -> None:
    with pytest.raises(ValueError, match="may not contain"):
        isolated_stream_paths(
            volume_root="/Volumes/dbr_dev/schema/volume",
            checkpoint_subpath="../other-project",
            report_subpath="reports",
            execution_id="run-1",
        )


def test_bounded_completion_records_success() -> None:
    query = Mock()
    query.awaitTermination.return_value = True

    result = await_bounded_completion(query, timeout_seconds=15)

    assert result == {"terminated": True, "timed_out": False, "timeout_seconds": 15}
    query.awaitTermination.assert_called_once_with(15)


def test_bounded_completion_records_timeout_without_claiming_success() -> None:
    query = Mock()
    query.awaitTermination.return_value = False

    result = await_bounded_completion(query, timeout_seconds=15)

    assert result == {"terminated": False, "timed_out": True, "timeout_seconds": 15}


def test_bounded_completion_rejects_non_positive_timeout() -> None:
    with pytest.raises(ValueError, match="positive"):
        await_bounded_completion(Mock(), timeout_seconds=0)


EXPECTED_BRONZE_COLUMNS = (
    # the 16 fields of the explicit event contract, flattened out of the JSON struct
    "event_id",
    "execution_id",
    "station_id",
    "num_bikes_available",
    "num_docks_available",
    "num_bikes_disabled",
    "num_docks_disabled",
    "num_ebikes_available",
    "is_installed",
    "is_renting",
    "is_returning",
    "last_reported",
    "source_last_updated",
    "collected_at",
    "source_url",
    "gbfs_version",
    # Kafka lineage, so a Bronze row can always be traced back to its message
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    # ingestion bookkeeping
    "ingested_at",
    "raw_json",
    "parse_error",
)


@pytest.fixture(scope="module")
def spark_session():
    """A tiny local Spark session used only to ANALYZE query plans, never to run data."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.mark.spark
def test_parse_station_events_plan_resolves_and_keeps_kafka_lineage(spark_session) -> None:
    """Regression test for a real defect that would have crashed the first live run.

    parse_station_events used to select "kafka_*". Spark expands only "*" and
    "<struct>.*", so that prefix was read as a literal column name and the whole
    Bronze write failed analysis with UNRESOLVED_COLUMN before a single row moved.
    Resolving the plan is enough to catch it, so this test analyses only and never
    starts a Python worker - which keeps it fast and avoids executing any data.
    """
    from urbanflow.streaming import parse_station_events

    kafka_shaped = spark_session.createDataFrame(
        [], "value binary, topic string, partition int, offset long, timestamp timestamp"
    )

    # .columns forces analysis; it raised AnalysisException before the fix.
    columns = parse_station_events(kafka_shaped).columns

    assert tuple(columns) == EXPECTED_BRONZE_COLUMNS
    for lineage_column in ("kafka_topic", "kafka_partition", "kafka_offset"):
        assert lineage_column in columns
    assert not any("*" in column for column in columns)
