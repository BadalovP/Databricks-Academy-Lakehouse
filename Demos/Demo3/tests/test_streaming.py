from __future__ import annotations

import pytest

from urbanflow.streaming import (
    REQUIRED_EVENT_FIELDS,
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
