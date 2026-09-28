from __future__ import annotations

import pytest

from urbanflow.streaming import event_hubs_kafka_options


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


def test_kafka_options_reject_missing_secret() -> None:
    with pytest.raises(ValueError, match="connection string"):
        event_hubs_kafka_options(
            namespace="namespace",
            connection_string="",
            event_hub_name="hub",
            consumer_group="group",
        )
