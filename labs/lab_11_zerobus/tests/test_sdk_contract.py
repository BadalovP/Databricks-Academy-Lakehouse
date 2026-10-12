"""The installed Zerobus SDK exposes exactly the interface ``ZerobusSink`` calls.

Skipped when the SDK is not installed. No network: only signatures and class relationships.
"""

from __future__ import annotations

import inspect

import pytest

pytestmark = pytest.mark.sdk
zerobus_sync = pytest.importorskip("zerobus.sdk.sync")
shared = pytest.importorskip("zerobus.sdk.shared")


def test_sdk_constructor_and_create_stream_signatures():
    init = inspect.signature(zerobus_sync.ZerobusSdk.__init__).parameters
    assert list(init)[1:3] == ["host", "unity_catalog_url"] and "application_name" in init
    create = list(inspect.signature(zerobus_sync.ZerobusSdk.create_stream).parameters)
    assert create[1:5] == ["client_id", "client_secret", "table_properties", "options"]


def test_stream_methods_used_by_the_sink_exist():
    for name in ("ingest_records_offset", "wait_for_offset", "flush", "close"):
        assert callable(getattr(zerobus_sync.ZerobusStream, name))


def test_options_accept_the_timeouts_the_sink_sets():
    options = shared.StreamConfigurationOptions(
        flush_timeout_ms=60_000, server_lack_of_ack_timeout_ms=60_000
    )
    assert options is not None
    assert shared.TableProperties("main.default.t") is not None


def test_non_retriable_is_a_zerobus_exception():
    # The sink catches NonRetriableException first, then ZerobusException.
    assert issubclass(shared.NonRetriableException, shared.ZerobusException)
