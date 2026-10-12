"""Offline transport tests with a fake Zerobus SDK. They prove control flow, not live delivery."""

from __future__ import annotations

import pytest

from lab11.config import Credentials
from lab11.ingestion import (
    IngestionError,
    PermanentIngestionError,
    RetriesExhaustedError,
    ZerobusSink,
)


class FakeZerobusException(Exception):
    """Stands in for zerobus.sdk.shared.ZerobusException (retryable)."""


class FakeNonRetriable(FakeZerobusException):
    """Stands in for NonRetriableException, which subclasses ZerobusException in the SDK."""


class FakeOptions:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class FakeStream:
    def __init__(self, sdk):
        self.sdk = sdk
        self.offset = -1
        self.closed = False

    def ingest_records_offset(self, payloads):
        failure = self.sdk.next_failure("ingest")
        if failure:
            raise failure
        self.sdk.received.extend(payloads)
        self.offset += 1  # SDK 1.10 returns one offset per ingest_records_offset call
        return None if self.sdk.return_no_offset else self.offset

    def wait_for_offset(self, offset):
        failure = self.sdk.next_failure("ack")
        if failure:
            raise failure
        self.sdk.acknowledged.append(offset)

    def close(self):
        self.closed = True
        self.sdk.closed += 1
        if self.sdk.fail_close:
            raise FakeZerobusException("close failed")


class FakeSdk:
    def __init__(self, failures=None):
        self.failures = list(failures or [])  # (stage, exception) consumed in order
        self.streams = []
        self.received = []
        self.acknowledged = []
        self.closed = 0
        self.fail_close = False
        self.return_no_offset = False
        self.created_with = []

    def next_failure(self, stage):
        if self.failures and self.failures[0][0] == stage:
            return self.failures.pop(0)[1]
        return None

    # ZerobusSdk(host, unity_catalog_url, application_name)
    def __call__(self, host, unity_catalog_url, application_name):
        self.host, self.unity_catalog_url = host, unity_catalog_url
        return self

    def create_stream(self, client_id, client_secret, table_properties, options):
        failure = self.next_failure("open")
        if failure:
            raise failure
        self.created_with.append((client_id, table_properties, options.kwargs))
        stream = FakeStream(self)
        self.streams.append(stream)
        return stream


def make_sink(target, config, fake, sleeps):
    return ZerobusSink(
        target,
        Credentials("client-id-1234", "never-logged"),
        config.transport,
        sdk_types=lambda: (
            fake,
            lambda name: f"table:{name}",
            FakeOptions,
            FakeZerobusException,
            FakeNonRetriable,
        ),
        sleep=sleeps.append,
    )


def test_every_batch_waits_for_its_durability_acknowledgment(target, config, plan):
    fake, sleeps = FakeSdk(), []
    report = make_sink(target, config, fake, sleeps).send(plan.batches)
    assert fake.host == target.zerobus_endpoint and fake.unity_catalog_url == target.workspace_url
    assert fake.created_with[0][1] == f"table:{target.bronze_table}"
    assert (
        fake.created_with[0][2]["flush_timeout_ms"] == config.transport.flush_timeout_seconds * 1000
    )
    assert fake.acknowledged == [0, 1, 2, 3]  # one offset per batch, as observed live
    assert report.records_acknowledged == 30 and report.retries == 0
    assert len(fake.streams) == 1 and fake.closed == 1
    assert fake.received == plan.records


def test_a_transient_failure_reopens_the_stream_and_resends_the_batch(target, config, plan):
    fake, sleeps = FakeSdk([("ack", FakeZerobusException("connection reset"))]), []
    report = make_sink(target, config, fake, sleeps).send(plan.batches)
    assert sleeps == [1.0]
    assert len(fake.streams) == 2 and fake.streams[0].closed
    assert report.batches[0].attempts == 2
    assert report.records_acknowledged == 30
    # The first batch went out twice: at-least-once delivery, deduplicated downstream.
    assert report.records_transmitted == 40
    assert len(fake.received) == 40


def test_backoff_grows_until_attempts_are_exhausted(target, config, plan):
    fake = FakeSdk([("ingest", FakeZerobusException("unavailable"))] * 3)
    sleeps = []
    with pytest.raises(RetriesExhaustedError, match="after 3 attempts"):
        make_sink(target, config, fake, sleeps).send(plan.batches)
    assert sleeps == [1.0, 2.0]
    assert all(s.closed for s in fake.streams)


def test_a_permanent_error_is_not_retried(target, config, plan):
    fake, sleeps = FakeSdk([("ingest", FakeNonRetriable("schema mismatch: unknown field"))]), []
    with pytest.raises(PermanentIngestionError, match="not retried"):
        make_sink(target, config, fake, sleeps).send(plan.batches)
    assert sleeps == [] and len(fake.streams) == 1 and fake.streams[0].closed
    assert fake.acknowledged == []


def test_bad_credentials_fail_clearly_when_the_stream_opens(target, config, plan):
    fake, sleeps = FakeSdk([("open", FakeNonRetriable("401 invalid_client"))]), []
    with pytest.raises(PermanentIngestionError, match="401 invalid_client"):
        make_sink(target, config, fake, sleeps).send(plan.batches)
    assert fake.received == []


def test_a_missing_offset_is_an_error_not_a_success(target, config, plan):
    fake, sleeps = FakeSdk(), []
    fake.return_no_offset = True
    with pytest.raises(IngestionError, match="no offset"):
        make_sink(target, config, fake, sleeps).send(plan.batches)
    assert fake.acknowledged == []


def test_a_failing_close_does_not_hide_the_original_error(target, config, plan):
    fake, sleeps = FakeSdk([("ingest", FakeNonRetriable("table not found"))]), []
    fake.fail_close = True
    with pytest.raises(PermanentIngestionError, match="table not found"):
        make_sink(target, config, fake, sleeps).send(plan.batches)


def test_the_secret_is_never_logged(target, config, plan, caplog):
    fake, sleeps = FakeSdk([("ack", FakeZerobusException("reset"))]), []
    with caplog.at_level("DEBUG"):
        make_sink(target, config, fake, sleeps).send(plan.batches)
    assert "never-logged" not in caplog.text
    assert "acknowledged" in caplog.text
