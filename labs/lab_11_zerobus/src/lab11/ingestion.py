"""Zerobus transport: open a stream, send batches, wait for durability acknowledgments.

A batch counts as delivered only after ``wait_for_offset`` returns for its last offset - the
server's durability acknowledgment. Enqueuing a record locally proves nothing.

Retry policy:

* ``NonRetriableException`` (bad credentials, missing table, schema mismatch): fail at once.
* Any other ``ZerobusException``: close the stream, back off exponentially, open a new stream and
  re-send the whole batch, up to ``max_attempts``. Records the server had already made durable are
  then written twice - this is at-least-once delivery, and Silver deduplicates by ``event_id``.

The SDK is imported lazily, so dry runs and tests need neither the package nor a network.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from lab11.config import Credentials, Target, TransportSettings
from lab11.producer import Batch

LOG = logging.getLogger("lab11.ingestion")
APPLICATION_NAME = "lab11-zerobus/0.1"


class IngestionError(RuntimeError):
    """Delivery did not complete; the message says which batch and why."""


class PermanentIngestionError(IngestionError):
    """The server refused the request in a way retrying cannot fix."""


class RetriesExhaustedError(IngestionError):
    """Transient failures persisted for every allowed attempt."""


@dataclass(frozen=True)
class BatchResult:
    batch_id: str
    delivery: str
    records: int
    attempts: int
    acknowledged_offset: int
    ack_seconds: float


@dataclass
class DeliveryReport:
    endpoint: str
    table: str
    batches: list[BatchResult] = field(default_factory=list)

    @property
    def records_acknowledged(self) -> int:
        return sum(b.records for b in self.batches)

    @property
    def records_transmitted(self) -> int:
        """Records handed to the SDK, counting every attempt (retries can add duplicates)."""
        return sum(b.records * b.attempts for b in self.batches)

    @property
    def retries(self) -> int:
        return sum(b.attempts - 1 for b in self.batches)

    def to_dict(self) -> dict[str, object]:
        return {
            "endpoint": self.endpoint,
            "table": self.table,
            "records_acknowledged": self.records_acknowledged,
            "records_transmitted": self.records_transmitted,
            "retries": self.retries,
            "batches": [b.__dict__ for b in self.batches],
        }


def _sdk_types():
    from zerobus.sdk.shared import (
        NonRetriableException,
        StreamConfigurationOptions,
        TableProperties,
        ZerobusException,
    )
    from zerobus.sdk.sync import ZerobusSdk

    return (
        ZerobusSdk,
        TableProperties,
        StreamConfigurationOptions,
        ZerobusException,
        NonRetriableException,
    )


class ZerobusSink:
    """Sends planned batches to one Unity Catalog table through Zerobus Ingest."""

    def __init__(
        self,
        target: Target,
        credentials: Credentials,
        transport: TransportSettings,
        *,
        sdk_types: Callable[[], tuple] = _sdk_types,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._target = target
        self._credentials = credentials
        self._transport = transport
        self._sdk_types = sdk_types
        self._sleep = sleep
        self._monotonic = monotonic

    def send(self, batches: Sequence[Batch]) -> DeliveryReport:
        sdk_cls, table_props, options_cls, retryable, permanent = self._sdk_types()
        sdk = sdk_cls(self._target.zerobus_endpoint, self._target.workspace_url, APPLICATION_NAME)
        options = options_cls(
            flush_timeout_ms=self._transport.flush_timeout_seconds * 1000,
            server_lack_of_ack_timeout_ms=self._transport.server_ack_timeout_seconds * 1000,
        )
        report = DeliveryReport(self._target.zerobus_endpoint, self._target.bronze_table)
        stream = None

        def open_stream():
            LOG.info(
                "opening stream table=%s endpoint=%s",
                self._target.bronze_table,
                self._target.zerobus_endpoint,
            )
            return sdk.create_stream(
                self._credentials.client_id,
                self._credentials.client_secret,
                table_props(self._target.bronze_table),
                options,
            )

        try:
            for batch in batches:
                attempt = 0
                while True:
                    attempt += 1
                    started = self._monotonic()
                    try:
                        if stream is None:
                            stream = open_stream()
                        offset = stream.ingest_records_offset(list(batch.records))
                        if offset is None:
                            raise IngestionError(
                                f"batch {batch.batch_id}: the SDK returned no offset"
                            )
                        stream.wait_for_offset(offset)  # durability acknowledgment
                    except permanent as exc:
                        raise PermanentIngestionError(
                            f"batch {batch.batch_id} refused (not retried): {exc}"
                        ) from exc
                    except retryable as exc:
                        LOG.warning(
                            "batch=%s attempt=%d transient failure: %s",
                            batch.batch_id,
                            attempt,
                            exc,
                        )
                        self._close_quietly(stream)
                        stream = None
                        if attempt >= self._transport.max_attempts:
                            raise RetriesExhaustedError(
                                f"batch {batch.batch_id} failed after {attempt} attempts: {exc}"
                            ) from exc
                        self._sleep(self._transport.backoff(attempt))
                        continue
                    elapsed = self._monotonic() - started
                    report.batches.append(
                        BatchResult(
                            batch.batch_id,
                            batch.delivery,
                            len(batch.records),
                            attempt,
                            offset,
                            round(elapsed, 3),
                        )
                    )
                    LOG.info(
                        "batch=%s records=%d acknowledged offset=%s attempts=%d ack_seconds=%.3f",
                        batch.batch_id,
                        len(batch.records),
                        offset,
                        attempt,
                        elapsed,
                    )
                    break
            if stream is not None:
                stream.close()  # flushes; every batch is already acknowledged
                stream = None
        finally:
            self._close_quietly(stream)
        return report

    @staticmethod
    def _close_quietly(stream) -> None:
        if stream is None:
            return
        try:
            stream.close()
        except Exception as exc:  # the original failure matters more than a failed close
            LOG.warning("ignoring error while closing a failed stream: %s", exc)
