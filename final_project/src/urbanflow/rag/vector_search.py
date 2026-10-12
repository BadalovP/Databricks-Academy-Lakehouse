"""Databricks AI Search integration with mutation gates."""

from __future__ import annotations

import datetime
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

UNAVAILABLE = "unavailable"


class AIClient(Protocol):
    def get_endpoint(self, name: str) -> Any: ...

    def create_endpoint(self, *, name: str, endpoint_type: str) -> Any: ...

    def get_index(self, *, endpoint_name: str, index_name: str) -> Any: ...

    def create_delta_sync_index(self, **kwargs: Any) -> Any: ...


@dataclass(frozen=True)
class DeltaSyncIndexSpec:
    endpoint_name: str
    index_name: str
    source_table_name: str
    embedding_model_endpoint_name: str = "databricks-gte-large-en"
    primary_key: str = "chunk_id"
    embedding_source_column: str = "content"
    pipeline_type: str = "TRIGGERED"
    endpoint_type: str = "STANDARD"
    columns_to_sync: tuple[str, ...] = (
        "chunk_id",
        "document_id",
        "source_name",
        "section_heading",
        "section_ref",
        "content",
        "content_hash",
    )

    def as_create_kwargs(self) -> dict[str, Any]:
        if self.pipeline_type != "TRIGGERED":
            raise ValueError("Lab 12 permits only TRIGGERED synchronization.")
        if self.endpoint_type != "STANDARD":
            raise ValueError("Lab 12 defaults to a STANDARD AI Search endpoint.")
        return {
            "endpoint_name": self.endpoint_name,
            "source_table_name": self.source_table_name,
            "index_name": self.index_name,
            "pipeline_type": self.pipeline_type,
            "primary_key": self.primary_key,
            "embedding_source_column": self.embedding_source_column,
            "embedding_model_endpoint_name": self.embedding_model_endpoint_name,
            "columns_to_sync": list(self.columns_to_sync),
        }


class VectorSearchManager:
    def __init__(self, client: AIClient | None = None) -> None:
        self._client = client

    @property
    def client(self) -> AIClient:
        if self._client is None:
            try:
                from databricks.ai_search.client import AISearchClient
            except ImportError as exc:
                raise RuntimeError(
                    "Install the 'databricks' extra for live AI Search access."
                ) from exc
            self._client = AISearchClient()
        return self._client

    def inspect(self, spec: DeltaSyncIndexSpec) -> dict[str, Any]:
        """Read metadata only; missing resources are returned as absent."""
        report: dict[str, Any] = {"spec": spec.as_create_kwargs()}
        try:
            report["endpoint"] = self.client.get_endpoint(name=spec.endpoint_name)
            report["endpoint_status"] = "present"
        except Exception as exc:  # SDK exception types vary by release
            report["endpoint_error"] = f"{type(exc).__name__}: {exc}"
            report["endpoint_status"] = _status(exc)
        try:
            index = self.client.get_index(
                endpoint_name=spec.endpoint_name, index_name=spec.index_name
            )
            report["index"] = index.describe() if hasattr(index, "describe") else index
            report["index_status"] = "present"
        except Exception as exc:
            report["index_error"] = f"{type(exc).__name__}: {exc}"
            report["index_status"] = _status(exc)
        return report

    def ensure(self, spec: DeltaSyncIndexSpec, *, allow_create: bool = False) -> Any:
        """Get the index or create missing endpoint/index only after explicit approval."""
        try:
            return self.client.get_index(
                endpoint_name=spec.endpoint_name, index_name=spec.index_name
            )
        except Exception:
            if not allow_create:
                raise PermissionError(
                    "AI Search creation is disabled; inspect the plan and pass allow_create=True "
                    "only after cost and permission approval."
                ) from None
        try:
            self.client.get_endpoint(name=spec.endpoint_name)
        except Exception:
            self.client.create_endpoint(name=spec.endpoint_name, endpoint_type=spec.endpoint_type)
        return self.client.create_delta_sync_index(**spec.as_create_kwargs())

    def sync(self, spec: DeltaSyncIndexSpec, *, allow_sync: bool = False) -> Any:
        if not allow_sync:
            raise PermissionError(
                "AI Search synchronization is disabled without explicit approval."
            )
        index = self.client.get_index(endpoint_name=spec.endpoint_name, index_name=spec.index_name)
        return index.sync()

    def provision(
        self,
        spec: DeltaSyncIndexSpec,
        *,
        expected_rows: int,
        allow_mutation: bool = False,
        endpoint_timeout_seconds: int = 1500,
        index_timeout_seconds: int = 1500,
        poll_seconds: float = 30,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> dict[str, Any]:
        """Bring the endpoint and index to a verified, fully indexed state in one gated step.

        - Existing index: trigger a sync (a TRIGGERED index never sees new Delta rows otherwise).
        - Missing index: create the endpoint if needed and wait for it to be online, because an
          index cannot be created on an endpoint that is still provisioning; then create the
          index, whose first sync starts automatically.
        - Then verify (``verify``): endpoint ONLINE and STANDARD, index configured as specified,
          ``status.ready`` and ``status.indexed_row_count == expected_rows``.

        Every wait is bounded. On failure the error names what was already created; nothing is
        deleted or recreated automatically.
        """
        if not allow_mutation:
            raise PermissionError(
                "AI Search provisioning is disabled; it creates billable resources and generates "
                "embeddings. Pass allow_mutation=True only after cost and permission approval."
            )
        report: dict[str, Any] = {"endpoint": spec.endpoint_name, "index": spec.index_name}
        try:
            try:
                index = self.client.get_index(
                    endpoint_name=spec.endpoint_name, index_name=spec.index_name
                )
                report["index_action"] = "syncing existing index"
                index.sync()
                report["index_action"] = "synced existing index"
            except Exception as lookup_error:
                if not _looks_missing(lookup_error):
                    raise
                try:
                    self.client.get_endpoint(name=spec.endpoint_name)
                    report["endpoint_action"] = "reused existing endpoint"
                except Exception as endpoint_error:
                    if not _looks_missing(endpoint_error):
                        raise
                    report["endpoint_action"] = "creating endpoint"
                    # The SDK waits take a timedelta; its default (60 min) exceeds the task timeout.
                    self.client.create_endpoint_and_wait(
                        name=spec.endpoint_name,
                        endpoint_type=spec.endpoint_type,
                        timeout=datetime.timedelta(seconds=endpoint_timeout_seconds),
                    )
                    report["endpoint_action"] = "created endpoint"
                report["index_action"] = "creating index"
                self.client.create_delta_sync_index(**spec.as_create_kwargs())
                report["index_action"] = "created index (initial sync starts automatically)"
            report["verification"] = self.verify(
                spec,
                expected_rows=expected_rows,
                timeout_seconds=index_timeout_seconds,
                poll_seconds=poll_seconds,
                sleep=sleep,
                clock=clock,
            )
        except Exception as exc:
            raise RuntimeError(
                f"AI Search provisioning stopped ({type(exc).__name__}: {exc}). Progress so far: "
                f"{report}. Nothing was deleted or recreated; inspect before retrying."
            ) from exc
        return report

    def verify(
        self,
        spec: DeltaSyncIndexSpec,
        *,
        expected_rows: int,
        timeout_seconds: int = 1500,
        poll_seconds: float = 30,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> dict[str, Any]:
        """Read-only: poll until the index is ready and holds every source row, then check config.

        Success needs a *reported* ``indexed_row_count`` equal to ``expected_rows``; a count the
        API does not return stays "unavailable" and the wait ends in a timeout, never a pass.
        """
        started = clock()
        attempts = 0
        while True:
            attempts += 1
            description = _as_dict(
                self.client.get_index(
                    endpoint_name=spec.endpoint_name, index_name=spec.index_name
                ).describe()
            )
            status = description.get("status") or {}
            observed = {
                "ready": status.get("ready", UNAVAILABLE),
                "indexed_row_count": status.get("indexed_row_count", UNAVAILABLE),
                "detailed_state": status.get("detailed_state", UNAVAILABLE),
                "message": status.get("message", UNAVAILABLE),
            }
            state = str(observed["detailed_state"])
            if "OFFLINE" in state or "FAILED" in state:
                raise RuntimeError(f"Index is {state}: {observed['message']}")
            if observed["ready"] is True and observed["indexed_row_count"] == expected_rows:
                break
            waited = clock() - started
            if waited >= timeout_seconds:
                raise TimeoutError(
                    f"Index not fully indexed after {waited:.0f}s: expected {expected_rows} rows, "
                    f"observed {observed}"
                )
            sleep(poll_seconds)
        configuration = _configuration_check(description, spec)
        endpoint = _as_dict(self.client.get_endpoint(name=spec.endpoint_name))
        endpoint_status = endpoint.get("endpoint_status") or {}
        endpoint_report = {
            "state": endpoint_status.get("state", UNAVAILABLE),
            "endpoint_type": endpoint.get("endpoint_type", UNAVAILABLE),
            "num_indexes": endpoint.get("num_indexes", UNAVAILABLE),
        }
        problems = [
            f"{key}: {value}" for key, value in configuration.items() if value["matches"] is False
        ]
        if endpoint_report["state"] != "ONLINE":
            problems.append(f"endpoint state is {endpoint_report['state']}")
        if endpoint_report["endpoint_type"] != spec.endpoint_type:
            problems.append(f"endpoint type is {endpoint_report['endpoint_type']}")
        if problems:
            raise RuntimeError(f"Index verification failed: {problems}")
        return {
            **observed,
            "expected_rows": expected_rows,
            "attempts": attempts,
            "waited_seconds": round(clock() - started, 1),
            "endpoint": endpoint_report,
            "configuration": configuration,
        }


def _configuration_check(description: dict[str, Any], spec: DeltaSyncIndexSpec) -> dict[str, Any]:
    """Compare the deployed index with the spec; a field the API omits is "unavailable"."""
    sync_spec = description.get("delta_sync_index_spec") or {}
    sources = sync_spec.get("embedding_source_columns") or [{}]
    actual = {
        "endpoint_name": description.get("endpoint_name"),
        "index_type": description.get("index_type"),
        "primary_key": description.get("primary_key"),
        "source_table": sync_spec.get("source_table"),
        "pipeline_type": sync_spec.get("pipeline_type"),
        "embedding_source_column": sources[0].get("name"),
        "embedding_model_endpoint_name": sources[0].get("embedding_model_endpoint_name"),
    }
    expected = {
        "endpoint_name": spec.endpoint_name,
        "index_type": "DELTA_SYNC",
        "primary_key": spec.primary_key,
        "source_table": spec.source_table_name,
        "pipeline_type": spec.pipeline_type,
        "embedding_source_column": spec.embedding_source_column,
        "embedding_model_endpoint_name": spec.embedding_model_endpoint_name,
    }
    return {
        key: {
            "expected": expected[key],
            "actual": UNAVAILABLE if actual[key] is None else actual[key],
            "matches": UNAVAILABLE if actual[key] is None else actual[key] == expected[key],
        }
        for key in expected
    }


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _status(error: Exception) -> str:
    """'missing' for a not-found error; otherwise the error type, so access problems stay visible."""
    return "missing" if _looks_missing(error) else f"error: {type(error).__name__}"


def _looks_missing(error: Exception) -> bool:
    """Treat only not-found errors as "missing"; permission or network errors must surface.

    The AI Search SDK raises different exception types across releases, so this checks both the
    type name and the message instead of importing a specific class.
    """
    text = f"{type(error).__name__} {error}".lower()
    return any(
        marker in text
        for marker in ("not found", "notfound", "does not exist", "resource_does_not_exist", "404")
    ) or isinstance(error, LookupError | KeyError)
