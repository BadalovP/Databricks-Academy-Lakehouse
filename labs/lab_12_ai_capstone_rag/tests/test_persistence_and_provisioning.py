"""Delta persistence SQL, AI Search provisioning lifecycle and runtime guards (all offline)."""

from __future__ import annotations

import datetime
from types import SimpleNamespace

import pytest

from lab12.delta_store import (
    MONITORING_SCHEMA,
    _split_schema,
    append_rows,
    merge_chunks,
    qualified_name,
)
from lab12.evaluation import EvaluationScore, score_to_row
from lab12.models import Chunk, RagAnswer, SearchResult
from lab12.monitoring import event_from_answer, event_to_row
from lab12.runtime import assert_workspace, flag, index_name, source_table
from lab12.vector_search import DeltaSyncIndexSpec, VectorSearchManager, _looks_missing


class RecordingFrame:
    def __init__(self, sink: list) -> None:
        self._sink = sink

    def createOrReplaceTempView(self, name):  # noqa: N802 - Spark API name
        self._sink.append(("view", name))


class RecordingSpark:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.frames: list = []

    def sql(self, query: str) -> None:
        self.queries.append(query)

    def merge_sql(self) -> str:
        return next(query for query in self.queries if query.lstrip().startswith("MERGE"))

    def createDataFrame(self, rows, schema=None):  # noqa: N802 - Spark API name
        self.frames.append((rows, schema))
        return RecordingFrame(self.frames)


def _chunk(chunk_id: str = "c1") -> Chunk:
    return Chunk(chunk_id, "d1", "g.md", "H", "g.md#h", 0, "text", "hash")


TABLE = qualified_name("dbr_dev", "lab12_dev", "document_chunks")


def test_chunk_merge_prunes_rows_missing_from_the_snapshot_by_default() -> None:
    spark = RecordingSpark()
    merge_chunks(spark, TABLE, [_chunk()], allow_write=True)
    sql = spark.merge_sql()
    assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in sql
    assert "current_timestamp()" in sql  # ingested_at is now populated
    assert spark.frames[0][1] is not None  # explicit schema, no inference


def test_pruning_can_be_disabled_for_partial_loads() -> None:
    spark = RecordingSpark()
    merge_chunks(spark, TABLE, [_chunk()], allow_write=True, prune_missing=False)
    assert "NOT MATCHED BY SOURCE" not in spark.merge_sql()


def test_an_empty_snapshot_never_reaches_merge() -> None:
    # Pruning an empty snapshot would delete the whole table.
    spark = RecordingSpark()
    merge_chunks(spark, TABLE, [], allow_write=True)
    assert spark.queries == []


def test_writes_require_opt_in() -> None:
    with pytest.raises(PermissionError):
        merge_chunks(RecordingSpark(), TABLE, [_chunk()])
    with pytest.raises(PermissionError):
        append_rows(RecordingSpark(), TABLE, [{"a": 1}], schema="a INT", timestamp_column="a")


def test_schema_split_respects_array_types() -> None:
    names = [part.strip().split(" ", 1)[0] for part in _split_schema(MONITORING_SCHEMA)]
    assert "retrieved_chunk_ids" in names
    assert names.count("retrieved_chunk_ids") == 1
    assert len(names) == 10


def test_monitoring_rows_append_with_timestamp_cast_and_no_raw_text() -> None:
    answer = RagAnswer(
        question="secret question text",
        answer="secret answer text",
        sources=(SearchResult("c1", "d1", "g.md", "H", "g.md#h", "body", 0.5),),
        model="m",
        latency_ms=12,
    )
    row = event_to_row(event_from_answer(answer, "c.s.idx"))
    assert row["retrieved_chunk_ids"] == ["c1"]
    assert "secret" not in str(row)
    spark = RecordingSpark()
    table = qualified_name("dbr_dev", "lab12_dev", "query_monitoring")
    assert (
        append_rows(
            spark, table, [row], schema=MONITORING_SCHEMA,
            timestamp_column="query_timestamp", allow_write=True,
        )
        == 1
    )
    assert "CAST(`query_timestamp` AS TIMESTAMP)" in spark.queries[-1]


def test_evaluation_rows_keep_metrics_as_json() -> None:
    score = EvaluationScore(
        "q01", "live-rag", "answerable", 0.5, 0.5, None, retrieval_recall=1.0, citation_precision=0.5
    )
    row = score_to_row(score, "eval-1", "2026-10-11T00:00:00+00:00")
    assert row["question_id"] == "q01" and row["variant"] == "live-rag"
    assert '"retrieval_recall": 1.0' in row["metrics_json"]
    assert '"source_coverage": null' in row["metrics_json"]  # not applicable stays null


# ---------------------------------------------------------------- AI Search provisioning
# The fakes follow databricks-ai-search 0.78: SDK waits take a datetime.timedelta (an int
# timeout raises TypeError there), and describe()/get_endpoint() return the REST payloads.

SOURCE = "dbr_dev.lab12_dev.document_chunks"
INDEX = "dbr_dev.lab12_dev.document_chunks_index"


def _description(ready, rows, state="ONLINE_NO_PENDING_UPDATE", **overrides) -> dict:
    status = {"ready": ready, "detailed_state": state, "message": "m"}
    if rows is not None:
        status["indexed_row_count"] = rows
    description = {
        "name": INDEX,
        "endpoint_name": "ep",
        "index_type": "DELTA_SYNC",
        "primary_key": "chunk_id",
        "delta_sync_index_spec": {
            "source_table": SOURCE,
            "pipeline_type": "TRIGGERED",
            "embedding_source_columns": [
                {"name": "content", "embedding_model_endpoint_name": "databricks-gte-large-en"}
            ],
        },
        "status": status,
    }
    description.update(overrides)
    return description


class Index:
    def __init__(self, descriptions: list[dict]) -> None:
        self.descriptions = descriptions
        self.synced = False

    def sync(self):
        self.synced = True

    def describe(self):
        # The last description repeats once the scripted sequence is exhausted.
        return self.descriptions.pop(0) if len(self.descriptions) > 1 else self.descriptions[0]


class ProvisionClient:
    def __init__(
        self,
        *,
        index_exists: bool,
        endpoint_exists: bool,
        descriptions: list[dict] | None = None,
        lookup_error=None,
        endpoint_state: str = "ONLINE",
    ) -> None:
        self.index = Index(descriptions or [_description(True, 30)])
        self.index_exists = index_exists
        self.endpoint_exists = endpoint_exists
        self.lookup_error = lookup_error
        self.endpoint_state = endpoint_state
        self.calls: list[str] = []

    def get_index(self, *, endpoint_name, index_name):
        if self.lookup_error:
            raise self.lookup_error
        if not self.index_exists:
            raise Exception(f"NotFound: Unity Catalog entity {index_name} does not exist.")
        return self.index

    def get_endpoint(self, *, name):
        if not self.endpoint_exists:
            raise Exception(f"NotFound: AI Search endpoint {name} not found.")
        return {
            "name": name,
            "endpoint_type": "STANDARD",
            "endpoint_status": {"state": self.endpoint_state},
            "num_indexes": 1,
        }

    def create_endpoint_and_wait(self, *, name, endpoint_type, timeout):
        assert isinstance(timeout, datetime.timedelta), "SDK requires a timedelta"
        self.calls.append("create_endpoint_and_wait")
        self.endpoint_exists = True

    def create_delta_sync_index(self, **kwargs):
        self.calls.append("create_delta_sync_index")
        self.index_exists = True
        return self.index


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _spec() -> DeltaSyncIndexSpec:
    return DeltaSyncIndexSpec("ep", INDEX, SOURCE)


def _provision(client, clock=None, **kwargs):
    clock = clock or FakeClock()
    return VectorSearchManager(client).provision(
        _spec(), expected_rows=30, allow_mutation=True, sleep=clock.sleep, clock=clock, **kwargs
    )


def test_provisioning_is_gated() -> None:
    with pytest.raises(PermissionError):
        VectorSearchManager(ProvisionClient(index_exists=True, endpoint_exists=True)).provision(
            _spec(), expected_rows=30
        )


def test_missing_resources_are_created_endpoint_first_then_verified() -> None:
    client = ProvisionClient(
        index_exists=False,
        endpoint_exists=False,
        descriptions=[
            _description(False, None, "PROVISIONING_INITIAL_SNAPSHOT"),
            _description(True, 12, "ONLINE_TRIGGERED_UPDATE"),
            _description(True, 30),
        ],
    )
    clock = FakeClock()
    report = _provision(client, clock)
    assert client.calls == ["create_endpoint_and_wait", "create_delta_sync_index"]
    assert report["endpoint_action"] == "created endpoint"
    verification = report["verification"]
    assert verification["indexed_row_count"] == 30 and verification["ready"] is True
    assert verification["attempts"] == 3 and clock.sleeps == [30, 30]
    assert verification["endpoint"] == {"state": "ONLINE", "endpoint_type": "STANDARD", "num_indexes": 1}
    assert all(item["matches"] is True for item in verification["configuration"].values())


def test_ready_but_partially_indexed_is_not_a_pass() -> None:
    # The SDK's wait_until_ready returns at any ONLINE state; the row count must also match.
    client = ProvisionClient(
        index_exists=False, endpoint_exists=True, descriptions=[_description(True, 12)]
    )
    with pytest.raises(RuntimeError, match="TimeoutError") as error:
        _provision(client, index_timeout_seconds=90)
    assert "created index" in str(error.value)  # partial progress is reported
    assert client.calls == ["create_delta_sync_index"]  # endpoint reused, nothing recreated


def test_a_missing_row_count_is_unavailable_and_times_out() -> None:
    client = ProvisionClient(
        index_exists=True, endpoint_exists=True, descriptions=[_description(True, None)]
    )
    with pytest.raises(RuntimeError, match="'indexed_row_count': 'unavailable'"):
        _provision(client, index_timeout_seconds=60)


def test_existing_index_is_synced_and_verified() -> None:
    client = ProvisionClient(index_exists=True, endpoint_exists=True)
    report = _provision(client)
    assert client.index.synced and client.calls == []
    assert report["index_action"] == "synced existing index"
    assert report["verification"]["attempts"] == 1


def test_failed_index_state_stops_immediately() -> None:
    client = ProvisionClient(
        index_exists=False, endpoint_exists=True, descriptions=[_description(False, 0, "OFFLINE_FAILED")]
    )
    with pytest.raises(RuntimeError, match="OFFLINE_FAILED"):
        _provision(client)


def test_configuration_mismatch_fails_verification() -> None:
    wrong = _description(True, 30, primary_key="document_id")
    client = ProvisionClient(index_exists=True, endpoint_exists=True, descriptions=[wrong])
    with pytest.raises(RuntimeError, match="primary_key"):
        _provision(client)


def test_endpoint_not_online_fails_verification() -> None:
    client = ProvisionClient(index_exists=True, endpoint_exists=True, endpoint_state="PROVISIONING")
    with pytest.raises(RuntimeError, match="endpoint state is PROVISIONING"):
        _provision(client)


def test_verify_is_read_only() -> None:
    client = ProvisionClient(index_exists=True, endpoint_exists=True)
    clock = FakeClock()
    VectorSearchManager(client).verify(_spec(), expected_rows=30, sleep=clock.sleep, clock=clock)
    assert client.calls == [] and client.index.synced is False


def test_permission_errors_are_not_mistaken_for_missing_resources() -> None:
    client = ProvisionClient(
        index_exists=False, endpoint_exists=False, lookup_error=PermissionError("PERMISSION_DENIED")
    )
    with pytest.raises(RuntimeError, match="PermissionError"):
        _provision(client)
    assert client.calls == []


def test_missing_detection() -> None:
    assert _looks_missing(Exception("RESOURCE_DOES_NOT_EXIST"))
    assert _looks_missing(Exception("404 Client Error: Not Found"))
    assert not _looks_missing(Exception("PERMISSION_DENIED: user lacks CAN_USE"))


# ---------------------------------------------------------------- runtime guards


def _client(host: str) -> SimpleNamespace:
    return SimpleNamespace(config=SimpleNamespace(host=host))


def test_workspace_guard_accepts_the_expected_host_case_and_slash_insensitively() -> None:
    expected = "https://dbc-1750318a-76a9.cloud.databricks.com"
    assert assert_workspace(expected + "/", _client(expected.upper())) == expected


def test_workspace_guard_refuses_the_other_environment() -> None:
    with pytest.raises(RuntimeError, match="Wrong workspace"):
        assert_workspace(
            "https://dbc-1750318a-76a9.cloud.databricks.com",
            _client("https://adb-7405618361895520.0.azuredatabricks.net"),
        )


def test_workspace_guard_requires_an_expected_host() -> None:
    with pytest.raises(ValueError):
        assert_workspace("", _client("https://x.cloud.databricks.com"))


def test_flags_are_strict() -> None:
    assert flag("TRUE") is True and flag("false") is False
    with pytest.raises(ValueError):
        flag("yes")


def test_index_and_source_names_follow_the_target_schema() -> None:
    assert index_name("dbr_dev", "parvinbadalov_lab12_rag_dev") == (
        "dbr_dev.parvinbadalov_lab12_rag_dev.document_chunks_index"
    )
    assert source_table("dbr_dev", "parvinbadalov_lab12_rag_prod") == (
        "dbr_dev.parvinbadalov_lab12_rag_prod.document_chunks"
    )
    with pytest.raises(ValueError):
        index_name("dbr_dev", "x", "not-a-three-part-name")
