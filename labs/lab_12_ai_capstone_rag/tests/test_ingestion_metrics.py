"""MERGE metrics, table fingerprints and AI Search status reporting (all offline).

A rerun that changes nothing must be proven by reported metrics; a metric that is not reported
is "unavailable", never silently 0.
"""

from __future__ import annotations

import pytest

from lab12.delta_store import (
    UNAVAILABLE,
    change_data_feed_enabled,
    history_metrics,
    merge_chunks,
    qualified_name,
    snapshot_fingerprint,
    statement_metrics,
    table_snapshot,
)
from lab12.models import Chunk
from lab12.vector_search import DeltaSyncIndexSpec, VectorSearchManager

TABLE = qualified_name("dbr_dev", "lab12_dev", "document_chunks")


class Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def collect(self) -> list[dict]:
        return self._rows


class ScriptedSpark:
    """Answers DESCRIBE HISTORY from a list of commits and MERGE with a scripted metrics row."""

    def __init__(self, history: list[dict], merge_row: dict | None, commit: dict | None) -> None:
        self.history = history
        self.merge_row = merge_row
        self.commit = commit
        self.queries: list[str] = []

    def createDataFrame(self, rows, schema=None):  # noqa: N802 - Spark API name
        return type("Frame", (), {"createOrReplaceTempView": lambda self, name: None})()

    def sql(self, query: str):
        self.queries.append(query)
        if query.startswith("DESCRIBE HISTORY"):
            return Result(self.history[:1])
        if query.lstrip().startswith("MERGE"):
            if self.commit is not None:
                self.history.insert(0, self.commit)
            return Result([self.merge_row] if self.merge_row is not None else [])
        return Result([])


def _chunk(chunk_id: str, content_hash: str = "h") -> Chunk:
    return Chunk(chunk_id, "d1", "g.md", "H", "g.md#h", 0, "text", content_hash)


def _merge(history, merge_row, commit):
    spark = ScriptedSpark(history, merge_row, commit)
    return merge_chunks(spark, TABLE, [_chunk("c1")], allow_write=True)


def test_first_load_reports_statement_and_commit_metrics() -> None:
    report = _merge(
        history=[{"version": 0, "operation": "CREATE TABLE", "operationMetrics": {}}],
        merge_row={"num_affected_rows": 30, "num_inserted_rows": 30, "num_updated_rows": 0,
                   "num_deleted_rows": 0},
        commit={"version": 1, "operation": "MERGE", "operationMetrics": {
            "numTargetRowsInserted": "30", "numTargetRowsUpdated": "0", "numTargetRowsDeleted": "0"}},
    )
    assert report["statement"] == {"inserted": 30, "updated": 0, "deleted": 0}
    history = report["history"]
    assert history["commit"] == "MERGE"
    assert (history["version_before"], history["version_after"]) == (0, 1)
    assert (history["inserted"], history["updated"], history["deleted"]) == (30, 0, 0)


def test_rerun_without_a_new_version_reports_history_as_unavailable_not_zero() -> None:
    history = [{"version": 1, "operation": "MERGE", "operationMetrics": {
        "numTargetRowsInserted": "30", "numTargetRowsUpdated": "0", "numTargetRowsDeleted": "0"}}]
    report = _merge(
        history=history,
        merge_row={"num_affected_rows": 0, "num_inserted_rows": 0, "num_updated_rows": 0,
                   "num_deleted_rows": 0},
        commit=None,
    )
    assert report["statement"] == {"inserted": 0, "updated": 0, "deleted": 0}
    assert report["history"]["commit"] == "no new Delta version"
    # The previous commit's 30 inserts must not be attributed to this run.
    assert report["history"]["inserted"] == UNAVAILABLE


def test_missing_metrics_are_unavailable() -> None:
    report = _merge(
        history=[{"version": 3, "operation": "MERGE", "operationMetrics": {}}],
        merge_row=None,
        commit={"version": 4, "operation": "MERGE", "operationMetrics": {"numTargetRowsInserted": "2"}},
    )
    assert report["statement"] == dict.fromkeys(("inserted", "updated", "deleted"), UNAVAILABLE)
    assert report["history"]["inserted"] == 2
    assert report["history"]["updated"] == UNAVAILABLE


def test_a_foreign_latest_commit_is_not_read_as_merge_metrics() -> None:
    spark = ScriptedSpark(
        [{"version": 5, "operation": "OPTIMIZE", "operationMetrics": {"numTargetRowsInserted": "9"}}],
        None,
        None,
    )
    report = history_metrics(spark, TABLE, version_before=4)
    assert report["commit"] == "latest commit is OPTIMIZE, not MERGE"
    assert report["inserted"] == UNAVAILABLE


def test_statement_metrics_tolerate_a_result_without_rows_or_collect() -> None:
    assert statement_metrics(None)["inserted"] == UNAVAILABLE
    assert statement_metrics(Result([]))["deleted"] == UNAVAILABLE


def test_fingerprint_is_order_independent_and_sensitive_to_content() -> None:
    a, b = _chunk("c1", "h1"), _chunk("c2", "h2")
    assert snapshot_fingerprint([a, b], "chunk_id") == snapshot_fingerprint([b, a], "chunk_id")
    assert snapshot_fingerprint([a, b], "chunk_id") != snapshot_fingerprint(
        [a, _chunk("c2", "changed")], "chunk_id"
    )
    # Same construction as the SQL: sha2(concat_ws(',', sort_array(key || ':' || hash)), 256).
    import hashlib

    assert snapshot_fingerprint([b, a], "chunk_id") == hashlib.sha256(b"c1:h1,c2:h2").hexdigest()


def test_table_snapshot_reads_the_aggregate_and_rejects_unsafe_input() -> None:
    class Spark:
        def sql(self, query):
            self.query = query
            return Result([{"row_count": 30, "distinct_keys": 30, "fingerprint": "f",
                            "max_ingested_at": "2026-10-11", "null_ingested_at": 0}])

    spark = Spark()
    assert table_snapshot(spark, TABLE, "chunk_id")["row_count"] == 30
    assert "sort_array(collect_list(concat(`chunk_id`, ':', content_hash)))" in spark.query
    with pytest.raises(ValueError):
        table_snapshot(spark, TABLE, "chunk_id; DROP")
    with pytest.raises(ValueError):
        table_snapshot(spark, "dbr_dev.x.y", "chunk_id")


@pytest.mark.parametrize("value, expected", [("true", True), ("false", False), (None, False)])
def test_change_data_feed_detection(value, expected) -> None:
    class Spark:
        def sql(self, query):
            return Result([] if value is None else [{"key": "delta.enableChangeDataFeed", "value": value}])

    assert change_data_feed_enabled(Spark(), TABLE) is expected


class InspectClient:
    def __init__(self, endpoint_error, index_error) -> None:
        self.endpoint_error, self.index_error = endpoint_error, index_error

    def get_endpoint(self, *, name):
        if self.endpoint_error:
            raise self.endpoint_error
        return {"name": name}

    def get_index(self, *, endpoint_name, index_name):
        if self.index_error:
            raise self.index_error
        return {"name": index_name}


def _spec() -> DeltaSyncIndexSpec:
    return DeltaSyncIndexSpec("ep", "dbr_dev.lab12_dev.document_chunks_index", TABLE.replace("`", ""))


def test_inspect_distinguishes_missing_from_access_errors() -> None:
    report = VectorSearchManager(
        InspectClient(Exception("Endpoint does not exist"), PermissionError("PERMISSION_DENIED"))
    ).inspect(_spec())
    assert report["endpoint_status"] == "missing"
    assert report["index_status"] == "error: PermissionError"


def test_inspect_reports_present_resources() -> None:
    report = VectorSearchManager(InspectClient(None, None)).inspect(_spec())
    assert (report["endpoint_status"], report["index_status"]) == ("present", "present")
