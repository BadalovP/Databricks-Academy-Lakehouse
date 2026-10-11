"""Generation diagnostics, empty/truncated completions, citation checks and no-op MERGE skipping.

The fakes follow the OpenAI chat-completion shape returned by databricks-openai 0.17.1
(choices[0].message.content / finish_reason, usage.prompt_tokens / completion_tokens /
completion_tokens_details.reasoning_tokens). They prove the adapters' handling, not a live model.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

from lab12.delta_store import merge_chunks, qualified_name, snapshot_fingerprint
from lab12.evaluation import (
    EvaluationCase,
    citation_map,
    cited_numbers,
    score_answer,
    summarize,
    summarize_generation,
)
from lab12.models import Chunk, RagAnswer, SearchResult
from lab12.monitoring import event_from_answer
from lab12.rag import (
    FAILURE,
    INSUFFICIENT,
    DatabricksChatModel,
    NoRetrievalBaseline,
    RagService,
    call_diagnostics,
)
from lab12.retrieval import DatabricksAISearchRetriever

QUESTION = "How should an Aurora streaming replay be performed safely?"


def _response(content, finish_reason="stop", usage=True, reasoning=40):
    details = SimpleNamespace(reasoning_tokens=reasoning) if reasoning is not None else None
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=content), finish_reason=finish_reason)],
        usage=SimpleNamespace(
            prompt_tokens=900, completion_tokens=120, total_tokens=1020,
            completion_tokens_details=details,
        ) if usage else None,
    )


class FakeClient:
    def __init__(self, response=None, error=None) -> None:
        self.response, self.error, self.calls = response, error, 0
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.calls += 1
        if self.error:
            raise self.error
        return self.response


def _model(response=None, error=None) -> DatabricksChatModel:
    return DatabricksChatModel("databricks-gpt-oss-20b", FakeClient(response, error), max_tokens=700)


class Retriever:
    def search(self, question, *, top_k, filters=None):
        return [
            SearchResult("c1", "d1", "01_batch_job_recovery.md", "Replay", "01#replay", "Use a checkpoint.", 0.9),
            SearchResult("c2", "d1", "01_batch_job_recovery.md", "Verify", "01#verify", "Reconcile counts.", 0.7),
        ]


# ---------------------------------------------------------------- call diagnostics


def test_diagnostics_report_finish_reason_tokens_and_content_shape() -> None:
    content = [{"type": "reasoning", "summary": []}, {"type": "text", "text": "Use a checkpoint [1]."}]
    diagnostics = call_diagnostics(_response(content), "Use a checkpoint [1].", 812, 700)
    assert diagnostics["finish_reason"] == "stop"
    assert (diagnostics["prompt_tokens"], diagnostics["completion_tokens"]) == (900, 120)
    assert diagnostics["reasoning_tokens"] == 40 and diagnostics["total_tokens"] == 1020
    assert diagnostics["content_blocks"] == ["reasoning", "text"]
    assert diagnostics["truncated"] is False and diagnostics["empty_response"] is False
    assert diagnostics["generation_ms"] == 812


def test_missing_usage_is_unavailable_not_zero() -> None:
    diagnostics = call_diagnostics(_response("ok", finish_reason=None, usage=False), "ok", 5, 700)
    assert diagnostics["prompt_tokens"] == "unavailable"
    assert diagnostics["reasoning_tokens"] == "unavailable"
    assert diagnostics["finish_reason"] == "unavailable"


def test_model_records_errors_without_retrying() -> None:
    model = _model(error=TimeoutError("upstream"))
    answer = NoRetrievalBaseline(model).answer(QUESTION)
    assert answer.error == "TimeoutError" and answer.answer == FAILURE
    assert model._client.calls == 1
    assert answer.diagnostics["error"] == "TimeoutError"


# ---------------------------------------------------------------- empty / truncated completions


def test_reasoning_only_completion_is_an_error_not_a_refusal() -> None:
    # GPT-OSS can spend the whole budget on reasoning and return no text block.
    model = _model(_response([{"type": "reasoning", "summary": []}], finish_reason="length"))
    answer = RagService(Retriever(), model).answer(QUESTION)
    assert answer.error == "TruncatedModelResponse"
    assert answer.answer == FAILURE and INSUFFICIENT not in answer.answer
    assert answer.diagnostics["empty_response"] is True and answer.diagnostics["truncated"] is True


def test_empty_completion_scores_as_wrong_even_for_unanswerable_questions() -> None:
    answer = NoRetrievalBaseline(_model(_response(""))).answer("What is the DBA's phone number?")
    assert answer.error == "EmptyModelResponse"
    case = EvaluationCase("q16", "What is the DBA's phone number?", "unanswerable", (), ())
    assert score_answer(case, answer, "live-baseline").answer_correctness == 0.0


def test_truncated_text_is_kept_but_flagged() -> None:
    answer = RagService(Retriever(), _model(_response("Use a checkpoint [1] and", "length"))).answer(QUESTION)
    assert answer.error == "TruncatedModelResponse"
    assert answer.answer.startswith("Use a checkpoint")


def test_rag_diagnostics_hold_timings_and_tokens_but_no_text() -> None:
    answer = RagService(Retriever(), _model(_response("Use a checkpoint [1]."))).answer(QUESTION)
    diagnostics = answer.diagnostics
    assert answer.error is None
    assert diagnostics["sources"] == 2 and diagnostics["sources_in_context"] == 2
    assert isinstance(diagnostics["retrieval_ms"], int) and diagnostics["context_chars"] > 0
    assert diagnostics["prompt_tokens"] == 900
    serialized = json.dumps(diagnostics)
    assert "Aurora" not in serialized and "checkpoint" not in serialized


def test_monitoring_keeps_diagnostics_and_still_no_raw_text() -> None:
    answer = RagService(Retriever(), _model(_response("Use a checkpoint [1]."))).answer(QUESTION)
    event = event_from_answer(answer, "dbr_dev.s.idx")
    assert json.loads(event.evaluation_json)["diagnostics"]["finish_reason"] == "stop"
    assert "Aurora" not in str(event) and "checkpoint" not in str(event)


# ---------------------------------------------------------------- citations


def _answer(text: str, sources: int = 2) -> RagAnswer:
    results = tuple(
        SearchResult(f"c{i}", "d1", "01_batch_job_recovery.md", "H", f"01#s{i}", "t", 0.5)
        for i in range(1, sources + 1)
    )
    return RagAnswer(QUESTION, text, results, "m", 1)


def test_grouped_citations_are_parsed() -> None:
    assert cited_numbers("A [1]. B [1, 2]. C [2][3].") == {1, 2, 3}


def test_citations_map_to_retrieved_sections_and_invalid_ones_are_counted() -> None:
    answer = _answer("Checkpoint first [1]. Reconcile [2]. Escalate [5].")
    assert citation_map(answer) == {1: "01#s1", 2: "01#s2", 5: "INVALID"}
    case = EvaluationCase("q01", QUESTION, "procedure", ("01_batch_job_recovery.md",), ())
    score = score_answer(case, answer, "live-rag")
    assert score.invalid_citations == 1
    assert score.citation_precision == 1.0  # both valid citations support; [5] does not count
    assert score.source_coverage == 1.0
    assert summarize([score])["invalid_citations"] == 1


def test_generation_summary_counts_unavailable_values() -> None:
    reported = RagAnswer("q", "a", (), "m", 10, diagnostics={
        "prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150, "finish_reason": "stop"})
    missing = RagAnswer("q", "a", (), "m", 30, error="TimeoutError", diagnostics={})
    summary = summarize_generation([reported, missing])
    assert summary["prompt_tokens"] == 100 and summary["prompt_tokens_unavailable"] == 1
    assert summary["reasoning_tokens"] == "unavailable"
    assert summary["finish_reasons"] == {"stop": 1, "unavailable": 1}
    assert summary["errors"] == ["TimeoutError"] and summary["latency_ms_max"] == 30


def test_retriever_disables_sdk_retries() -> None:
    class Index:
        def similarity_search(self, **kwargs):
            self.kwargs = kwargs
            return {"manifest": {"columns": []}, "result": {"data_array": []}}

    index = Index()
    client = SimpleNamespace(get_index=lambda **kwargs: index)
    DatabricksAISearchRetriever(client, "ep", "c.s.idx").search("q", top_k=4)
    assert index.kwargs["total_retries"] == 0 and index.kwargs["query_type"] == "HYBRID"


# ---------------------------------------------------------------- no-op MERGE skipping

TABLE = qualified_name("dbr_dev", "lab12_dev", "document_chunks")


class SnapshotSpark:
    """Answers the snapshot SELECT with a scripted row and records every statement."""

    def __init__(self, snapshot: dict | None) -> None:
        self.snapshot, self.queries = snapshot, []

    def createDataFrame(self, rows, schema=None):  # noqa: N802 - Spark API name
        return SimpleNamespace(createOrReplaceTempView=lambda name: None)

    def sql(self, query):
        self.queries.append(query)
        if query.lstrip().startswith("SELECT count(*)") and self.snapshot is not None:
            return SimpleNamespace(collect=lambda: [self.snapshot])
        if query.startswith("DESCRIBE HISTORY"):
            return SimpleNamespace(collect=lambda: [{"version": 2, "operation": "MERGE"}])
        return SimpleNamespace(collect=lambda: [])


CHUNKS = [Chunk("c1", "d1", "g.md", "H", "g.md#h", 0, "text", "h1")]


def _snapshot(fingerprint, rows=1, nulls=0):
    return {"row_count": rows, "distinct_keys": rows, "fingerprint": fingerprint,
            "max_ingested_at": "t", "null_ingested_at": nulls}


def test_unchanged_snapshot_skips_the_merge_and_writes_no_version() -> None:
    spark = SnapshotSpark(_snapshot(snapshot_fingerprint(CHUNKS, "chunk_id")))
    report = merge_chunks(spark, TABLE, CHUNKS, allow_write=True)
    assert report == {"skipped": "table already matches the snapshot", "version": 2}
    assert not any(query.lstrip().startswith("MERGE") for query in spark.queries)


def test_changed_content_still_merges() -> None:
    spark = SnapshotSpark(_snapshot("different"))
    report = merge_chunks(spark, TABLE, CHUNKS, allow_write=True)
    assert "statement" in report
    assert any(query.lstrip().startswith("MERGE") for query in spark.queries)


def test_missing_ingested_at_or_row_mismatch_still_merges() -> None:
    fingerprint = snapshot_fingerprint(CHUNKS, "chunk_id")
    for snapshot in (_snapshot(fingerprint, nulls=1), _snapshot(fingerprint, rows=2), None):
        spark = SnapshotSpark(snapshot)
        merge_chunks(spark, TABLE, CHUNKS, allow_write=True)
        assert any(query.lstrip().startswith("MERGE") for query in spark.queries), snapshot
