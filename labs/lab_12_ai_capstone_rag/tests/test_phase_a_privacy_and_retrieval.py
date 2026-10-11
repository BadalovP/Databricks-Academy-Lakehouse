"""Phase A regressions: persisted-diagnostics privacy (A1) and section-aware retrieval (A2).

Rankings and diagnostics below are copied from the Stage 5C live run (fictional corpus). The
retrieval tests pin the mechanism; whether the live model now completes q13 needs a live run.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from lab12.cli import build_offline_service
from lab12.evaluation import (
    EvaluationCase,
    live_call_plan,
    load_cases,
    score_answer,
    score_to_row,
    summarize_by_category,
)
from lab12.ingestion import document_titles, load_documents, section_headings
from lab12.models import Chunk, RagAnswer, SearchResult
from lab12.monitoring import event_from_answer, event_to_row, safe_diagnostics
from lab12.rag import INSUFFICIENT, RagService, SectionTopics, drop_preamble
from lab12.retrieval import OfflineLexicalRetriever

ROOT = Path(__file__).resolve().parents[1]
CORPUS = load_documents([ROOT / "data" / "sample_docs"])
TITLES = document_titles(CORPUS)
HEADINGS = section_headings(CORPUS)
DOCS = {path.name: path.read_text(encoding="utf-8") for path in (ROOT / "data" / "sample_docs").glob("*.md")}
SYSTEM_NAMES = ["orion", "northstar", "atlas", "aurora", "beacon", "meridian"]


def _section(document: str, heading: str) -> str:
    match = re.search(rf"## {heading}\n\n(.*?)(?=\n## |\Z)", DOCS[document], flags=re.DOTALL)
    return match.group(1).strip()


def _source(document: str, heading: str, score: float = 0.95) -> SearchResult:
    slug = heading.lower().replace(" ", "-")
    return SearchResult(f"chk-{document[:2]}-{slug}", f"doc-{document[:2]}", document, heading,
                        f"{document}#{slug}", _section(document, heading), score)


def _title_chunk(document: str) -> SearchResult:
    title = TITLES[document]
    body = DOCS[document].split("\n\n", 2)[1].strip()  # the "> Fictional training document" note
    slug = title.lower().replace(" ", "-")
    return SearchResult(f"chk-{document[:2]}-title", f"doc-{document[:2]}", document, title,
                        f"{document}#{slug}", body, 0.99)


# ================================================================ A1: persisted diagnostics

STAGE_5C_Q01_DIAGNOSTICS = {  # the real q01 diagnostics shape from run 152927833894746
    "answer_chars": 174, "completion_tokens": 136, "content_blocks": ["reasoning", "text"],
    "context_chars": 355, "empty_response": False, "finish_reason": "stop", "generation_ms": 1735,
    "max_tokens": 2000, "prompt_tokens": 409, "reasoning_tokens": "unavailable",
    "retrieval_ms": 1174, "scope": {"applied": True, "dropped": 3, "systems": ["Orion"]},
    "sources": 4, "sources_in_context": 1, "total_tokens": 545, "truncated": False,
}


def _answer(diagnostics: dict, question: str = "What evidence ... the Orion nightly batch?") -> RagAnswer:
    source = _source("01_batch_job_recovery.md", "Scope")
    return RagAnswer(question, "Record the failed run ID [1].", (source,), "databricks-gpt-oss-20b",
                     2909, diagnostics=diagnostics)


def test_persisted_monitoring_row_has_no_system_names() -> None:
    row = event_to_row(event_from_answer(_answer(STAGE_5C_Q01_DIAGNOSTICS), "dbr_dev.s.idx"))
    serialized = json.dumps(row, default=str).lower()
    assert not any(name in serialized for name in SYSTEM_NAMES)
    assert "nightly" not in serialized and "record the failed" not in serialized
    persisted = json.loads(row["evaluation_json"])["diagnostics"]
    assert persisted["scope"] == {"applied": True, "dropped": 3}
    assert persisted["prompt_tokens"] == 409 and persisted["finish_reason"] == "stop"


def test_persisted_evaluation_row_has_no_system_names() -> None:
    case = EvaluationCase("q01", "evidence?", "answerable", ("01_batch_job_recovery.md",), ("run id",))
    score = score_answer(case, _answer(STAGE_5C_Q01_DIAGNOSTICS), "live-rag")
    row = score_to_row(score, "eval-1", "2026-10-11T00:00:00+00:00")
    assert not any(name in row["metrics_json"].lower() for name in SYSTEM_NAMES)
    assert json.loads(row["metrics_json"])["diagnostics"]["scope"] == {"applied": True, "dropped": 3}


def test_full_diagnostics_stay_available_for_the_run_output() -> None:
    answer = _answer(STAGE_5C_Q01_DIAGNOSTICS)
    assert answer.diagnostics["scope"]["systems"] == ["Orion"]  # printed in the Job review only


def test_allowlist_drops_unknown_keys_and_free_text() -> None:
    safe = safe_diagnostics({
        "supplemental_headings": ["Rollback"],
        "note": "the question asked about Orion",
        "error": "Something failed for user input 'Orion'",
        "finish_reason": "a free-text reason",
        "content_blocks": ["text", "Orion"],
        "prompt_tokens": "409",  # a string is not a number
        "sources": True,  # a bool is not a count
        "supplemental_queries": 1,
    })
    assert safe == {
        "error": "Error",
        "finish_reason": "other",
        "content_blocks": ["text", "other"],
        "supplemental_queries": 1,
    }


def test_exception_class_names_are_kept() -> None:
    assert safe_diagnostics({"error": "RateLimitError"}) == {"error": "RateLimitError"}


# ================================================================ A2: section topics


@pytest.mark.parametrize(
    "question, topics",
    [
        ("A deployment is slow; should we resize compute or roll back?", ["Rollback"]),  # q13
        ("What are the Orion recovery stop conditions?", ["Stop conditions"]),  # q03
        ("What evidence is required before a Meridian production deployment?", ["Before deployment"]),
        ("What is the production database administrator's phone number?", []),  # q14
        ("Give me the password for the billing service account.", []),  # q15
        ("Which Kubernetes namespace hosts the Atlas service?", []),  # q17
    ],
)
def test_topics_come_from_explicit_section_names(question: str, topics: list[str]) -> None:
    assert SectionTopics(HEADINGS).topics(question) == topics


def test_only_three_suite_questions_trigger_extra_retrieval() -> None:
    topics = SectionTopics(HEADINGS)
    cases = load_cases(ROOT / "config" / "evaluation_questions.yml")
    triggered = {case.question_id for case in cases if topics.topics(case.question)}
    assert triggered == {"q03", "q12", "q13"}


def test_extra_searches_are_bounded() -> None:
    topics = SectionTopics(["Scope", "Review", "Validation"], max_queries=2)
    assert len(topics.topics("Review the scope and validation")) == 2
    assert SectionTopics(HEADINGS, max_queries=0).topics("roll back?") == []


# Stage 5C live top-4 for q13; the Rollback section was missing.
Q13_PRIMARY = [
    _source("06_change_management.md", "Deployment controls", 1.0),
    _source("03_sql_performance.md", "Evidence first"),
    _source("04_pipeline_failure.md", "Checkpoints and replay"),
    _source("03_sql_performance.md", "Safe actions"),
]
ROLLBACK = _source("06_change_management.md", "Rollback", 0.9)


class FilteringRetriever:
    """Returns the fixed primary results; a section filter returns that section's excerpt."""

    def __init__(self, primary, sections, fail=False):
        self.primary, self.sections, self.fail = primary, sections, fail
        self.calls: list[dict | None] = []

    def search(self, question, *, top_k, filters=None):
        self.calls.append(filters)
        if filters and "section_heading" in filters:
            if self.fail:
                raise TimeoutError("search unavailable")
            return [s for s in self.sections if s.section_heading == filters["section_heading"]][:top_k]
        return list(self.primary)


class Recorder:
    model_name = "recorder"

    def __init__(self, reply="Answer [1]."):
        self.reply, self.prompts = reply, []

    def complete(self, *, system_prompt, user_prompt):
        self.prompts.append(user_prompt)
        return self.reply


def _service(retriever, model, **kwargs):
    return RagService(retriever, model, titles=TITLES, headings=HEADINGS, **kwargs)


def test_q13_context_gains_the_rollback_section_with_one_extra_search() -> None:
    retriever, model = FilteringRetriever(Q13_PRIMARY, [ROLLBACK]), Recorder()
    answer = _service(retriever, model).answer(
        "A deployment is slow; should we resize compute or roll back?"
    )
    assert "06_change_management.md#rollback" in [s.section_ref for s in answer.sources]
    assert "error rate breaches the release threshold" in model.prompts[0]
    assert retriever.calls == [None, {"section_heading": "Rollback"}]  # 1 main + 1 extra search
    assert len(model.prompts) == 1  # still a single model call
    assert answer.diagnostics["supplemental_queries"] == 1
    assert answer.diagnostics["supplemental_added"] == 1


def test_an_already_retrieved_section_is_not_duplicated() -> None:
    retriever = FilteringRetriever(Q13_PRIMARY + [ROLLBACK], [ROLLBACK])
    answer = _service(retriever, Recorder()).answer("Should we roll back?")
    assert [s.section_ref for s in answer.sources].count("06_change_management.md#rollback") == 1
    assert answer.diagnostics["supplemental_added"] == 0


def test_a_failed_extra_search_never_fails_the_answer() -> None:
    retriever = FilteringRetriever(Q13_PRIMARY, [ROLLBACK], fail=True)
    answer = _service(retriever, Recorder()).answer("Should we roll back?")
    assert answer.error is None and answer.sources
    assert answer.diagnostics["supplemental_errors"] == 1


def test_system_scope_still_filters_extra_excerpts() -> None:
    # Names Orion and the topic "Stop conditions"; an Aurora excerpt must not reach the prompt.
    aurora = _source("04_pipeline_failure.md", "Verification")
    stop = _source("01_batch_job_recovery.md", "Stop conditions")
    retriever = FilteringRetriever([aurora], [stop])
    answer = _service(retriever, Recorder()).answer("What are the Orion recovery stop conditions?")
    assert {s.source_name for s in answer.sources} == {"01_batch_job_recovery.md"}


def test_extra_search_is_off_without_headings() -> None:
    retriever = FilteringRetriever(Q13_PRIMARY, [ROLLBACK])
    RagService(retriever, Recorder(), titles=TITLES).answer("Should we roll back?")
    assert retriever.calls == [None]


# ================================================================ A2: title-only chunks


def test_title_only_chunks_are_dropped() -> None:
    kept, dropped = drop_preamble(
        [_title_chunk("03_sql_performance.md"), _source("03_sql_performance.md", "Safe actions")], TITLES
    )
    assert dropped == 1 and [s.section_heading for s in kept] == ["Safe actions"]


def test_a_long_section_under_the_title_heading_is_kept() -> None:
    long_body = SearchResult("c", "d", "03_sql_performance.md", TITLES["03_sql_performance.md"],
                             "03#title", "x " * 200, 0.9)
    assert drop_preamble([long_body], TITLES) == ([long_body], 0)


def test_only_title_chunks_retrieved_means_a_refusal_without_a_model_call() -> None:
    retriever = FilteringRetriever([_title_chunk("03_sql_performance.md")], [])
    model = Recorder()
    answer = _service(retriever, model).answer("Which Kubernetes namespace hosts the Atlas service?")
    assert answer.answer == INSUFFICIENT and model.prompts == []
    assert answer.diagnostics["preamble_dropped"] == 1


def test_offline_fixture_now_refuses_every_unanswerable_question() -> None:
    # Stage 5A noted the fixture "answered" q17 by quoting the Atlas title chunk.
    service = build_offline_service()
    cases = load_cases(ROOT / "config" / "evaluation_questions.yml")
    scores = [score_answer(c, service.answer(c.question), "offline-rag") for c in cases]
    unanswerable = summarize_by_category(scores)["unanswerable"]
    assert unanswerable["answer_correctness"] == 1.0 and unanswerable["refusals"] == {"clean": 3}


# ================================================================ bounds and fixtures


def test_call_plan_counts_the_extra_searches() -> None:
    plan = live_call_plan(1, include_baseline=False, supplemental_per_question=2)
    assert plan["ai_search_queries"] == 3 and plan["chat_calls"] == 1


def test_offline_filtered_search_ignores_the_fixture_threshold() -> None:
    chunk = Chunk("c1", "d1", "06_change_management.md", "Rollback", "06#rollback", 0,
                  "Rollback when a critical validation fails.", "h")
    retriever = OfflineLexicalRetriever([chunk], score_threshold=0.5)
    assert retriever.search("roll back?", top_k=1) == []  # unfiltered: below threshold
    assert retriever.search("roll back?", top_k=1, filters={"section_heading": "Rollback"})


def test_offline_q13_context_contains_the_rollback_section() -> None:
    answer = build_offline_service().answer(
        "A deployment is slow; should we resize compute or roll back?"
    )
    assert "06_change_management.md#rollback" in [s.section_ref for s in answer.sources]
