"""Refusal handling, citation precision, source coverage and honest not-applicable metrics."""

from __future__ import annotations

import pytest

from urbanflow.rag.evaluation import (
    EvaluationCase,
    refusal_status,
    score_answer,
    summarize,
    summarize_by_category,
)
from urbanflow.rag.models import RagAnswer, SearchResult

ANSWERABLE = EvaluationCase(
    "q01",
    "What evidence?",
    "answerable",
    ("01_batch_job_recovery.md",),
    ("run id", "failure timestamp", "last successful task"),
)
AMBIGUOUS = EvaluationCase(
    "q13",
    "Resize or roll back?",
    "ambiguous",
    ("03_sql_performance.md", "06_change_management.md"),
    ("query profile", "validation", "approval"),
)
UNANSWERABLE = EvaluationCase("q14", "DBA phone number?", "unanswerable", (), ())


def _source(name: str, ref: str = "") -> SearchResult:
    return SearchResult(f"c-{name}", "d", name, "H", ref or f"{name}#h", "text", 0.9)


SOURCES = (
    _source("01_batch_job_recovery.md"),
    _source("04_pipeline_failure.md"),
    _source("05_monitoring_alerting.md"),
    _source("02_incident_escalation.md"),
)


def _answer(text: str, sources=SOURCES, error=None) -> RagAnswer:
    return RagAnswer("q", text, sources, "m", 1, error=error)


@pytest.mark.parametrize(
    "text, expected",
    [
        (
            "I’m sorry, but the available documents do not contain enough information to answer that.",
            "clean",
        ),
        (
            "The available documents do not contain enough information to answer this question.",
            "clean",
        ),
        ("I can’t help with that.", "clean"),
        ("The provided context does not specify a namespace.", "clean"),
        (
            "The runbooks don’t mention a phone number. Escalate through the incident channel [1].",
            "with_extra_content",
        ),
        (
            "The documents do not contain this information. The namespace is atlas-prod-east.",
            "with_specific_claims",
        ),
        ("The documents do not list it, but try 555-0100.", "with_specific_claims"),
        ("The DBA phone number is 555-0100.", "none"),
        ("The replay does not include production tables; use an isolated checkpoint [1].", "none"),
        ("Record the failed run ID [1].", "none"),
    ],
)
def test_refusal_classification(text: str, expected: str) -> None:
    assert refusal_status(text) == expected


def test_paraphrased_refusal_is_correct_for_unanswerable_questions() -> None:
    text = (
        "I’m sorry, but the available documents do not contain enough information to answer that."
    )
    score = score_answer(UNANSWERABLE, _answer(text, sources=()), "live-baseline")
    assert score.answer_correctness == 1.0 and score.refusal == "clean"


def test_refusal_that_adds_specifics_is_not_rewarded() -> None:
    text = "The documents do not contain it. The number is 555-0100 [2]."
    score = score_answer(UNANSWERABLE, _answer(text), "live-rag")
    assert score.answer_correctness == 0.0
    assert "refusal_with_specific_claims:possible_fabrication" in score.review_flags


def test_refusal_with_grounded_guidance_counts_but_is_flagged_for_review() -> None:
    text = "The runbooks don't mention a phone number. Escalate through the incident channel [4]."
    score = score_answer(UNANSWERABLE, _answer(text), "live-rag")
    assert score.answer_correctness == 1.0
    assert "refusal_with_extra_content:review" in score.review_flags


def test_unanswerable_question_answered_with_a_fact_scores_zero() -> None:
    score = score_answer(UNANSWERABLE, _answer("The DBA phone number is 555-0100."), "live-rag")
    assert score.answer_correctness == 0.0 and score.refusal == "none"


def test_refusing_an_answerable_question_scores_zero_and_is_flagged() -> None:
    text = "The available documents do not contain enough information to answer this question."
    score = score_answer(ANSWERABLE, _answer(text), "live-rag")
    assert score.answer_correctness == 0.0
    assert score.review_flags == ("answerable_question_refused:clean",)


def test_retrieval_metrics_do_not_apply_to_the_baseline() -> None:
    score = score_answer(ANSWERABLE, _answer("Record the run ID.", sources=()), "live-baseline")
    assert score.retrieval_recall is None
    assert score.citation_precision is None and score.source_coverage is None
    assert score.uncited_sentences is None


def test_retrieval_metrics_do_not_apply_to_unanswerable_questions() -> None:
    text = "The available documents do not contain enough information to answer this question."
    score = score_answer(UNANSWERABLE, _answer(text), "live-rag")
    assert score.retrieval_recall is None and score.source_coverage is None
    assert score.citation_precision is None
    assert score.uncited_sentences == 0


def test_precise_single_citation_is_not_penalised_for_uncited_distractors() -> None:
    # The Stage 4B smoke answer: one exact citation, three retrieved distractors left uncited.
    text = "Record the failed run ID, the failure timestamp and the last successful task [1]."
    score = score_answer(ANSWERABLE, _answer(text), "live-rag")
    assert score.retrieval_recall == 1.0
    assert score.citation_precision == 1.0 and score.source_coverage == 1.0
    assert score.answer_correctness == 1.0 and score.uncited_sentences == 0


def test_citation_precision_counts_citations_to_non_supporting_documents() -> None:
    score = score_answer(ANSWERABLE, _answer("Record the run ID [1]. Page the IC [4]."), "live-rag")
    assert score.citation_precision == 0.5


def test_source_coverage_measures_expected_documents_actually_cited() -> None:
    sources = (_source("03_sql_performance.md"), _source("06_change_management.md"))
    score = score_answer(AMBIGUOUS, _answer("Check the query profile [1].", sources), "live-rag")
    assert score.retrieval_recall == 1.0
    assert score.source_coverage == 0.5 and score.citation_precision == 1.0


def test_errors_are_never_counted_as_refusals() -> None:
    answer = _answer("The assistant could not complete the request safely.", error="TimeoutError")
    score = score_answer(UNANSWERABLE, answer, "live-rag")
    assert score.refusal == "none" and score.answer_correctness == 0.0
    assert score.review_flags == ("error:TimeoutError",)
    assert score.uncited_sentences is None


def test_summary_reports_not_applicable_and_counts() -> None:
    baseline = score_answer(ANSWERABLE, _answer("Record the run ID.", sources=()), "live-baseline")
    summary = summarize([baseline])
    assert summary["retrieval_recall"] == "not_applicable" and summary["retrieval_recall_n"] == 0
    assert summary["uncited_sentences_heuristic"] == "not_applicable"
    assert summary["answer_correctness_n"] == 1


def test_summary_by_category() -> None:
    scores = [
        score_answer(ANSWERABLE, _answer("Record the run ID [1]."), "live-rag"),
        score_answer(UNANSWERABLE, _answer("I can't help with that."), "live-rag"),
    ]
    by_category = summarize_by_category(scores)
    assert set(by_category) == {"answerable", "unanswerable"}
    assert by_category["unanswerable"]["answer_correctness"] == 1.0
    assert by_category["unanswerable"]["citation_precision"] == "not_applicable"
