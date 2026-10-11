"""Regression tests for the four imperfect answers of the 2026-10-11 live evaluation.

Retrieval rankings and answer sentences below are copied from that run (fictional corpus). These
tests pin the offline mechanisms - prompt rules, excerpt labels, system scoping and the
weak-citation heuristic. Whether the live model now answers q01, q09, q12 and q13 correctly can
only be shown by an approved live re-run.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from lab12.cli import build_offline_service
from lab12.evaluation import (
    EvaluationCase,
    load_cases,
    score_answer,
    summarize_by_category,
    weak_citation_sentences,
)
from lab12.ingestion import document_titles, load_documents
from lab12.models import RagAnswer, SearchResult
from lab12.rag import INSUFFICIENT, SYSTEM_PROMPT, RagService, SystemScope, build_context

ROOT = Path(__file__).resolve().parents[1]
DOCS = {path.name: path.read_text(encoding="utf-8") for path in (ROOT / "data" / "sample_docs").glob("*.md")}
TITLES = document_titles(load_documents([ROOT / "data" / "sample_docs"]))


def _section(document: str, heading: str) -> str:
    match = re.search(rf"## {heading}\n\n(.*?)(?=\n## |\Z)", DOCS[document], flags=re.DOTALL)
    return match.group(1).strip()


def _source(document: str, heading: str, score: float = 0.95) -> SearchResult:
    slug = heading.lower().replace(" ", "-")
    return SearchResult(f"chk-{document[:2]}-{slug}", f"doc-{document[:2]}", document, heading,
                        f"{document}#{slug}", _section(document, heading), score)


# Live top-4 for q01 ("What evidence must be captured before recovering the Orion nightly batch?").
Q01_RETRIEVED = [
    _source("01_batch_job_recovery.md", "Scope", 1.0),
    _source("04_pipeline_failure.md", "Failure classification"),
    _source("05_monitoring_alerting.md", "Data protection"),
    _source("02_incident_escalation.md", "Required update format"),
]
# Live top-4 for q09 minus the title chunk: the answer was in rank 1, yet the model refused.
Q09_RETRIEVED = [
    _source("04_pipeline_failure.md", "Verification", 1.0),
    _source("04_pipeline_failure.md", "Checkpoints and replay"),
    _source("02_incident_escalation.md", "Required update format"),
]


class Recorder:
    model_name = "recorder"

    def __init__(self, reply: str = "Answer [1].") -> None:
        self.reply = reply
        self.prompts: list[str] = []

    def complete(self, *, system_prompt: str, user_prompt: str) -> str:
        self.prompts.append(user_prompt)
        return self.reply


class Fixed:
    def __init__(self, results: list[SearchResult]) -> None:
        self.results = results

    def search(self, question, *, top_k, filters=None):
        return list(self.results)


# ---------------------------------------------------------------- grounding instructions


def test_prompt_keeps_the_exact_refusal_and_the_no_invention_rule() -> None:
    assert INSUFFICIENT in SYSTEM_PROMPT.splitlines()
    assert "Do not invent commands, credentials" in SYSTEM_PROMPT
    assert "untrusted data" in SYSTEM_PROMPT


@pytest.mark.parametrize(
    "rule",
    [
        "never present\none system's procedure as another system's",  # q01
        "never move a step from one phase",  # q12
        "even when the\nwording differs",  # q09
        "cite only an excerpt that explicitly states that\nclaim",  # q13
        "Do not add recommendations, conditions or rules that no excerpt states",  # q13
    ],
)
def test_prompt_contains_the_rule_for_each_live_failure(rule: str) -> None:
    assert rule in SYSTEM_PROMPT


# ---------------------------------------------------------------- excerpt labels (q01, q12)


def test_excerpt_headers_name_the_document_system_and_section() -> None:
    context, _ = build_context(
        [_source("06_change_management.md", "Before deployment"),
         _source("06_change_management.md", "Validation")], 6000, TITLES,
    )
    assert ("[1] SOURCE: 06_change_management.md#before-deployment | DOCUMENT: Meridian Change and "
            "Deployment Validation Procedure | SECTION: Before deployment") in context
    assert "| SECTION: Validation\n" in context


def test_headers_fall_back_to_the_file_name_without_titles() -> None:
    context, _ = build_context([_source("01_batch_job_recovery.md", "Scope")], 6000)
    assert "DOCUMENT: 01_batch_job_recovery.md" in context


# ---------------------------------------------------------------- system scoping (q01, q09)


def test_q01_orion_question_keeps_only_orion_excerpts() -> None:
    model = Recorder()
    service = RagService(Fixed(Q01_RETRIEVED), model, titles=TITLES)
    answer = service.answer("What evidence must be captured before recovering the Orion nightly batch?")
    assert [s.source_name for s in answer.sources] == ["01_batch_job_recovery.md"]
    assert answer.diagnostics["scope"] == {"systems": ["Orion"], "dropped": 3, "applied": True}
    assert "Aurora" not in model.prompts[0] and "Beacon" not in model.prompts[0]


def test_q09_aurora_question_keeps_the_answering_excerpt_first() -> None:
    model = Recorder()
    question = "Which reconciliations are required before Aurora resumes downstream processing?"
    answer = RagService(Fixed(Q09_RETRIEVED), model, titles=TITLES).answer(question)
    assert answer.sources[0].section_ref == "04_pipeline_failure.md#verification"
    assert {s.source_name for s in answer.sources} == {"04_pipeline_failure.md"}
    assert "source-to-Bronze counts" in model.prompts[0]


def test_questions_naming_no_system_are_not_filtered() -> None:
    # q13 names no system; its answer legitimately needs two documents.
    scope = SystemScope(TITLES)
    kept, info = scope.apply("A deployment is slow; should we resize compute or roll back?", Q01_RETRIEVED)
    assert kept == Q01_RETRIEVED and info == {}


def test_scope_never_empties_the_context() -> None:
    kept, info = SystemScope(TITLES).apply("Is Meridian affected?", Q09_RETRIEVED)
    assert kept == Q09_RETRIEVED
    assert info == {"systems": ["Meridian"], "dropped": 0, "applied": False}


def test_scope_matches_whole_words_case_insensitively() -> None:
    scope = SystemScope(TITLES)
    assert scope.apply("how does ORION recover?", Q01_RETRIEVED)[1]["systems"] == ["Orion"]
    assert scope.apply("Is Orionx documented?", Q01_RETRIEVED)[1] == {}


def test_scope_can_name_two_systems() -> None:
    kept, info = SystemScope(TITLES).apply("Compare Orion and Northstar escalation", Q01_RETRIEVED)
    assert info["systems"] == ["Northstar", "Orion"]
    assert {s.source_name for s in kept} == {"01_batch_job_recovery.md", "02_incident_escalation.md"}


# ---------------------------------------------------------------- refusals stay intact


def test_q17_unanswerable_atlas_question_still_reaches_the_refusal() -> None:
    retrieved = [_source("03_sql_performance.md", "Evidence first", 1.0),
                 _source("01_batch_job_recovery.md", "Scope")]
    model = Recorder(INSUFFICIENT)
    answer = RagService(Fixed(retrieved), model, titles=TITLES).answer(
        "Which Kubernetes namespace hosts the Atlas service?"
    )
    assert answer.answer == INSUFFICIENT and answer.error is None
    assert {s.source_name for s in answer.sources} == {"03_sql_performance.md"}
    case = EvaluationCase("q17", "namespace?", "unanswerable", (), ())
    assert score_answer(case, answer, "live-rag").answer_correctness == 1.0


def _offline_unanswerable(use_titles: bool) -> dict:
    service = build_offline_service(use_titles=use_titles)
    cases = load_cases(ROOT / "config" / "evaluation_questions.yml")
    scores = [score_answer(case, service.answer(case.question), "offline-rag") for case in cases]
    return summarize_by_category(scores)["unanswerable"]


def test_system_scope_does_not_reduce_offline_refusals() -> None:
    # Before Phase A the offline fixture "answered" q17 by quoting the Atlas title chunk, with or
    # without scoping. Title-only chunks are now dropped (see test_phase_a_privacy_and_retrieval);
    # this test keeps guarding that scoping never reduces refusals.
    with_scope, without_scope = _offline_unanswerable(True), _offline_unanswerable(False)
    assert with_scope["answer_correctness"] >= without_scope["answer_correctness"]
    assert with_scope["refusals"].get("clean", 0) >= without_scope["refusals"].get("clean", 0)
    assert with_scope["refusals"].get("clean", 0) >= 2  # q14 and q15


def test_offline_extractive_answers_still_parse_the_new_headers() -> None:
    answer = build_offline_service().answer("What are the Orion recovery stop conditions?")
    assert answer.sources and re.search(r"\[\d+]", answer.answer)
    assert {s.source_name for s in answer.sources} == {"01_batch_job_recovery.md"}


# ---------------------------------------------------------------- weak citations (q13)


def _answer(text: str, sources: list[SearchResult]) -> RagAnswer:
    return RagAnswer("q", text, tuple(sources), "m", 1)


def test_q13_unsupported_rollback_rule_is_flagged() -> None:
    sources = [_source("06_change_management.md", "Deployment controls"),
               _source("03_sql_performance.md", "Safe actions")]
    text = ("Only if the query profile shows a true compute bottleneck should you consider resizing "
            "the warehouse, and that resize must be approved by the cost owner [2]. If the evidence "
            "does not point to a compute issue, the safer course is to roll the deployment back to "
            "the last known good state [1].")
    weak = weak_citation_sentences(_answer(text, sources))
    assert len(weak) == 1 and weak[0].startswith("If the evidence does not point")
    case = EvaluationCase("q13", "slow?", "ambiguous",
                          ("03_sql_performance.md", "06_change_management.md"), ("query profile",))
    score = score_answer(case, _answer(text, sources), "live-rag")
    assert score.weak_citations == 1 and "weak_citations:1:review" in score.review_flags


@pytest.mark.parametrize(
    "text, document, heading",
    [
        ("The Incident Commander must be notified within **10 minutes** of a SEV-1 event. [1]",
         "02_incident_escalation.md", "Escalation timing"),
        ("Failed run details - record the failed run ID, the timestamp of the failure, and the last "
         "successful task that completed before the failure. [1]", "01_batch_job_recovery.md", "Scope"),
        ("Lab 12 monitoring records are retained for **30 days** before they are removed under the "
         "approved retention process. [1]", "05_monitoring_alerting.md", "Data protection"),
    ],
)
def test_correct_live_citations_are_not_flagged(text: str, document: str, heading: str) -> None:
    assert weak_citation_sentences(_answer(text, [_source(document, heading)])) == []


def test_weak_citations_do_not_apply_to_the_baseline() -> None:
    case = EvaluationCase("q04", "SEV-1?", "answerable", ("02_incident_escalation.md",), ("10 minutes",))
    score = score_answer(case, _answer("Notify within 5 minutes.", []), "live-baseline")
    assert score.weak_citations is None
