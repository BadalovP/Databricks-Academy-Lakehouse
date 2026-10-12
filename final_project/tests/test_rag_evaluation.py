from __future__ import annotations

from urbanflow.rag.evaluation import baseline_answer, load_cases, score_answer, summarize
from urbanflow.rag.models import RagAnswer, SearchResult
from urbanflow.rag.rag import INSUFFICIENT


def test_suite_covers_every_knowledge_base_topic_and_refusals(project_root) -> None:
    cases = load_cases(project_root / "config" / "rag_evaluation_questions.yml")
    documents = {path.name for path in (project_root / "knowledge_base").glob("*.md")}

    assert len(cases) == 18
    assert len({case.question_id for case in cases}) == 18
    unanswerable = [case for case in cases if case.category == "unanswerable"]
    assert len(unanswerable) >= 2
    assert all(not case.expected_sources for case in unanswerable)
    answerable = [case for case in cases if case.category != "unanswerable"]
    # Every answerable question cites a real document, and every document is exercised.
    assert all(case.expected_sources for case in answerable)
    cited = {source for case in answerable for source in case.expected_sources}
    assert cited <= documents
    assert cited == documents


def test_unanswerable_baseline_scores_as_correct(project_root) -> None:
    case = next(
        item
        for item in load_cases(project_root / "config" / "rag_evaluation_questions.yml")
        if item.category == "unanswerable"
    )
    score = score_answer(case, baseline_answer(case), "baseline")
    assert score.answer_correctness == 1.0
    assert INSUFFICIENT in baseline_answer(case).answer


def test_citation_and_keyword_metrics() -> None:
    from urbanflow.rag.evaluation import EvaluationCase

    case = EvaluationCase("q", "question", "answerable", ("guide.md",), ("retry", "once"))
    source = SearchResult("c", "d", "guide.md", "H", "guide.md#h", "retry once", 1.0)
    answer = RagAnswer("question", "Retry once [1].", (source,), "model", 1)
    score = score_answer(case, answer, "rag")
    assert score.retrieval_recall == 1.0
    assert score.answer_correctness == 1.0
    assert score.citation_precision == 1.0 and score.source_coverage == 1.0


def test_summary_empty() -> None:
    assert summarize([]) == {"questions": 0}
