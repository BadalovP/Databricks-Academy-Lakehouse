from __future__ import annotations

from lab12.evaluation import baseline_answer, load_cases, score_answer, summarize
from lab12.models import RagAnswer, SearchResult
from lab12.rag import INSUFFICIENT


def test_suite_has_required_categories(project_root) -> None:
    cases = load_cases(project_root / "config" / "evaluation_questions.yml")
    assert len(cases) == 18
    assert {case.category for case in cases} == {"answerable", "unanswerable", "ambiguous"}


def test_unanswerable_baseline_scores_as_correct(project_root) -> None:
    case = next(
        item
        for item in load_cases(project_root / "config" / "evaluation_questions.yml")
        if item.category == "unanswerable"
    )
    score = score_answer(case, baseline_answer(case), "baseline")
    assert score.answer_correctness == 1.0
    assert INSUFFICIENT in baseline_answer(case).answer


def test_citation_and_keyword_metrics() -> None:
    from lab12.evaluation import EvaluationCase

    case = EvaluationCase("q", "question", "answerable", ("guide.md",), ("retry", "once"))
    source = SearchResult("c", "d", "guide.md", "H", "guide.md#h", "retry once", 1.0)
    answer = RagAnswer("question", "Retry once [1].", (source,), "model", 1)
    score = score_answer(case, answer, "rag")
    assert score.retrieval_recall == 1.0
    assert score.answer_correctness == 1.0
    assert score.citation_precision == 1.0 and score.source_coverage == 1.0


def test_summary_empty() -> None:
    assert summarize([]) == {"questions": 0}

