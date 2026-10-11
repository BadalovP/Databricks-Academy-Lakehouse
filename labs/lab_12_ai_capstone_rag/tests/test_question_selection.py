"""Targeted evaluation runs: exact question selection and an optional RAG-only mode.

The defaults must keep the full 18-question baseline-vs-RAG evaluation; a targeted run must
evaluate exactly the requested IDs and, with the baseline off, make no baseline call at all.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from lab12.evaluation import live_call_plan, load_cases, run_evaluation, select_cases
from lab12.models import RagAnswer

ROOT = Path(__file__).resolve().parents[1]
CASES = load_cases(ROOT / "config" / "evaluation_questions.yml")
TARGETED = "q01,q09,q12,q13"


def _ids(cases) -> list[str]:
    return [case.question_id for case in cases]


def test_no_ids_keeps_the_limit_behaviour() -> None:
    assert len(select_cases(CASES, "", 18)) == 18
    assert _ids(select_cases(CASES, "", 4)) == ["q01", "q02", "q03", "q04"]
    assert _ids(select_cases(CASES, "   ", 0)) == ["q01"]


def test_ids_select_exactly_those_questions() -> None:
    assert _ids(select_cases(CASES, TARGETED, 18)) == ["q01", "q09", "q12", "q13"]
    assert _ids(select_cases(CASES, " q13 , q01 ", 18)) == ["q01", "q13"]


def test_ids_are_not_the_first_n_questions() -> None:
    assert _ids(select_cases(CASES, TARGETED, 18)) != _ids(select_cases(CASES, "", 4))


@pytest.mark.parametrize(
    "ids, message",
    [
        ("q01,q99", "Unknown"),
        ("q01,q01", "Repeated"),
        ("q01,,q09", "Empty"),
        ("Q01", "Unknown"),
    ],
)
def test_bad_ids_are_errors(ids, message) -> None:
    with pytest.raises(ValueError, match=message):
        select_cases(CASES, ids, 18)


def test_limit_smaller_than_ids_is_an_error() -> None:
    with pytest.raises(ValueError, match="smaller"):
        select_cases(CASES, TARGETED, 3)


def test_call_plan() -> None:
    assert live_call_plan(4, include_baseline=False) == {
        "questions": 4,
        "ai_search_queries": 4,
        "rag_chat_calls": 4,
        "baseline_chat_calls": 0,
        "chat_calls": 4,
    }
    assert live_call_plan(18, include_baseline=True)["chat_calls"] == 36


class _Counter:
    def __init__(self, name: str) -> None:
        self.name, self.questions = name, []

    def __call__(self, question: str) -> RagAnswer:
        self.questions.append(question)
        return RagAnswer(question, "Not enough information.", (), self.name, 1)


def test_rag_only_run_makes_no_baseline_call() -> None:
    cases = select_cases(CASES, TARGETED, 18)
    rag = _Counter("rag")
    run = run_evaluation(cases, rag, None, "live")
    assert rag.questions == [case.question for case in cases]
    assert run.baseline_answers == [] and run.baseline_scores == []
    assert [score.variant for score in run.rag_scores] == ["live-rag"] * 4
    assert [score.question_id for score in run.rag_scores] == ["q01", "q09", "q12", "q13"]


def test_full_run_still_answers_baseline_then_rag() -> None:
    order: list[str] = []
    baseline, rag = _Counter("baseline"), _Counter("rag")

    def logged(counter: _Counter):
        def answer(question: str) -> RagAnswer:
            order.append(counter.name)
            return counter(question)

        return answer

    run = run_evaluation(CASES, logged(rag), logged(baseline), "live")
    assert order == ["baseline"] * 18 + ["rag"] * 18
    assert {score.variant for score in run.baseline_scores} == {"live-baseline"}
    assert {score.variant for score in run.rag_scores} == {"live-rag"}
    assert len(run.baseline_scores) == len(run.rag_scores) == 18


def test_notebook_and_job_share_the_defaults() -> None:
    notebook = (ROOT / "notebooks" / "04_evaluate.py").read_text(encoding="utf-8")
    assert re.search(r'widgets\.text\("eval_question_ids", ""\)', notebook)
    assert re.search(r'widgets\.dropdown\("eval_include_baseline", "true"', notebook)
    job = yaml.safe_load((ROOT / "resources" / "lab12_job.job.yml").read_text(encoding="utf-8"))
    defaults = {
        item["name"]: item["default"]
        for item in job["resources"]["jobs"]["lab12_rag_workflow"]["parameters"]
    }
    assert defaults["eval_question_ids"] == ""
    assert defaults["eval_include_baseline"] == "true"
    assert defaults["eval_question_limit"] == "18"
