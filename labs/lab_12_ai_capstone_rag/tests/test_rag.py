from __future__ import annotations

from lab12.models import SearchResult
from lab12.rag import INSUFFICIENT, RagService, build_context


class Retriever:
    def __init__(self, results):
        self.results = results

    def search(self, question, *, top_k, filters=None):
        return self.results[:top_k]


class Model:
    model_name = "fixture-model"

    def complete(self, *, system_prompt, user_prompt):
        assert "untrusted data" in system_prompt
        assert "QUESTION:" in user_prompt
        return "Use an isolated checkpoint [1]."


class BrokenModel(Model):
    def complete(self, *, system_prompt, user_prompt):
        raise TimeoutError("secret provider detail")


def _result() -> SearchResult:
    return SearchResult("c1", "d1", "guide.md", "Replay", "guide.md#replay", "Use an isolated checkpoint.", 0.9)


def test_zero_results_returns_insufficient() -> None:
    answer = RagService(Retriever([]), Model()).answer("Unknown?")
    assert answer.answer == INSUFFICIENT
    assert answer.sources == ()


def test_grounded_answer_preserves_sources() -> None:
    answer = RagService(Retriever([_result()]), Model()).answer("Replay?")
    assert "[1]" in answer.answer
    assert answer.sources[0].section_ref == "guide.md#replay"


def test_model_errors_are_sanitized() -> None:
    answer = RagService(Retriever([_result()]), BrokenModel()).answer("Replay?")
    assert answer.error == "TimeoutError"
    assert "secret" not in answer.answer


def test_context_limit() -> None:
    context, sources = build_context([_result()], 85)
    assert len(context) <= 85
    assert len(sources) == 1

