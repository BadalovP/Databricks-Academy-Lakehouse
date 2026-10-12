"""UrbanFlow-specific RAG behaviour: domain prompt, opt-in scoping, offline retrieval quality.

All numbers here come from the deterministic OFFLINE fixture (local retriever and extractive
answerer), not from a language model; they guard retrieval and scoping logic only.
"""

from __future__ import annotations

from pathlib import Path

from urbanflow.rag.cli import build_offline_service
from urbanflow.rag.evaluation import evaluate, load_cases, summarize
from urbanflow.rag.models import SearchResult
from urbanflow.rag.rag import BASELINE_SYSTEM_PROMPT, SYSTEM_PROMPT, SystemScope

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _cases():
    return load_cases(PROJECT_ROOT / "config" / "rag_evaluation_questions.yml")


def test_prompts_describe_the_mobility_domain_not_lab_12() -> None:
    for prompt in (SYSTEM_PROMPT, BASELINE_SYSTEM_PROMPT):
        assert "mobility operations" in prompt
        assert "IT operations" not in prompt
    assert "The available documents do not contain enough information" in SYSTEM_PROMPT


def _result(source: str) -> SearchResult:
    return SearchResult("c", "d", source, "Heading", f"{source}#h", "text", 1.0)


def test_scope_is_off_unless_systems_are_configured() -> None:
    sources = [_result("01_station_rebalancing.md"), _result("04_data_quality.md")]
    kept, info = SystemScope({}).apply("What happens to a station event?", sources)
    assert kept == sources and info == {}


def test_title_word_scoping_would_drop_the_answering_document() -> None:
    """Why scoping is opt-in: 'station' in a data-quality question is not a system name."""
    titles = {
        "01_station_rebalancing.md": "Station Rebalancing Procedure",
        "04_data_quality.md": "Data Quality and Quarantine",
    }
    sources = [_result("01_station_rebalancing.md"), _result("04_data_quality.md")]
    kept, _ = SystemScope.from_title_first_word(titles).apply(
        "What happens to a station event with a missing station identifier?", sources
    )
    assert [s.source_name for s in kept] == ["01_station_rebalancing.md"]


def test_the_offline_service_does_not_scope_by_title_words() -> None:
    service = build_offline_service()
    question = "What happens to a station event with a missing station identifier?"
    answer = service.answer(question)
    assert "04_data_quality.md" in {source.source_name for source in answer.sources}


def test_offline_retrieval_finds_the_expected_document_for_nearly_every_question() -> None:
    summary = summarize(evaluate(_cases(), build_offline_service().answer, "offline-rag"))
    assert summary["questions"] == 18
    assert summary["errors"] == 0
    assert summary["invalid_citations"] == 0
    assert summary["retrieval_recall"] >= 0.9
    assert summary["source_coverage"] >= 0.85
    assert summary["refusals"].get("clean", 0) >= 2  # both unanswerable questions refuse
