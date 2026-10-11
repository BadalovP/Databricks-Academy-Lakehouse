"""Offline-safe command-line demonstration."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .chunking import ChunkingConfig, chunk_documents
from .config import load_config
from .evaluation import baseline_answer, evaluate, load_cases, score_answer, summarize
from .ingestion import document_titles, load_documents, section_headings
from .rag import ExtractiveOfflineModel, RagService
from .retrieval import OfflineLexicalRetriever


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def build_offline_service(*, use_titles: bool = True) -> RagService:
    root = project_root()
    cfg = load_config(root / "config" / "default.yml")
    documents = load_documents([root / "data" / "sample_docs"])
    settings = cfg.chunking
    chunks = chunk_documents(
        documents,
        ChunkingConfig(settings.max_chars, settings.overlap_chars, settings.min_chunk_chars),
    )
    return RagService(
        OfflineLexicalRetriever(chunks, cfg.retrieval.score_threshold),
        ExtractiveOfflineModel(),
        top_k=cfg.retrieval.top_k,
        max_context_chars=cfg.retrieval.max_context_chars,
        titles=document_titles(documents) if use_titles else None,
        headings=section_headings(documents) if use_titles else None,
        max_supplemental_queries=int(cfg.raw["retrieval"].get("max_supplemental_queries", 0)),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    ask = subparsers.add_parser("ask", help="Run the deterministic offline RAG fixture")
    ask.add_argument("question")
    subparsers.add_parser("evaluate", help="Compare no-retrieval and offline RAG fixtures")
    arguments = parser.parse_args(argv)
    service = build_offline_service()
    if arguments.command == "ask":
        answer = service.answer(arguments.question)
        print(answer.answer)
        for source in answer.sources:
            print(f"- {source.section_ref} (score={source.score:.2f})")
        return 0
    cases = load_cases(project_root() / "config" / "evaluation_questions.yml")
    baseline = [baseline_answer(case) for case in cases]
    payload = {
        "baseline": summarize(
            [
                score_answer(case, answer, "baseline")
                for case, answer in zip(cases, baseline, strict=True)
            ]
        ),
        "rag": summarize(evaluate(cases, service.answer, "offline-rag")),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
