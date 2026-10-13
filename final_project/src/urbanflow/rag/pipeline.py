"""Notebook-level orchestration for the UrbanFlow 2.0 RAG Job, kept out of the notebooks.

Each function is one Job step: ingest and reconcile the corpus, provision or inspect the AI Search
index, build the live assistant, and summarise or persist an evaluation. The behaviour is the one
proven in the Academy Lab 12 capstone; only the corpus and the target tables differ.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .chunking import ChunkingConfig, chunk_documents
from .config import ProjectConfig as RagConfig
from .delta_store import (
    EVALUATION_SCHEMA,
    MONITORING_SCHEMA,
    append_rows,
    change_data_feed_enabled,
    create_lab_tables,
    latest_version,
    merge_chunks,
    merge_documents,
    qualified_name,
    snapshot_fingerprint,
    table_snapshot,
)
from .evaluation import (
    baseline_answer,
    citation_map,
    evaluate,
    score_answer,
    score_to_row,
    summarize,
    summarize_by_category,
    summarize_generation,
)
from .ingestion import document_titles, load_documents, section_headings
from .monitoring import event_from_answer, event_to_row
from .runtime import index_name, source_table
from .vector_search import DeltaSyncIndexSpec


def load_corpus(cfg: RagConfig, project_root: Path) -> tuple[list, list]:
    """Documents and chunks of the knowledge base; an empty corpus is an error."""
    settings = cfg.chunking
    documents = load_documents([project_root / "knowledge_base"])
    chunks = chunk_documents(
        documents,
        ChunkingConfig(settings.max_chars, settings.overlap_chars, settings.min_chunk_chars),
    )
    if not documents or not chunks:
        raise RuntimeError("The corpus produced no documents or chunks; refusing to continue.")
    return documents, chunks


def ingest_and_reconcile(
    spark: Any, cfg: RagConfig, catalog: str, schema: str, documents: list, chunks: list
) -> dict[str, Any]:
    """MERGE one full corpus snapshot and reconcile every table against the local corpus.

    Reconciliation checks row count, unique keys, a fingerprint of every (key, content_hash) pair
    and populated ``ingested_at``; metrics not reported by Delta stay "unavailable", never 0.
    """
    tables = {name: str(cfg.raw["tables"][name]) for name in cfg.raw["tables"]}
    create_lab_tables(spark, catalog=catalog, schema=schema, tables=tables, allow_create=True)
    targets = {
        "documents": (
            qualified_name(catalog, schema, tables["documents"]),
            "document_id",
            documents,
        ),
        "chunks": (qualified_name(catalog, schema, tables["chunks"]), "chunk_id", chunks),
    }
    merges = {
        "documents": merge_documents(spark, targets["documents"][0], documents, allow_write=True),
        "chunks": merge_chunks(spark, targets["chunks"][0], chunks, allow_write=True),
    }
    report: dict[str, Any] = {}
    problems: list[str] = []
    for name, (table, key, items) in targets.items():
        snapshot = table_snapshot(spark, table, key)
        expected = snapshot_fingerprint(items, key)
        report[name] = {
            "merge": merges[name],
            "table": snapshot,
            "expected_rows": len(items),
            "fingerprint_matches": snapshot["fingerprint"] == expected,
        }
        if snapshot["row_count"] != len(items) or snapshot["distinct_keys"] != len(items):
            problems.append(
                f"{name}: {snapshot['row_count']} rows / {snapshot['distinct_keys']} keys"
            )
        if snapshot["fingerprint"] != expected:
            problems.append(f"{name}: (key, content_hash) fingerprint differs from the corpus")
        if snapshot["null_ingested_at"]:
            problems.append(f"{name}: {snapshot['null_ingested_at']} rows without ingested_at")
    report["chunks"]["change_data_feed"] = change_data_feed_enabled(spark, targets["chunks"][0])
    if not report["chunks"]["change_data_feed"]:
        problems.append("chunks: Change Data Feed is not enabled")
    if problems:
        raise RuntimeError(f"Reconciliation failed: {problems}")
    return {"status": "PASS", **report}


def existing_table_baseline(spark: Any, cfg: RagConfig, catalog: str, schema: str) -> dict:
    """Read-only versions and snapshots of existing tables, so later steps can prove no change."""
    tables = {name: str(cfg.raw["tables"][name]) for name in cfg.raw["tables"]}
    existing: dict[str, dict[str, Any]] = {}
    for name, table_name in tables.items():
        if spark.catalog.tableExists(f"{catalog}.{schema}.{table_name}"):
            existing[name] = {
                "version": latest_version(spark, qualified_name(catalog, schema, table_name))
            }
    for name, key in (("documents", "document_id"), ("chunks", "chunk_id")):
        if name in existing:
            existing[name].update(
                table_snapshot(spark, qualified_name(catalog, schema, tables[name]), key)
            )
    return existing


def table_versions(spark: Any, cfg: RagConfig, catalog: str, schema: str) -> dict[str, Any]:
    return {
        name: latest_version(spark, qualified_name(catalog, schema, str(table)))
        for name, table in cfg.raw["tables"].items()
    }


def index_spec(
    cfg: RagConfig,
    catalog: str,
    schema: str,
    *,
    endpoint: str,
    embedding_endpoint: str,
    index: str,
) -> DeltaSyncIndexSpec:
    """The Delta Sync index plan; source table and index always come from THIS target's schema."""
    if not endpoint.strip() or not embedding_endpoint.strip():
        raise ValueError("ai_search_endpoint and embedding_endpoint are required.")
    search = cfg.raw["ai_search"]
    return DeltaSyncIndexSpec(
        endpoint_name=endpoint.strip(),
        index_name=index_name(catalog, schema, index),
        source_table_name=source_table(catalog, schema, str(cfg.raw["tables"]["chunks"])),
        embedding_model_endpoint_name=embedding_endpoint.strip(),
        primary_key=search["primary_key"],
        embedding_source_column=search["embedding_source_column"],
        columns_to_sync=tuple(search["columns_to_sync"]),
    )


def provision_index(
    spark: Any, cfg: RagConfig, catalog: str, schema: str, manager: Any, spec: DeltaSyncIndexSpec
) -> dict[str, Any]:
    """Create or sync the endpoint and index (billable), then wait until it holds every row.

    Waits are bounded: endpoint <= 25 min, then indexing <= 25 min, inside a 60 min task timeout.
    Table versions are read afterwards to show indexing wrote to no project table.
    """
    expected_rows = spark.table(spec.source_table_name).count()
    if expected_rows == 0:
        raise RuntimeError(f"{spec.source_table_name} is empty; ingest before indexing.")
    report = manager.provision(
        spec,
        expected_rows=expected_rows,
        allow_mutation=True,
        endpoint_timeout_seconds=1500,
        index_timeout_seconds=1500,
    )
    return {
        "status": "READY",
        "source_rows": expected_rows,
        **report,
        "table_versions_after": table_versions(spark, cfg, catalog, schema),
    }


def inspect_index(spark: Any, manager: Any, spec: DeltaSyncIndexSpec) -> dict[str, Any]:
    """Read-only inspection plus a readiness gate for an existing index.

    An existing index must be ready and hold every source row, so evaluation never queries a
    half-built or stale index; ``manager.verify`` raises otherwise and stops the Job.
    """
    report = manager.inspect(spec)
    result: dict[str, Any] = {
        "status": "DRY_RUN",
        "index": spec.index_name,
        "endpoint_status": report["endpoint_status"],
        "index_status": report["index_status"],
    }
    if report["index_status"] == "present":
        source_rows = spark.table(spec.source_table_name).count()
        readiness = manager.verify(
            spec, expected_rows=source_rows, timeout_seconds=120, poll_seconds=15
        )
        result.update(status="READY_READ_ONLY", source_rows=source_rows, readiness=readiness)
    return result


def build_live_service(
    cfg: RagConfig,
    project_root: Path,
    *,
    search_client: Any,
    ai_search_endpoint: str,
    index: str,
    chat_endpoint: str,
) -> tuple[Any, Any]:
    """The live RAG assistant and its chat model, built only for approved inference."""
    from .rag import DatabricksChatModel, RagService
    from .retrieval import DatabricksAISearchRetriever

    generation = cfg.raw["generation"]
    model = DatabricksChatModel(
        chat_endpoint,
        temperature=float(generation["temperature"]),
        max_tokens=int(generation["max_tokens"]),
    )
    retriever = DatabricksAISearchRetriever(
        search_client,
        ai_search_endpoint,
        index,
        query_type=cfg.raw["retrieval"].get("query_type", "HYBRID"),
    )
    corpus = load_documents([project_root / "knowledge_base"])
    service = RagService(
        retriever,
        model,
        top_k=cfg.retrieval.top_k,
        max_context_chars=cfg.retrieval.max_context_chars,
        titles=document_titles(corpus),
        headings=section_headings(corpus),
        max_supplemental_queries=int(cfg.raw["retrieval"].get("max_supplemental_queries", 0)),
    )
    return service, model


def offline_summary(cases: list, service: Any, include_baseline: bool) -> dict[str, Any]:
    """Deterministic fixture evaluation: no AI Search request and no model call."""
    baseline = (
        [score_answer(case, baseline_answer(case), "offline-baseline") for case in cases]
        if include_baseline
        else []
    )
    rag = evaluate(cases, service.answer, "offline-rag")
    return {
        "mode": "offline",
        "question_ids": [case.question_id for case in cases],
        "baseline": summarize(baseline) if include_baseline else "not_run",
        "rag": summarize(rag),
        "by_category": {
            "baseline": summarize_by_category(baseline) if include_baseline else "not_run",
            "rag": summarize_by_category(rag),
        },
    }


def live_summary(run: Any, cases: list, include_baseline: bool, **context: Any) -> dict[str, Any]:
    """Baseline-versus-RAG summary of one live evaluation run."""
    return {
        "mode": "live",
        **context,
        "question_ids": [case.question_id for case in cases],
        "baseline": summarize(run.baseline_scores) if include_baseline else "not_run",
        "rag": summarize(run.rag_scores),
        "by_category": {
            "baseline": (
                summarize_by_category(run.baseline_scores) if include_baseline else "not_run"
            ),
            "rag": summarize_by_category(run.rag_scores),
        },
        "generation": {
            "baseline": (
                summarize_generation(run.baseline_answers) if include_baseline else "not_run"
            ),
            "rag": summarize_generation(run.rag_answers),
        },
    }


def review_records(run: Any, cases: list, include_baseline: bool) -> list[dict[str, Any]]:
    """Per-question review of grounding and citations (questions and corpus are fictional)."""
    records = []
    for position, (case, answer, score) in enumerate(
        zip(cases, run.rag_answers, run.rag_scores, strict=True)
    ):
        baseline = "not_run"
        if include_baseline:
            base = run.baseline_answers[position]
            baseline = {
                "answer": base.answer,
                "error": base.error,
                "latency_ms": base.latency_ms,
                "diagnostics": base.diagnostics,
                "score": score_to_row(run.baseline_scores[position], "review", "n/a")[
                    "metrics_json"
                ],
            }
        records.append(
            {
                "question_id": case.question_id,
                "category": case.category,
                "question": case.question,
                "expected_sources": list(case.expected_sources),
                "retrieved": [
                    {"rank": rank, "source": s.source_name, "section": s.section_ref,
                     "score": round(s.score, 4), "chunk_id": s.chunk_id}
                    for rank, s in enumerate(answer.sources, start=1)
                ],  # fmt: skip
                "rag": {
                    "answer": answer.answer,
                    "citations": citation_map(answer),
                    "error": answer.error,
                    "latency_ms": answer.latency_ms,
                    "diagnostics": answer.diagnostics,
                    "score": score_to_row(score, "review", "n/a")["metrics_json"],
                },
                "baseline": baseline,
            }
        )
    return records


def persist_evaluation(
    spark: Any, cfg: RagConfig, catalog: str, schema: str, run: Any, index: str
) -> dict[str, Any]:
    """Append scores and redacted monitoring events of one live evaluation."""
    evaluation_id = str(uuid.uuid4())
    evaluated_at = datetime.now(UTC).isoformat()
    tables = cfg.raw["tables"]
    evaluation_rows = [
        score_to_row(score, evaluation_id, evaluated_at)
        for score in run.baseline_scores + run.rag_scores
    ]
    monitoring_rows = [event_to_row(event_from_answer(answer, index)) for answer in run.rag_answers]
    written = {
        "evaluation_results": append_rows(
            spark,
            qualified_name(catalog, schema, tables["evaluation_results"]),
            evaluation_rows,
            schema=EVALUATION_SCHEMA,
            timestamp_column="evaluated_at",
            allow_write=True,
        ),
        "query_monitoring": append_rows(
            spark,
            qualified_name(catalog, schema, tables["monitoring"]),
            monitoring_rows,
            schema=MONITORING_SCHEMA,
            timestamp_column="query_timestamp",
            allow_write=True,
        ),
    }
    return {"evaluation_id": evaluation_id, "rows_written": written}
