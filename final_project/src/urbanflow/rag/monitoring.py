"""Privacy-conscious structured query monitoring."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from .models import RagAnswer

logger = logging.getLogger("urbanflow_final.rag")


@dataclass(frozen=True)
class QueryEvent:
    event_id: str
    query_timestamp: str
    question_hash: str
    question_chars: int
    model_id: str
    index_id: str
    retrieved_chunk_ids: tuple[str, ...]
    latency_ms: int
    error_status: str | None
    evaluation_json: str | None = None


# Persisted diagnostics use an allowlist: a key not listed here never reaches a Delta table.
# Stage 5C found the system names a question mentions (scope.systems) persisted in monitoring;
# the full diagnostics stay in the Job run output for review, never in the tables.
_NUMERIC_DIAGNOSTICS = (
    "generation_ms",
    "retrieval_ms",
    "sources",
    "sources_in_context",
    "context_chars",
    "prompt_tokens",
    "completion_tokens",
    "reasoning_tokens",
    "total_tokens",
    "max_tokens",
    "answer_chars",
    "preamble_dropped",
    "supplemental_queries",
    "supplemental_added",
)
_BOOLEAN_DIAGNOSTICS = ("empty_response", "truncated")
_FINISH_REASONS = {"stop", "length", "content_filter", "tool_calls", "function_call", "unavailable"}
_BLOCK_TYPES = {"text", "output_text", "reasoning", "refusal", "string"}


def safe_diagnostics(diagnostics: dict[str, Any]) -> dict[str, Any]:
    """The subset of answer diagnostics that may be persisted: counts, timings, flags and fixed
    enumerations. Free text and query-derived strings (such as scope.systems) are dropped."""
    safe: dict[str, Any] = {}
    for key in _NUMERIC_DIAGNOSTICS:
        value = diagnostics.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int | float) or value == "unavailable":
            safe[key] = value
    for key in _BOOLEAN_DIAGNOSTICS:
        if isinstance(diagnostics.get(key), bool):
            safe[key] = diagnostics[key]
    if "finish_reason" in diagnostics:
        reason = diagnostics["finish_reason"]
        safe["finish_reason"] = reason if reason in _FINISH_REASONS else "other"
    if isinstance(diagnostics.get("content_blocks"), list):
        safe["content_blocks"] = [
            block if block in _BLOCK_TYPES else "other" for block in diagnostics["content_blocks"]
        ]
    if "error" in diagnostics:
        error = str(diagnostics["error"])
        safe["error"] = error if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,80}", error) else "Error"
    scope = diagnostics.get("scope")
    if isinstance(scope, dict):
        safe["scope"] = {
            "applied": bool(scope.get("applied")),
            "dropped": int(scope.get("dropped", 0)),
        }
    return safe


def event_from_answer(answer: RagAnswer, index_id: str) -> QueryEvent:
    """Capture identifiers and metrics, never raw question, context, or answer text."""
    return QueryEvent(
        event_id=str(uuid.uuid4()),
        query_timestamp=datetime.now(UTC).isoformat(),
        question_hash=hashlib.sha256(answer.question.encode()).hexdigest(),
        question_chars=len(answer.question),
        model_id=answer.model,
        index_id=index_id,
        retrieved_chunk_ids=tuple(source.chunk_id for source in answer.sources),
        latency_ms=answer.latency_ms,
        error_status=answer.error,
        # Allowlisted counts, timings and enumerations only (see safe_diagnostics).
        evaluation_json=(
            json.dumps({"diagnostics": safe_diagnostics(answer.diagnostics)}, sort_keys=True)
            if answer.diagnostics
            else None
        ),
    )


def log_event(event: QueryEvent) -> None:
    logger.info(json.dumps(asdict(event), sort_keys=True, separators=(",", ":")))


def event_to_row(event: QueryEvent) -> dict[str, object]:
    """Row for the query_monitoring Delta table; still no raw question, context or answer."""
    row = asdict(event)
    row["retrieved_chunk_ids"] = list(event.retrieved_chunk_ids)
    return row
