"""Shared, dependency-free domain models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Section:
    heading: str
    text: str


@dataclass(frozen=True)
class Document:
    document_id: str
    title: str
    source_name: str
    source_path: str
    content: str
    content_hash: str
    sections: tuple[Section, ...]
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    document_id: str
    source_name: str
    section_heading: str
    section_ref: str
    ordinal: int
    content: str
    content_hash: str
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SearchResult:
    chunk_id: str
    document_id: str
    source_name: str
    section_heading: str
    section_ref: str
    content: str
    score: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RagAnswer:
    question: str
    answer: str
    sources: tuple[SearchResult, ...]
    model: str
    latency_ms: int
    error: str | None = None
    # Operational metadata only (timings, token counts, finish reason) - never prompt or answer text.
    diagnostics: dict[str, Any] = field(default_factory=dict)
