"""Section-aware, deterministic character chunking."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .models import Chunk, Document


@dataclass(frozen=True)
class ChunkingConfig:
    max_chars: int = 900
    overlap_chars: int = 120
    min_chunk_chars: int = 40

    def __post_init__(self) -> None:
        if self.max_chars <= 0 or self.min_chunk_chars <= 0:
            raise ValueError("Chunk sizes must be positive.")
        if not 0 <= self.overlap_chars < self.max_chars:
            raise ValueError("overlap_chars must be smaller than max_chars.")


def chunk_documents(documents: list[Document], config: ChunkingConfig) -> list[Chunk]:
    """Chunk documents and suppress exact duplicate chunks across the corpus."""
    chunks: list[Chunk] = []
    seen_content: set[str] = set()
    for document in documents:
        ordinal = 0
        for section in document.sections:
            for piece in _split_text(section.text, config):
                content_hash = hashlib.sha256(piece.encode("utf-8")).hexdigest()
                if content_hash in seen_content:
                    continue
                seen_content.add(content_hash)
                section_ref = f"{document.source_name}#{_slug(section.heading)}"
                identity = f"{document.document_id}|{section_ref}|{ordinal}|{content_hash}"
                chunk_id = f"chk_{hashlib.sha256(identity.encode()).hexdigest()[:24]}"
                chunks.append(
                    Chunk(
                        chunk_id=chunk_id,
                        document_id=document.document_id,
                        source_name=document.source_name,
                        section_heading=section.heading,
                        section_ref=section_ref,
                        ordinal=ordinal,
                        content=piece,
                        content_hash=content_hash,
                        metadata=document.metadata,
                    )
                )
                ordinal += 1
    return chunks


def _split_text(text: str, config: ChunkingConfig) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if len(text) <= config.max_chars:
        return [text]
    chunks: list[str] = []
    start = 0
    while start < len(text):
        hard_end = min(start + config.max_chars, len(text))
        end = hard_end
        if hard_end < len(text):
            boundary_floor = start + max(config.min_chunk_chars, config.max_chars // 2)
            candidates = [
                text.rfind("\n\n", boundary_floor, hard_end),
                text.rfind(". ", boundary_floor, hard_end),
            ]
            boundary = max(candidates)
            if boundary >= boundary_floor:
                end = boundary + (1 if text[boundary] == "." else 0)
        piece = text[start:end].strip()
        if piece:
            chunks.append(piece)
        if end >= len(text):
            break
        next_start = max(end - config.overlap_chars, start + 1)
        while next_start < end and not text[next_start].isspace():
            next_start += 1
        start = next_start if next_start < end else end
    if len(chunks) > 1 and len(chunks[-1]) < config.min_chunk_chars:
        chunks[-2] = f"{chunks[-2]} {chunks[-1]}".strip()
        chunks.pop()
    return chunks


def _slug(value: str) -> str:
    slug = "".join(character.lower() if character.isalnum() else "-" for character in value)
    return "-".join(part for part in slug.split("-") if part) or "section"
