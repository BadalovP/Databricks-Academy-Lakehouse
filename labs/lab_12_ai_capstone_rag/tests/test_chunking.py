from __future__ import annotations

from lab12.chunking import ChunkingConfig, chunk_documents
from lab12.models import Document, Section


def _document(text: str, document_id: str = "doc_1") -> Document:
    return Document(
        document_id=document_id,
        title="Guide",
        source_name="guide.md",
        source_path="guide.md",
        content=text,
        content_hash="hash",
        sections=(Section("Recovery Steps", text),),
    )


def test_short_section_is_one_chunk() -> None:
    chunks = chunk_documents([_document("short text")], ChunkingConfig(50, 10, 5))
    assert len(chunks) == 1
    assert chunks[0].section_ref == "guide.md#recovery-steps"


def test_boundary_and_overlap() -> None:
    text = " ".join(f"word{i}" for i in range(80))
    chunks = chunk_documents([_document(text)], ChunkingConfig(100, 20, 10))
    assert len(chunks) > 1
    assert all(len(chunk.content) <= 120 for chunk in chunks)


def test_stable_chunk_ids() -> None:
    config = ChunkingConfig(40, 5, 5)
    assert [c.chunk_id for c in chunk_documents([_document("x " * 80)], config)] == [
        c.chunk_id for c in chunk_documents([_document("x " * 80)], config)
    ]


def test_duplicate_chunks_are_suppressed() -> None:
    first = _document("identical section", "doc_1")
    second = _document("identical section", "doc_2")
    assert len(chunk_documents([first, second], ChunkingConfig(50, 5, 5))) == 1


def test_empty_section_produces_no_chunks() -> None:
    assert chunk_documents([_document("   ")], ChunkingConfig(50, 5, 5)) == []


def test_invalid_overlap_rejected() -> None:
    try:
        ChunkingConfig(10, 10, 1)
    except ValueError as exc:
        assert "overlap" in str(exc)
    else:
        raise AssertionError("Expected ValueError")

