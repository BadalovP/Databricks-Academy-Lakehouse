from __future__ import annotations

from pathlib import Path

import pytest

from urbanflow.rag.ingestion import (
    DuplicateDocumentError,
    EmptyDocumentError,
    load_documents,
    normalize_text,
)


def test_normalize_text_collapses_space_and_blank_lines() -> None:
    assert normalize_text(" A  B\r\n\r\n\r\n C \t") == "A B\n\nC"


def test_load_markdown_extracts_sections(tmp_path: Path) -> None:
    source = tmp_path / "guide.md"
    source.write_text("# Guide\nIntro text.\n## Recovery\nRetry once.", encoding="utf-8")
    document = load_documents([source])[0]
    assert document.title == "Guide"
    assert [section.heading for section in document.sections] == ["Guide", "Recovery"]
    assert document.document_id.startswith("doc_")


def test_text_file_has_document_section(tmp_path: Path) -> None:
    source = tmp_path / "note.txt"
    source.write_text("Plain operational note.", encoding="utf-8")
    assert load_documents([source])[0].sections[0].heading == "Document"


def test_empty_document_rejected(tmp_path: Path) -> None:
    source = tmp_path / "empty.md"
    source.write_text(" \n\n", encoding="utf-8")
    with pytest.raises(EmptyDocumentError):
        load_documents([source])


def test_duplicate_content_rejected(tmp_path: Path) -> None:
    for name in ("a.txt", "b.txt"):
        (tmp_path / name).write_text("same content", encoding="utf-8")
    with pytest.raises(DuplicateDocumentError):
        load_documents([tmp_path])


def test_discovery_is_stable(project_root: Path) -> None:
    first = load_documents([project_root / "knowledge_base"])
    second = load_documents([project_root / "knowledge_base"])
    assert [item.document_id for item in first] == [item.document_id for item in second]
    assert len(first) == 8  # one original document per mobility-operations topic
