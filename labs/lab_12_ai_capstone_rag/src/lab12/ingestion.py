"""Deterministic ingestion for fictional Markdown, text, and optional PDF documents."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from pathlib import Path

from .models import Document, Section

_SUPPORTED = {".md", ".markdown", ".txt", ".pdf"}
_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$")


class EmptyDocumentError(ValueError):
    """Raised when a source has no usable text."""


class DuplicateDocumentError(ValueError):
    """Raised when two inputs normalize to the same content."""


def normalize_text(text: str) -> str:
    """Normalize line endings and whitespace while retaining paragraph boundaries."""
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "")
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]
    normalized: list[str] = []
    previous_blank = False
    for line in lines:
        is_blank = not line
        if is_blank and previous_blank:
            continue
        normalized.append(line)
        previous_blank = is_blank
    return "\n".join(normalized).strip()


def discover_files(inputs: Iterable[str | Path]) -> list[Path]:
    """Expand files and directories into a stable, supported source list."""
    discovered: set[Path] = set()
    for item in inputs:
        path = Path(item)
        if path.is_dir():
            discovered.update(
                child for child in path.rglob("*") if child.is_file() and child.suffix.lower() in _SUPPORTED
            )
        elif path.is_file() and path.suffix.lower() in _SUPPORTED:
            discovered.add(path)
        elif path.exists():
            raise ValueError(f"Unsupported document type: {path}")
        else:
            raise FileNotFoundError(path)
    return sorted(discovered, key=lambda value: value.as_posix().lower())


def load_documents(inputs: Iterable[str | Path]) -> list[Document]:
    """Load sources, reject empty and duplicate content, and assign stable IDs."""
    documents: list[Document] = []
    hashes: dict[str, Path] = {}
    for path in discover_files(inputs):
        raw = _read_source(path)
        content = normalize_text(raw)
        if not content:
            raise EmptyDocumentError(f"Document is empty after normalization: {path}")
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        if content_hash in hashes:
            raise DuplicateDocumentError(f"Duplicate content: {path} and {hashes[content_hash]}")
        hashes[content_hash] = path
        sections = tuple(_extract_sections(content, path.suffix.lower()))
        title = _title_for(path, sections)
        documents.append(
            Document(
                document_id=f"doc_{content_hash[:20]}",
                title=title,
                source_name=path.name,
                source_path=path.as_posix(),
                content=content,
                content_hash=content_hash,
                sections=sections,
                metadata={"extension": path.suffix.lower(), "classification": "fictional"},
            )
        )
    return documents


def _read_source(path: Path) -> str:
    if path.suffix.lower() != ".pdf":
        return path.read_text(encoding="utf-8")
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError("PDF support is optional; install the 'databricks' extra.") from exc
    reader = PdfReader(str(path))
    return "\n\n".join(page.extract_text() or "" for page in reader.pages)


def _extract_sections(content: str, suffix: str) -> list[Section]:
    if suffix not in {".md", ".markdown"}:
        return [Section(heading="Document", text=content)]
    sections: list[Section] = []
    heading = "Introduction"
    body: list[str] = []
    for line in content.splitlines():
        match = _HEADING.match(line)
        if match:
            text = normalize_text("\n".join(body))
            if text:
                sections.append(Section(heading=heading, text=text))
            heading = match.group(2).strip()
            body = []
        else:
            body.append(line)
    text = normalize_text("\n".join(body))
    if text:
        sections.append(Section(heading=heading, text=text))
    return sections or [Section(heading="Document", text=content)]


def section_headings(documents: list[Document]) -> list[str]:
    """Section headings across the corpus, excluding document titles and placeholder headings."""
    titles = {document.title for document in documents}
    return sorted(
        {section.heading for document in documents for section in document.sections}
        - titles
        - {"Introduction", "Document"}
    )


def document_titles(documents: list[Document]) -> dict[str, str]:
    """source_name -> title, used to label retrieved excerpts with the document they come from."""
    return {document.source_name: document.title for document in documents}


def _title_for(path: Path, sections: tuple[Section, ...]) -> str:
    if sections and sections[0].heading not in {"Introduction", "Document"}:
        return sections[0].heading
    return path.stem.replace("_", " ").replace("-", " ").title()

