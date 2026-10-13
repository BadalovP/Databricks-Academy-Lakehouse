"""Lightweight documentation checks for the repository homepage and the Final Project docs.

Checked, with no network access and no extra dependency:

- every relative Markdown link resolves to an existing file or folder;
- every in-page or cross-file ``#anchor`` matches a heading (GitHub slug rules);
- every ```mermaid block starts with a diagram type GitHub renders, and its brackets balance;
- no image is loaded from a remote host (only repository files or shields.io badges).

The Mermaid syntax itself is parsed separately with the official parser when Node is available
(see docs/CI_CD.md); this script keeps CI dependency-free.

Usage: ``python scripts/check_docs.py`` from ``final_project/``; exit 1 on any finding.
"""

from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parent
DIAGRAM_TYPES = (
    "flowchart", "graph", "sequenceDiagram", "erDiagram", "stateDiagram-v2", "stateDiagram",
    "classDiagram", "timeline", "journey", "gantt", "pie", "mindmap",
)  # fmt: skip
ALLOWED_IMAGE_HOSTS = ("https://img.shields.io/", "https://github.com/")
LINK = re.compile(r"(!?)\[(?:[^\]]|\][^(])*?\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
FENCE = re.compile(r"^```")
# erDiagram cardinality markers such as ||--o{ or }|..|| carry unpaired braces by design.
ER_CARDINALITY = re.compile(r"[|}][o|](?:--|\.\.)[o|][|{]")


def documents() -> list[Path]:
    files = [REPOSITORY_ROOT / "README.md", PROJECT_ROOT / "README.md"]
    files += sorted((PROJECT_ROOT / "docs").glob("*.md"))
    files += sorted((PROJECT_ROOT / "evidence").glob("*.md"))
    return [path for path in files if path.exists()]


def slug(heading: str) -> str:
    """GitHub's heading anchor: lowercase, drop punctuation, spaces become hyphens."""
    text = re.sub(r"`|\*\*|__|\[|\]\([^)]*\)", "", heading).strip().lower()
    out = []
    for char in text:
        category = unicodedata.category(char)
        if char in " -":
            out.append("-" if char == " " else char)
        elif char == "_" or char.isalnum() or category.startswith(("L", "N")):
            out.append(char)
        # everything else (punctuation, symbols, emoji) is dropped
    return "".join(out)


def anchors(path: Path) -> set[str]:
    seen: dict[str, int] = {}
    result = set()
    in_code = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if FENCE.match(line.strip()):
            in_code = not in_code
            continue
        match = re.match(r"^(#{1,6})\s+(.*?)\s*#*\s*$", line)
        if in_code or not match:
            continue
        base = slug(match.group(2))
        count = seen.get(base, 0)
        seen[base] = count + 1
        result.add(base if count == 0 else f"{base}-{count}")
    for explicit in re.findall(r'<a\s+(?:id|name)="([^"]+)"', path.read_text(encoding="utf-8")):
        result.add(explicit)
    return result


def strip_code(text: str) -> str:
    """Remove fenced and inline code so example links are not checked."""
    text = re.sub(r"```.*?```", "", text, flags=re.S)
    return re.sub(r"`[^`\n]*`", "", text)


def check_links(path: Path) -> list[str]:
    problems = []
    for image, target in LINK.findall(strip_code(path.read_text(encoding="utf-8"))):
        if target.startswith(("http://", "https://")):
            if image and not target.startswith(ALLOWED_IMAGE_HOSTS):
                problems.append(f"{path}: remote image {target}")
            continue
        if target.startswith("mailto:"):
            continue
        file_part, _, anchor = target.partition("#")
        destination = (path.parent / file_part).resolve() if file_part else path
        if file_part and not destination.exists():
            problems.append(f"{path}: broken link {target}")
            continue
        if anchor and destination.suffix == ".md" and anchor not in anchors(destination):
            problems.append(f"{path}: missing anchor #{anchor} in {destination.name}")
    return problems


def check_mermaid(path: Path) -> list[str]:
    problems = []
    blocks = re.findall(r"```mermaid\r?\n(.*?)```", path.read_text(encoding="utf-8"), flags=re.S)
    for index, block in enumerate(blocks, start=1):
        first = next((line.strip() for line in block.splitlines() if line.strip()), "")
        if not first.startswith(DIAGRAM_TYPES):
            problems.append(f"{path}: mermaid block {index} starts with {first[:30]!r}")
        body = ER_CARDINALITY.sub("", block)
        for opening, closing in ("[]", "()", "{}"):
            if body.count(opening) != body.count(closing):
                problems.append(f"{path}: mermaid block {index} has unbalanced {opening}{closing}")
    return problems


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="backslashreplace")
    files = documents()
    problems = []
    for path in files:
        problems += check_links(path) + check_mermaid(path)
    for problem in problems:
        print(problem)
    print(f"Checked {len(files)} documents; {len(problems)} problem(s).")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
