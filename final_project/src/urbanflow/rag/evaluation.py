"""Deterministic evaluation harness for baseline and retrieval-augmented answers."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .models import RagAnswer
from .rag import INSUFFICIENT


@dataclass(frozen=True)
class EvaluationCase:
    question_id: str
    question: str
    category: str
    expected_sources: tuple[str, ...]
    expected_keywords: tuple[str, ...]


@dataclass(frozen=True)
class EvaluationScore:
    """Automated, lexical scores for one answer. ``None`` means the metric does not apply.

    - ``retrieval_recall``: expected source documents among the retrieved results.
    - ``citation_precision``: valid citations that point at an expected source document.
    - ``source_coverage``: expected source documents that the answer actually cites.
    - ``uncited_sentences``: a heuristic count of answer sentences without an inline [n]. It
      flags text for review; it is not a hallucination detector.
    - ``weak_citations``: a heuristic count of cited sentences whose content words barely appear
      in any excerpt they cite (q13 cited a deployment-controls excerpt for a rollback rule). It
      flags citations for review; it does not prove a claim unsupported.
    - ``answer_correctness`` / ``answer_completeness``: expected-keyword coverage (correctness is
      0 when an answerable question is refused); for unanswerable questions, a refusal without
      specific factual claims.
    """

    question_id: str
    variant: str
    category: str
    answer_correctness: float
    answer_completeness: float
    error_status: str | None
    retrieval_recall: float | None = None
    citation_precision: float | None = None
    source_coverage: float | None = None
    uncited_sentences: int | None = None
    # Citation numbers that point at no retrieved source (e.g. [5] with four sources).
    invalid_citations: int = 0
    # Heuristic: cited sentences sharing almost no content words with the excerpt they cite.
    weak_citations: int | None = None
    # "none", "clean", "with_extra_content" or "with_specific_claims".
    refusal: str = "none"
    review_flags: tuple[str, ...] = ()
    # Generation metadata copied from the answer: timings, tokens, finish reason. No text.
    diagnostics: dict[str, Any] = field(default_factory=dict)


def load_cases(path: str | Path) -> list[EvaluationCase]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    cases = payload.get("questions", []) if isinstance(payload, dict) else []
    if len(cases) < 15:
        raise ValueError("Evaluation suite must contain at least 15 questions.")
    return [
        EvaluationCase(
            question_id=str(item["id"]),
            question=str(item["question"]),
            category=str(item["category"]),
            expected_sources=tuple(item.get("expected_sources", [])),
            expected_keywords=tuple(
                str(value).lower() for value in item.get("expected_keywords", [])
            ),
        )
        for item in cases
    ]


def baseline_answer(case: EvaluationCase) -> RagAnswer:
    return RagAnswer(
        question=case.question,
        answer=INSUFFICIENT,
        sources=(),
        model="no-retrieval-baseline",
        latency_ms=0,
    )


# Insufficient-information and safety refusals, matched on normalized text ("don't" -> "do not").
_REFUSAL_PATTERNS = tuple(
    re.compile(pattern)
    for pattern in (
        r"\b(?:do|does|did) not (?:contain|include|provide|have|mention|specify|cover)\b"
        r"[^.]{0,60}\b(?:information|details|data)\b",
        r"\b(?:documents?|context|excerpts?|sources?|runbooks?)\b[^.]{0,40}\b(?:do|does|did) not "
        r"(?:contain|include|provide|mention|specify|list|cover|say|state)\b",
        r"\b(?:not enough|insufficient|no relevant) (?:information|details|context)\b",
        r"\bno (?:information|details|mention) (?:about|on|of|regarding)\b",
        r"\bnot (?:covered|mentioned|specified|documented|available|included) in the\b"
        r"[^.]{0,30}\b(?:documents|context|excerpts|sources|runbooks)\b",
        r"\b(?:can not|unable to|could not) (?:answer|determine|find|locate)\b"
        r"[^.]{0,80}\b(?:documents|context|excerpts|sources|runbooks)\b",
        r"\b(?:can not|will not|unable to) (?:help|assist|share|provide|disclose|give)\b",
    )
)
# Concrete details that a refusal to an unanswerable question must not add (phone numbers,
# passwords, hostnames, code, addresses): their presence means possible fabrication.
_SPECIFIC_CLAIM = re.compile(r"\d{3,}|`[^`]+`|\b[\w.+-]+@[\w-]+\.\w+|https?://|\b\w+(?:-\w+){2,}\b")


def _normalize(text: str) -> str:
    text = text.lower().replace("’", "'").replace("‘", "'")
    text = re.sub(r"\bcan't\b|\bcannot\b", "can not", text)
    text = re.sub(r"\bwon't\b", "will not", text)
    text = re.sub(r"n't\b", " not", text)
    return re.sub(r"\s+", " ", text)


def _sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+|\n+", text)
    return [part.strip(" -*\t") for part in parts if part.strip(" -*\t")]


def refusal_status(text: str) -> str:
    """Classify an answer as a refusal: "none", "clean", "with_extra_content" or
    "with_specific_claims" (a refusal that still asserts concrete details)."""
    refusing, other = [], []
    for sentence in _sentences(text):
        normalized = _normalize(sentence)
        (refusing if any(p.search(normalized) for p in _REFUSAL_PATTERNS) else other).append(
            sentence
        )
    if not refusing:
        return "none"
    # Specifics anywhere count, including inside the refusal sentence ("..., but try 555-0100").
    if _SPECIFIC_CLAIM.search(re.sub(r"\[\d+(?:\s*,\s*\d+)*]", "", text)):
        return "with_specific_claims"
    extra = re.sub(r"\[\d+(?:\s*,\s*\d+)*]", "", " ".join(other)).strip()
    return "with_extra_content" if extra else "clean"


def score_answer(case: EvaluationCase, answer: RagAnswer, variant: str) -> EvaluationScore:
    uses_retrieval = "baseline" not in variant
    answerable = bool(case.expected_sources)
    expected_sources = set(case.expected_sources)
    flags: list[str] = []

    refusal = "none" if answer.error else refusal_status(answer.answer)
    if answer.error:
        flags.append(f"error:{answer.error}")

    lowered = _normalize(answer.answer)
    matched = sum(_normalize(keyword) in lowered for keyword in case.expected_keywords)
    if case.category == "unanswerable":
        correctness = 1.0 if refusal in ("clean", "with_extra_content") else 0.0
        completeness = correctness
        if refusal == "with_extra_content":
            flags.append("refusal_with_extra_content:review")
        if refusal == "with_specific_claims":
            flags.append("refusal_with_specific_claims:possible_fabrication")
    else:
        completeness = matched / len(case.expected_keywords) if case.expected_keywords else 1.0
        correctness = 0.0 if refusal == "clean" else completeness
        if refusal != "none":
            flags.append(f"answerable_question_refused:{refusal}")

    citations = cited_numbers(answer.answer)
    valid = {number for number in citations if 1 <= number <= len(answer.sources)}
    cited_sources = {answer.sources[number - 1].source_name for number in valid}

    retrieval_recall = citation_precision = source_coverage = None
    uncited: int | None = None
    if uses_retrieval and answerable:
        retrieved = {source.source_name for source in answer.sources}
        retrieval_recall = len(expected_sources & retrieved) / len(expected_sources)
        source_coverage = len(expected_sources & cited_sources) / len(expected_sources)
        if valid:
            supporting = sum(
                answer.sources[number - 1].source_name in expected_sources for number in valid
            )
            citation_precision = supporting / len(valid)
    weak: int | None = None
    if uses_retrieval and answer.sources and not answer.error:
        uncited = 0 if refusal == "clean" else _uncited_sentence_count(answer.answer)
        weak = len(weak_citation_sentences(answer))
        if weak:
            flags.append(f"weak_citations:{weak}:review")

    return EvaluationScore(
        question_id=case.question_id,
        variant=variant,
        category=case.category,
        answer_correctness=round(correctness, 3),
        answer_completeness=round(completeness, 3),
        error_status=answer.error,
        retrieval_recall=_round(retrieval_recall),
        citation_precision=_round(citation_precision),
        source_coverage=_round(source_coverage),
        uncited_sentences=uncited,
        invalid_citations=len(citations - valid),
        weak_citations=weak,
        refusal=refusal,
        review_flags=tuple(flags),
        diagnostics=dict(answer.diagnostics),
    )


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 3)


def cited_numbers(text: str) -> set[int]:
    """Every [n] citation in an answer, including grouped forms such as [1, 3] or [1][2]."""
    numbers: set[int] = set()
    for group in re.findall(r"\[(\d+(?:\s*,\s*\d+)*)]", text):
        numbers.update(int(part) for part in group.split(","))
    return numbers


def citation_map(answer: RagAnswer) -> dict[int, str]:
    """Citation number -> section_ref of the retrieved source it points at (or 'INVALID')."""
    return {
        number: (
            answer.sources[number - 1].section_ref
            if 1 <= number <= len(answer.sources)
            else "INVALID"
        )
        for number in sorted(cited_numbers(answer.answer))
    }


def summarize_generation(answers: list[RagAnswer]) -> dict[str, Any]:
    """Totals across calls; counts only reported values and says how many were unavailable."""
    summary: dict[str, Any] = {"calls": len(answers)}
    for name in ("prompt_tokens", "completion_tokens", "reasoning_tokens", "total_tokens"):
        values = [a.diagnostics.get(name) for a in answers]
        reported = [v for v in values if isinstance(v, int)]
        summary[name] = sum(reported) if reported else "unavailable"
        summary[f"{name}_unavailable"] = len(values) - len(reported)
    reasons: dict[str, int] = {}
    for answer in answers:
        reason = str(answer.diagnostics.get("finish_reason", "unavailable"))
        reasons[reason] = reasons.get(reason, 0) + 1
    summary["finish_reasons"] = reasons
    summary["truncated"] = sum(bool(a.diagnostics.get("truncated")) for a in answers)
    summary["empty_responses"] = sum(bool(a.diagnostics.get("empty_response")) for a in answers)
    summary["errors"] = sorted({a.error for a in answers if a.error})
    latencies = sorted(a.latency_ms for a in answers)
    if latencies:
        summary["latency_ms_max"] = latencies[-1]
        summary["latency_ms_median"] = latencies[len(latencies) // 2]
    return summary


def evaluate(
    cases: list[EvaluationCase], answerer: Callable[[str], RagAnswer], variant: str
) -> list[EvaluationScore]:
    return [score_answer(case, answerer(case.question), variant) for case in cases]


def select_cases(
    cases: list[EvaluationCase], question_ids: str = "", limit: int = 18
) -> list[EvaluationCase]:
    """The cases one run evaluates.

    Without ``question_ids`` this is the first ``limit`` cases, as before. With a comma-separated
    list it is exactly those cases, in suite order. An unknown, repeated or empty ID is an error,
    never skipped, and so is a ``limit`` smaller than the list: the run must not quietly evaluate
    something other than what was asked for.
    """
    if not question_ids.strip():
        return cases[: max(limit, 1)]
    requested = [part.strip() for part in question_ids.split(",")]
    if any(not part for part in requested):
        raise ValueError(f"Empty question ID in {question_ids!r}.")
    repeated = sorted({part for part in requested if requested.count(part) > 1})
    if repeated:
        raise ValueError(f"Repeated question IDs: {repeated}.")
    known = {case.question_id for case in cases}
    unknown = [part for part in requested if part not in known]
    if unknown:
        raise ValueError(f"Unknown question IDs: {unknown}.")
    if limit < len(requested):
        raise ValueError(
            f"eval_question_limit={limit} is smaller than the {len(requested)} requested IDs."
        )
    return [case for case in cases if case.question_id in set(requested)]


def live_call_plan(
    question_count: int, include_baseline: bool, supplemental_per_question: int = 0
) -> dict[str, int]:
    """Upper bound of live requests for one run; printed before any request is made."""
    return {
        "questions": question_count,
        "ai_search_queries": question_count * (1 + max(0, supplemental_per_question)),
        "rag_chat_calls": question_count,
        "baseline_chat_calls": question_count if include_baseline else 0,
        "chat_calls": question_count * (2 if include_baseline else 1),
    }


@dataclass(frozen=True)
class EvaluationRun:
    """Answers and scores of one run; the baseline lists are empty when it was not run."""

    baseline_answers: list[RagAnswer]
    baseline_scores: list[EvaluationScore]
    rag_answers: list[RagAnswer]
    rag_scores: list[EvaluationScore]


def run_evaluation(
    cases: list[EvaluationCase],
    rag: Callable[[str], RagAnswer],
    baseline: Callable[[str], RagAnswer] | None,
    prefix: str,
) -> EvaluationRun:
    """Answer every case with RAG and, only when ``baseline`` is given, without retrieval."""
    baseline_answers = [baseline(case.question) for case in cases] if baseline else []
    rag_answers = [rag(case.question) for case in cases]
    return EvaluationRun(
        baseline_answers=baseline_answers,
        baseline_scores=[
            score_answer(case, answer, f"{prefix}-baseline")
            for case, answer in zip(cases, baseline_answers, strict=False)
        ],
        rag_answers=rag_answers,
        rag_scores=[
            score_answer(case, answer, f"{prefix}-rag")
            for case, answer in zip(cases, rag_answers, strict=True)
        ],
    )


def score_to_row(score: EvaluationScore, evaluation_id: str, evaluated_at: str) -> dict[str, Any]:
    """Row for the evaluation_results Delta table (metrics kept together as JSON)."""
    import json
    from dataclasses import asdict

    from .monitoring import safe_diagnostics

    metrics = asdict(score)
    for key in ("question_id", "variant", "error_status"):
        metrics.pop(key)
    # Same allowlist as query_monitoring: no query-derived strings in persisted rows.
    metrics["diagnostics"] = safe_diagnostics(score.diagnostics)
    return {
        "evaluation_id": evaluation_id,
        "evaluated_at": evaluated_at,
        "variant": score.variant,
        "question_id": score.question_id,
        "metrics_json": json.dumps(metrics, sort_keys=True),
        "error_status": score.error_status,
    }


_AVERAGED = (
    "answer_correctness",
    "answer_completeness",
    "retrieval_recall",
    "citation_precision",
    "source_coverage",
)


def summarize(scores: list[EvaluationScore]) -> dict[str, Any]:
    """Means over the questions where a metric applies; ``<metric>_n`` says how many those were.

    A metric that applies to none of the scores is reported as "not_applicable", never as 0.
    """
    if not scores:
        return {"questions": 0}
    summary: dict[str, Any] = {"questions": len(scores)}
    for metric in _AVERAGED:
        values = [getattr(score, metric) for score in scores if getattr(score, metric) is not None]
        summary[metric] = round(sum(values) / len(values), 3) if values else "not_applicable"
        summary[f"{metric}_n"] = len(values)
    uncited = [score.uncited_sentences for score in scores if score.uncited_sentences is not None]
    summary["uncited_sentences_heuristic"] = sum(uncited) if uncited else "not_applicable"
    summary["invalid_citations"] = sum(score.invalid_citations for score in scores)
    weak = [score.weak_citations for score in scores if score.weak_citations is not None]
    summary["weak_citations_heuristic"] = sum(weak) if weak else "not_applicable"
    refusals: dict[str, int] = {}
    for score in scores:
        refusals[score.refusal] = refusals.get(score.refusal, 0) + 1
    summary["refusals"] = refusals
    summary["review_flags"] = sum(len(score.review_flags) for score in scores)
    summary["errors"] = sum(score.error_status is not None for score in scores)
    return summary


def summarize_by_category(scores: list[EvaluationScore]) -> dict[str, dict[str, Any]]:
    categories = sorted({score.category for score in scores})
    return {
        category: summarize([score for score in scores if score.category == category])
        for category in categories
    }


_STOP = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "before",
        "by",
        "can",
        "do",
        "does",
        "for",
        "from",
        "has",
        "have",
        "if",
        "in",
        "is",
        "it",
        "its",
        "may",
        "must",
        "not",
        "of",
        "on",
        "or",
        "should",
        "so",
        "that",
        "the",
        "their",
        "then",
        "there",
        "these",
        "this",
        "to",
        "was",
        "were",
        "what",
        "when",
        "which",
        "while",
        "who",
        "will",
        "with",
        "within",
        "you",
        "your",
    }
)


def _content_words(text: str) -> set[str]:
    """Lower-cased words of two or more characters, citations and stopwords removed."""
    text = _normalize(re.sub(r"\[\d+(?:\s*,\s*\d+)*]", " ", text))
    return {word for word in re.findall(r"[a-z0-9][a-z0-9-]+", text) if word not in _STOP}


def weak_citation_sentences(answer: RagAnswer, threshold: float = 0.25) -> list[str]:
    """Cited sentences whose content words mostly do not occur in any excerpt they cite.

    For each sentence with valid citations, the best overlap across its cited excerpts is the
    share of the sentence's content words found in that excerpt. Below ``threshold`` (and with at
    least four content words, so short phrases are not judged) the sentence is returned for review.
    """
    weak: list[str] = []
    for sentence in _sentences(answer.answer):
        numbers = {n for n in cited_numbers(sentence) if 1 <= n <= len(answer.sources)}
        words = _content_words(sentence)
        if not numbers or len(words) < 4:
            continue
        best = max(
            len(words & _content_words(answer.sources[n - 1].content)) / len(words) for n in numbers
        )
        if best < threshold:
            weak.append(sentence)
    return weak


def _uncited_sentence_count(answer: str) -> int:
    """Heuristic: answer sentences or list items that carry no inline [n] citation."""
    return sum("[" not in sentence for sentence in _sentences(answer) if len(sentence) > 25)
