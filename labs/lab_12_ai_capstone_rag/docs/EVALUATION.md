# Evaluation design

The evaluation set has 18 fictional IT-operations questions: 13 answerable, two ambiguous/cross-document, and three unanswerable. Expected sources and keywords live in `config/evaluation_questions.yml`.

## Variants

- `offline-baseline`: no retrieval and no model call; it always returns the insufficient-information response. Used only for the offline fixture comparison.
- `live-baseline`: the **same chat model** answers with no retrieved context (`NoRetrievalBaseline`). This is the fair comparison: it shows what retrieval adds, instead of comparing RAG with a fixed string that scores every unanswerable question as correct by construction.
- `offline-rag`: deterministic lexical retrieval plus an extractive fixture answerer. It validates corpus loading, chunking, ranking, context construction, citations, metrics, and error-free execution without presenting the fixture as semantic or generative quality.
- `live-rag`: AI Search (HYBRID query) plus `databricks-gpt-oss-20b`, run by `notebooks/04_evaluate.py` with `run_live_inference=true`. Scores go to `evaluation_results`; redacted query events (with generation diagnostics, no text) to `query_monitoring`.

## Metrics

Each metric applies only where it is meaningful; elsewhere it is `null` (per answer) or
`not_applicable` (in summaries), and every summary mean carries `<metric>_n`, the number of
questions it averages. Nothing that does not apply is reported as 0.

| Metric | Definition | Applies to |
|---|---|---|
| Retrieval recall | expected source documents present among the retrieved results | RAG, answerable/ambiguous |
| Citation precision | valid citations whose source is an expected (supporting) document | RAG, answerable/ambiguous, at least one valid citation |
| Source coverage | expected source documents the answer actually cites | RAG, answerable/ambiguous |
| Invalid citations | `[n]` pointing at no retrieved source | every answer |
| Uncited sentences (heuristic) | answer sentences or list items over 25 characters with no inline `[n]` | RAG answers with sources |
| Weak citations (heuristic) | cited sentences (4+ content words) sharing under 25 % of their content words with every excerpt they cite | RAG answers with sources |
| Answer completeness | expected keywords present | answerable/ambiguous |
| Answer correctness | completeness, but 0 when an answerable question is refused; for unanswerable questions, 1 for a refusal without specific claims | every answer |

Refusals are recognised by meaning, not by one exact sentence ("do not contain enough
information", "the runbooks don't mention...", "I can't help with that"). Each answer gets a
`refusal` status: `none`, `clean`, `with_extra_content` (counted as correct for an unanswerable
question but flagged for review) or `with_specific_claims` - a refusal that still states concrete
details such as numbers, codes, hostnames or addresses, scored 0 as possible fabrication.

**Neither heuristic proves anything.** Weak citations flag a citation for review (it catches
q13's rollback rule in the live run's answers). Uncited sentences are not hallucinations: that
heuristic only marks text without an inline citation (an introduction, a summary line). Whether a claim is grounded is decided by manual
review against the cited source, recorded in the evidence.

Generation diagnostics are kept for every live call: finish reason, prompt/completion/total
tokens (reasoning tokens when the endpoint reports them), retrieval and generation latency,
empty or truncated responses, and errors. Empty and truncated responses are errors, never refusals.

These deterministic metrics are regression signals, not a substitute for human review. Keyword metrics can reward wording without true reasoning, lexical retrieval is not semantic search, and the offline extractive answer may quote more context than a production answer should. A reviewer must assess safety, usefulness, and citation entailment on live outputs.

## Retrieval and context controls

- HYBRID top-k (`retrieval.top_k: 4`) from AI Search.
- Section-aware supplemental retrieval: a question that names a section topic ("roll back" ->
  Rollback) gets one extra search filtered to that section, at most
  `retrieval.max_supplemental_queries: 2` per question.
- Title-only chunks (document title plus the fictional-document note) are dropped.
- System scoping: a question naming one fictional system keeps only that system's runbook excerpts.
- Excerpt headers name the document and section; persisted diagnostics are allowlisted.

## Live results (Personal DEV, 2026-10-11)

Run `410537433766515`: RAG answer correctness 0.843 vs 0.181 for the no-retrieval baseline
(automated); manual review: RAG 14 correct, 3 partially correct, 1 false refusal (q09); no
invented facts. Details, per-question table and caveats:
[stage 4C/4D evidence](../evidence/2026-10-11_personal_dev_stage4cd_full_evaluation.md).

## Running offline

```powershell
lab12 evaluate
pytest
```

Evidence in `evidence/` may quote answers to the fictional evaluation questions for review. Never
commit real user questions, answers, credentials or provider error details.
