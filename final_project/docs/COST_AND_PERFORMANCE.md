# Cost and performance analysis

[← Main README](../README.md) · [Architecture](ARCHITECTURE.md) · [Release and validation](RELEASE_AND_VALIDATION.md)

This analysis is **design-time**. No Final Project run has happened yet, so there are no measured DBU, warehouse or token figures to report. Prices change and differ by cloud, region and contract, so this page shows **cost drivers and hard upper bounds in units** (runs, minutes, requests). It does not invent dollar amounts. Measured figures will be added from system tables after the first approved run (see [Measuring after a live run](#measuring-after-a-live-run)).

## What can cost money, and the bound on each

| Driver | When it is billed | Bound built into the project | Default state |
|---|---|---|---|
| Serverless Jobs (platform Job) | Only while a run executes | No schedule or trigger. `max_concurrent_runs: 1`. Job timeout 2 h. Every task has its own timeout (5–60 min). Streams use `availableNow` with bounded waits. | Every `run_*` switch is `false`: a default run does almost nothing |
| Serverless Lakeflow update | Only during a triggered update | `continuous: false`. Runs only with `run_lakeflow=true`. Task timeout 40 min. | Off |
| Serverless Jobs (assistant Job) | Only while a run executes | Job timeout 90 min; task timeouts 20/60/30 min | Writes, index mutation and inference are all `false` |
| **AI Search endpoint** | **For every hour it exists, used or not** | Created only with `allow_ai_search_mutation=true`. One Standard endpoint per target. Provisioning waits are capped at 25 + 25 min. | Not created |
| Embeddings | Once per index sync | 30 chunks, about 6.4k characters in total; TRIGGERED sync, never continuous | Not run |
| Chat model calls | Per request (tokens) | Printed call plan before any call. Full evaluation = 36 chat calls and up to 54 searches (18 questions × [baseline + RAG], 1 + 2 supplemental searches). `max_tokens` 1800, temperature 0. `eval_question_ids` runs a subset. | Not run (offline fixture only) |
| SQL warehouse | While running (dashboard, governance demo) | Uses an existing serverless warehouse with auto-stop. The project creates no warehouse. | Not used until the dashboard is opened |
| Zerobus (optional) | Per ingested data | At most 30 synthetic records per run. Dry run by default. | Not run |
| GitHub Actions | Runner minutes | CI on PRs touching `final_project/`; CD manual only | CI only |
| Storage | Data at rest | Small samples: 40 stations, 40 trips, 48 weather hours, 6 tickets, 30 chunks | Nothing written yet |

**The single biggest cost risk is an AI Search endpoint left running.** It bills by the hour whether or not anyone queries it. The Lab 12 lesson is applied: create it only for an approved evaluation window, and delete it afterwards (with approval). The Delta tables remain and are cheap to keep.

## AI Search endpoint: the one continuously billed resource

A STANDARD AI Search endpoint is billed **for every hour it exists**, whether or not anyone queries it. The billing does not depend on the run. The exposure is therefore *hours × the endpoint's hourly DBU rate × the workspace's DBU price*. The rate and price are not hard-coded here; read them from `system.billing.list_prices` (SKU names containing `VECTOR_SEARCH`) when a warehouse is next running.

| Window | Exposure |
|---|---|
| Planned evaluation window (steps 6–8) | About 1–2 endpoint-hours. Provisioning plus index sync took about 25–30 minutes in Lab 12; the evaluation takes minutes. |
| Endpoint forgotten for a day | 24 endpoint-hours: roughly 12–24 times the planned exposure |
| Endpoint forgotten for a week | 168 endpoint-hours |

Control: create it only in step 6, delete it in step 8 (approval required), and verify with `vector_search_endpoints.list_endpoints()` that none remains. The Delta tables stay and cost only storage.

## First DEV deployment

`bundle deploy` alone starts no compute. It uploads files and creates 2 schemas, 1 Volume, 2 Jobs, 1 pipeline definition and 1 dashboard definition. It runs no Job or pipeline update, and it does not start the warehouse. Opening the dashboard would start the Starter warehouse (2X-Small, auto-stop 10 minutes); with empty tables there is nothing to see yet, so leave it closed until after step 2.

## Cost of one approved validation, in units

| Step | Work | Expected order of magnitude |
|---|---|---|
| CD release with `run_platform=true` | Deploy, plus one platform run with station, historical, weather and CDC on the samples | One serverless Job run of minutes, not hours; mostly startup time, since the data is tiny |
| Optional `run_lakeflow=true` | One triggered update over the sample Bronze | One short serverless pipeline update |
| Assistant Job, ingest + index | MERGE 8 docs / 30 chunks; create endpoint and index; one sync | A few minutes of serverless compute, plus endpoint hours until it is deleted |
| Assistant Job, evaluation | 36 chat calls, up to 54 searches | Small token volume: short prompts over at most a 6,000-character context |

## Performance design

| Decision | Why |
|---|---|
| `availableNow` batch streams instead of continuous | Process the backlog, then stop. No idle compute, and the checkpoint keeps it incremental. |
| CDC reads only `checkpoint + 1 … latest` | Work grows with the number of changes, not the table size. No full table comparison on each run except the reconciliation. |
| MERGE skips unchanged rows (`<=>` comparison) | No rewrite of unchanged files, and no empty CDF events downstream |
| Execution-scoped replace and `execution_id` in every Gold grain | A rerun touches one execution's rows only |
| Broadcast join to the small station reference | The 40-station dimension never causes a shuffle (`F.broadcast` in `silver.py`) |
| No `OPTIMIZE`, Z-order or liquid clustering applied | The tables are kilobytes. Compaction would cost more than it saves. The maintenance SQL documents when it would pay off and stays commented. |
| Small-file lesson kept local | `scripts/generate_many_small_files.py` shows the problem with about 0.5 MB of labelled synthetic files, not gigabytes |
| Hybrid retrieval, top-k 4, 6,000-character context | Keeps prompts short (cost and latency) while recall stays high on the fixture |
| Serverless with `disable_auto_optimization: true` | Predictable, bounded task behaviour for an educational workload; failures surface instead of being retried silently |

## Measured offline performance

| Measurement | Value | Source |
|---|---|---|
| Test suite | 700+ tests (local Spark included) in about 2 minutes on a laptop | `pytest` |
| Corpus | 8 documents, 30 chunks, 6,365 characters | `rag.pipeline.load_corpus` |
| Offline RAG fixture | Recall 0.938, source coverage 0.875, correctness 0.648 (baseline 0.111) | `rag.pipeline.offline_summary` (fixture retriever and extractive model; **not** model quality) |

## Measuring after a live run

After an approved run, record these figures in the release evidence. Do not estimate them:

```sql
-- DBUs by Final Project Job and pipeline (system tables; read-only).
SELECT usage_metadata.job_id, usage_metadata.dlt_pipeline_id, sku_name,
       SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_date >= date_sub(current_date(), 7)
  AND custom_tags['project'] = 'urbanflow_final_project'
GROUP BY ALL;
```

Also record task durations from the Jobs API (`validate_job_run.py` keeps the run JSON), the assistant's `latency_ms` and the `generation` summary, and how long the AI Search endpoint existed.
