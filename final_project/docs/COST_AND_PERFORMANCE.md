# Cost and performance analysis

[← Main README](../README.md) · [Architecture](ARCHITECTURE.md) · [Release and validation](RELEASE_AND_VALIDATION.md)

This analysis is mostly **design-time**. Steps 1–5 ran in Personal DEV, so wall-clock durations are measured (below). DBUs for the step 2 and 3 runs were read from `system.billing.usage` (below). Later runs were not yet in the billing tables when this was written. Prices change and differ by cloud, region and contract, so this page shows **cost drivers and hard upper bounds in units** (runs, minutes, requests). It does not invent dollar amounts. Measured figures will be added from system tables after the first approved run (see [Measuring after a live run](#measuring-after-a-live-run)).

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

A STANDARD AI Search endpoint is billed **for every hour it exists**, whether or not anyone queries it. The billing does not depend on the run. The exposure is therefore *hours × the endpoint's hourly DBU rate × the workspace's DBU price*. In this workspace, AI Search bills under `PREMIUM_SERVERLESS_REAL_TIME_INFERENCE` (billing origin `VECTOR_SEARCH`). Lab 12's endpoint measured about 3.3 DBU per hour (above). At the $0.07 list price, that is about $0.23 per endpoint-hour.

| Window | Exposure |
|---|---|
| Planned evaluation window (steps 6–8) | About 1–2 endpoint-hours, ≈ 3.3–6.7 DBU (≈ $0.23–0.47 at list). Provisioning plus index sync took about 25–30 minutes in Lab 12; the evaluation takes minutes. |
| Endpoint forgotten for a day | 24 endpoint-hours, ≈ 80 DBU (≈ $5.6 at list): roughly 12–24 times the planned exposure |
| Endpoint forgotten for a week | 168 endpoint-hours, ≈ 560 DBU (≈ $39 at list) |

Control: create it only in step 6, delete it in step 8 (approval required), and verify with `vector_search_endpoints.list_endpoints()` that none remains. The Delta tables stay and cost only storage.

## First DEV deployment

`bundle deploy` alone starts no compute. It uploads files and creates 2 schemas, 1 Volume, 2 Jobs, 1 pipeline definition and 1 dashboard definition. It runs no Job or pipeline update, and it does not start the warehouse. Opening the dashboard would start the Starter warehouse (2X-Small, auto-stop 10 minutes); with empty tables there is nothing to see yet, so leave it closed until after step 2.

## Cost of one approved validation, in units

| Step | Work | Expected order of magnitude |
|---|---|---|
| CD release with `mode=deploy_and_sample_run` | Deploy, plus one platform run with station, historical, weather and CDC on the samples | One serverless Job run of minutes, not hours; mostly startup time, since the data is tiny |
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

## Measured live durations (Personal DEV, serverless, sample data)

Wall-clock times from the Jobs API, as recorded in `evidence/`. They are dominated by serverless task startup, not data volume (40 events, 40 trips, 48 hours, 5 tickets). DBUs were not read from billing.

| Run | Job run | Total (s) | preflight | land_sources | station_source_check | station_silver | station_gold | historical_trips | weather_enrichment | validate_medallion | federation_cdc |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Step 2 pass | `896549012431425` | 485 | 44 | 41 | 19 | 60 | 96 | 76 | 49 | 40 | 54 |
| Step 3 (18/19) | `605056708110586` | 480 | 49 | 43 | 18 | 62 | 98 | 73 | 49 | 39 | 45 |
| Step 3 retry (19/19) | `412521206243476` | 458 | 49 | 39 | 18 | 63 | 99 | 75 | 46 | 37 | 29 |
| Step 4 `batch_002` (CDC only) | `343513312512955` | 294 | 50 | 47 | 16 | 19 | 17 | 16 | 18 | 18 | 88 |
| Step 4 `batch_003` (CDC only) | `500336374896739` | 220 | 49 | 17 | 16 | 15 | 15 | 15 | 15 | 16 | 58 |
| Step 5 Lakeflow only | `441981394008476` | 286 | 26 | 16 | 17 | 17 | 16 | 17 | 14 | 17 | 17 |

In the CDC-only and Lakeflow-only runs, the disabled tasks only start, check their switch and exit (`DRY_RUN` or `SKIPPED`). Their 15–20 seconds each are serverless start-up time.

**Lakeflow update** `a26b4385-f0b8-454a-9aad-d9a3424151c7` (step 5): 129 s from request to `COMPLETED`, of which 34 s waiting for resources, 42 s initializing, 13 s setting up tables and 40 s computing the ten flows. The Job task `refresh_lakeflow` took 142 s.

## Measured DBUs (from `system.billing.usage`, read-only)

The billing tables reported usage up to 2026-10-12 20:00 UTC when they were read, so they cover the step 2 and 3 runs only. List prices come from `system.billing.list_prices` for this workspace (Premium, AWS `us-east-2`). They are **list prices, not the invoiced amount**: contract discounts or credits are not visible here.

| Run | Purpose | SKU | DBU | At list price (USD) |
|---|---|---|---|---|
| `304828008837365` | Step 2, first attempt (failed after 141 s) | `PREMIUM_JOBS_SERVERLESS_COMPUTE` ($0.35/DBU) | 0.3016 | ≈ 0.11 |
| `896235701785511` | Step 2, retry (failed at weather) | same | 0.8249 | ≈ 0.29 |
| `896549012431425` | Step 2, pass | same | 1.2672 | ≈ 0.44 |
| `605056708110586` | Step 3 (18/19) | same | 1.3531 | ≈ 0.47 |
| `412521206243476` | Step 3 retry (19/19) | same | 1.1901 | ≈ 0.42 |
| Warehouse `410e5f3b4784e981` | All SQL on the Starter warehouse on 2026-10-12, mostly the read-only verification queries. It is shared with other work by the same user and **not Final Project-only**. | `PREMIUM_SERVERLESS_SQL_COMPUTE` ($0.70/DBU) | 10.1142 | ≈ 7.08 |

A full sample platform run costs about 1.2–1.4 DBU (≈ $0.45 at list). The verification queries cost more than the pipeline runs, because each warehouse start bills at least a few minutes. Batching verification into fewer warehouse sessions is the cheapest saving.

**AI Search reference (measured in this workspace, Lab 12, not the Final Project):** endpoint `lab12-rag-dev` billed 23.374 DBU over 7 billed hours under `PREMIUM_SERVERLESS_REAL_TIME_INFERENCE` ($0.07/DBU). That is about 3.3 DBU per endpoint-hour, ≈ $0.23 per hour at list. Its index sync used 0.787 DBU of serverless jobs compute.

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
