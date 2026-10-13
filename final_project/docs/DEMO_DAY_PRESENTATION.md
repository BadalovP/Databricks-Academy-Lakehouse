# Demo Day presentation: UrbanFlow 2.0 (25 minutes)

[← Main README](../README.md) · [Coverage matrix](LABS_1_TO_12_COVERAGE.md) · [Architecture](ARCHITECTURE.md)

A 25-minute talk: 20 minutes of presentation and demo, then 5 minutes of questions. The timing column can be compressed to 20 minutes or stretched to 30 (see [Timing variants](#timing-variants)). Each slide lists **what to show** and **what to say**.

**Slides:** the deck *UrbanFlow 2.0 Demo Day* (14 slides with speaker notes) is a private claude.ai Slides artifact owned by the project owner. It exports to PowerPoint (.pptx) or PDF from its Share › Export menu. It follows this run of show; slide 7a, *Version numbers are not data*, is the CDC lesson.

**Evidence rule for the talk.** Show only Final Project evidence. Lab and Demo 3 live results are not Final Project results and are not shown as such.

**Live evidence available today (Personal DEV, 2026-10-12):**
- step 1 deploy;
- step 2 platform run `896549012431425`: 40 events → Silver 40 → 13 actionable priorities; 40 trips; 48 weather hours with 4 matched trips; CDC `batch_001` 5 inserts; all reconciled;
- step 3 strict idempotency run `412521206243476`: 19/19, with no data change, no rewrites and the CDC checkpoint unchanged;
- step 4 CDC:
  - `batch_002` run `343513312512955`: 1 insert, 2 updates, 1 delete; history 11; checkpoint 3;
  - then an automatic `OPTIMIZE` made version 4; the corrected design moved the checkpoint 3 → 4 in `batch_003` run `500336374896739` with no events;
- step 5 Lakeflow run `441981394008476`: update `a26b4385` COMPLETED; 22 expectations with 0 failed records; row-for-row parity with the imperative tables.

- 2026-10-13, assistant:
  - ingest run `870039412110159`: 8 documents, 30 chunks;
  - AI Search index ready with 30/30 rows;
  - live evaluation run `712590508231976`: RAG correctness 0.685 vs baseline 0.148, recall 1.0, citation precision 0.967, refusal decisions 18/18, 0 RAG errors;
  - endpoint deleted after 25.7 minutes, guarded by a scheduled cleanup Job.
- 2026-10-13: all 25 dashboard queries ran. The governance demo filtered, masked, rolled back and dropped its schema.

- 2026-10-13, Azure trial (PROD-style, not paid production): 7 resources created; sample run `240527908342651` gave the same 40 / 40 / 13 / 13, 40 trips, 48 hours, 4 matched trips, CDC 5 inserts with reconciliation 0.
- Dashboard title fix deployed; published to the owner only.

Not demonstrated at the release merge: a GitHub CD run (identity/environment closeout), multi-principal governance, Zerobus, Event Hubs, the full month and the external Federation source.

## Run of show

| # | Time | Slide | Labs |
|---|---|---|---|
| 1 | 0:00–1:30 | The problem: an empty station and a van sent to a broken one | — |
| 2 | 1:30–3:30 | One lakehouse, two questions (architecture) | All |
| 3 | 3:30–6:00 | Ingestion that stops by itself | 1, 2, 3 |
| 4 | 6:00–8:30 | Trust: Silver, quarantine and reconciliation | 4, 7 |
| 5 | 8:30–10:30 | Gold KPIs, Lakeflow parity and the dashboard | 5, 6 |
| 6 | 10:30–13:30 | Maintenance CDC: only what changed, reconciled before the checkpoint | 10 |
| 7 | 13:30–17:00 | The operations assistant: grounded, cited, measured | 12 |
| 8 | 17:00–18:00 | Optional push ingestion with Zerobus, isolated | 11 |
| 9 | 18:00–19:30 | DEV to PROD: CI, approvals, plan gates | 8, 9 |
| 10 | 19:30–20:30 | Cost (measured), safety and what is honestly not done yet | — |
| 11 | 20:30–25:00 | Questions | — |

## Slides

### 1. The problem (1.5 min)

**Show:** a station with 0 bikes next to one marked out of service. **Say:** "Dispatchers need two answers. Where should the van go next, and where must it not go? When something breaks, they also need the procedure, quickly. UrbanFlow 2.0 answers both from one governed lakehouse."

### 2. Architecture (2 min)

**Show:** the overall diagram from the README. **Say:**

- "One bundle and two Jobs: the platform Job and the assistant Job. One schema per target. Everything is serverless and bounded."
- "DEV is my personal workspace. PROD is an Academy Azure workspace. They are different clouds and different metastores, so a DEV mistake cannot touch PROD data."

### 3. Ingestion (2.5 min)

**Show:** the `land_sources` task and `landing.py`. **Say:**

- "The station snapshot lands once in a Unity Catalog Volume. Auto Loader picks it up with `availableNow`, processes the backlog, and stops. The checkpoint makes the next run incremental."
- "On Azure, the same Bronze can come from Event Hubs over Kafka."

**Point at:** `land_once` refuses to overwrite a landed file with different content.

### 4. Trust (2.5 min)

**Show:** the three-way split and the reconciliation identity. **Say:** "Every Bronze row ends up as Silver, Quarantine or Duplicate, and the counts must add up exactly. A rejected row is a finding with a named rule; a vanished row is a bug." Mention Great Expectations on local Spark, and the final validation task that gates the Job.

### 5. Gold and dashboard (2 min)

**Show:** the generated dashboard definition (or the published dashboard after a live run). **Say:**

- "The priority score is severity plus deficit plus size, so you can recompute it by hand."
- "Each page states its limits. The snapshot page is not a trend. The weather page shows association, not causation."
- "Lakeflow re-implements the station path declaratively in its own schema, with expectations. It imports the same rule functions as the notebooks. One triggered serverless update produced the same 40 / 13 / 13 rows, matched row for row in both directions, and all 22 expectations reported zero failures."

### 6. Maintenance CDC (3 min)

**Show:** `cdc_sql.py` and the three snapshot batches. **Say:**

- "Maintenance tickets live in an operational system. A read-only Federation query or a fixture snapshot is MERGEd into a table with Change Data Feed. Unchanged rows aren't rewritten, so CDF holds only real changes, deletes included."
- "We read exactly the versions after the checkpoint, keep every event in an audit history, and update Silver."
- "The checkpoint moves only after reconciliation passes: values compared both ways at the processed version. So a crash mid-run replays safely."

**Demo:** `pytest tests/test_cdc_pipeline.py -q`. Live: `batch_002` gave 1 insert, 2 updates (before and after images) and 1 delete. `MT-1003` left Silver, but its delete stays in the audit history.

**The lesson (1 min):**
- "Five seconds after that MERGE, Databricks ran an automatic `OPTIMIZE` and the source jumped to version 4 with no data change. I stopped the next batch, because version numbers are not data."
- "My first fix kept the checkpoint behind such versions. In review I saw that it would go stale until the Delta log no longer held them."
- "The final rule: the checkpoint is the newest version that was scanned and reconciled. An `OPTIMIZE`-only range needs no MERGE but is passed. A range the log or VACUUM no longer holds fails closed with a recovery instruction."
- "`batch_003` proved it live: no events, no MERGE, checkpoint 3 → 4."

**Payoff:** the dashboard table *Stations with an open maintenance ticket*: every open ticket, with a HOLD note where a van would otherwise be sent.

### 7. The assistant (3.5 min)

**Show:** the eight procedures and one cited answer. **Say:**

- "The corpus is a governed Delta table with Change Data Feed, and an AI Search Delta Sync index follows it. Retrieval is hybrid: vectors plus keywords."
- "The prompt allows only the numbered excerpts, requires citations, and refuses when the procedures don't cover the question."

**Then the lesson:** "The Lab 12 design scoped excerpts by the first word of each title. That was right for runbooks named after systems. For our topic documents it dropped the very document that answered, so scoping is now opt-in. On the offline fixture, recall went from 0.625 to 0.938."

**Live result (show the E5 evidence table):**
- "Same model, same 18 questions. With retrieval it answered all 16 answerable questions with citations and refused the 2 it should. Without retrieval it refused 5 it could have answered, and wrote generic text for the rest."
- "The scores were not tuned after the fact. One answer was right but scored 0 on my keyword metric, because the model wrote 'low‑bike threshold' with a Unicode hyphen. I report that as a metric limitation."
- "The endpoint lived 26 minutes. A scheduled guard Job would have deleted it at 2.5 hours, even if my laptop had died."

**Demo:** `urbanflow-rag ask "What should an operator do when both bikes and docks are critically low?"` (offline fixture). **Say clearly:** "The offline fixture tests the pipeline; the live run above measured the model." 

### 8. Zerobus (1 min)

**Say:** "Optional push ingestion of synthetic station-health readings. It delivers at least once. Silver keeps the earliest delivery per event ID, and running the MERGE twice proves idempotency. It is isolated: the code refuses Lab 11's schemas, tables and credentials."

### 9. Release (1.5 min)

**Show:** the CD workflow graph. **Say:**

- "CI runs a secret scan, lint, 858 passing tests at final release, a dashboard freshness check, offline bundle schema validation, and a change-scope check, so a Final Project PR cannot touch the labs or Demo 3."
- "CD is manual and starts in plan-only mode. A read-only plan is validated and uploaded before anyone approves. After approval it plans again, deploys, and requires an all-`skip` post-deploy plan. An optional sample run is bounded, validated by script and cancelled if it overruns."
- "Compared with Lab 08: same principles, no PAT, no Terraform (nothing in Azure to manage), and no automatic deploy on push." (Table in `CI_CD.md`.)
- "PR #75 is merged and the manual CD workflow is on `main`; its first `plan_only` demonstration is tracked separately from the validated operator releases."

### 10. Cost, safety, honesty (1 min)

**Say:**

- "No schedules, every task bounded, every billable switch off by default."
- "Measured from `system.billing`: a full sample platform run is about 1.3 DBU, roughly 45 cents at list price. The verification queries cost more than the runs."
- "The biggest risk is an AI Search endpoint left running: about 23 cents an hour, used or not. So it exists only for an approved window and is deleted at the end."
- "Not done yet: the assistant with AI Search, dashboard on live data, governance demo, PROD, and the CD workflow run. Each has a bounded proposal." Point to the evidence table in [RELEASE_AND_VALIDATION.md](RELEASE_AND_VALIDATION.md).

## Live demo checklist (before the talk)

- [ ] `pytest -q` is green on the presented commit; CI is green on the PR.
- [ ] `python scripts/build_dashboard.py --check` passes.
- [ ] `python scripts/run_zerobus_health.py --target personal_dev --run-id demo-day` prints a DRY_RUN plan.
- [ ] `python scripts/run_governance_demo.py --target personal_dev` prints a DRY_RUN plan.
- [ ] If a live run was approved: its run IDs are in the evidence table and the dashboard is published. If the AI Search endpoint is still up for the demo, delete it right after the talk (with approval).

## Expected questions

| Question | Short answer |
|---|---|
| Is this real CDC? | It is Delta Change Data Feed on a Final-Project-owned table, fed by snapshots, not log-based CDC of the source database. The source is only read. |
| Why fixtures instead of a real maintenance system? | No approved external database exists for the project. The Federation path is implemented and switched on by an approved foreign catalog; the fixtures make the same logic testable. |
| What if the Job fails halfway? | Each task is idempotent and execution-scoped. CDC replays the same version range. The checkpoint never moves past an unreconciled version. |
| How do you know the assistant isn't making things up? | Citations per sentence, a refusal rule, unanswerable questions in the evaluation set, and a no-retrieval baseline on the same model. |
| Why is PROD a trial workspace? | It is what the Academy provides. It is labelled PROD-style; the separation from DEV (different cloud and metastore) is real. |
| What does it cost? | A sample platform run is 1.2–1.4 DBU (≈ $0.45 at list), measured in `system.billing.usage`. AI Search would be ≈ $0.23 per endpoint-hour (measured on Lab 12's endpoint in this workspace). These are list prices, not the invoice. See [COST_AND_PERFORMANCE.md](COST_AND_PERFORMANCE.md). |
| Why did the checkpoint move to 4 when nothing changed? | Version 4 is an automatic `OPTIMIZE`. The checkpoint means "scanned and reconciled", not "last data change", which is reported separately (3). Keeping it behind would let it go stale until the Delta log or VACUUM removed those versions. |
| What happens when CDF history has expired? | The run fails closed with `CdcRetentionGapError`: nothing is written and the checkpoint stays where it was. The message gives the re-baseline steps. This path is tested offline only. |
| Is the Lakeflow pipeline a copy of the notebooks? | No. The declarative layer defines tables, expectations and the graph, and imports the same rule functions. Parity was proven live, row for row in both directions. |
| Why no Terraform, unlike Lab 08? | Every Final Project object is bundle-managed and no Azure infrastructure is created, so Terraform would add state and permissions without a resource to manage. |
| Why was the CD workflow never run? | `workflow_dispatch` works only from the default branch, and the environments and identities do not exist yet. Every gate it calls was exercised live by operator releases with the same scripts. |
| How is DEV kept from touching PROD? | Different cloud, workspace and metastore. Hosts are pinned per target, shared Academy hosts are refused, schemas are per target, and each dashboard is generated for one target only. |
| Is the governance demo real enforcement? | It ran live: the filter showed 0, 28 and then 40 stations as the mapping changed; coordinates were rounded and ride IDs hashed; everything was rolled back and the schema dropped. With one identity, multi-user enforcement is not demonstrated, and the docs say so. |
| Why is RAG correctness only 0.685? | The metric is a literal keyword match. A manual review found every RAG claim supported by its cited procedure. The scores are reported as computed, with the limitations listed. |
| What stops a forgotten AI Search endpoint? | A temporary scheduled Job that deletes only that endpoint once it is 150 minutes old, tested before creation and deleted afterwards. The endpoint was actually removed by hand after 25.7 minutes. |

## Timing variants

- **20 minutes:** cut slide 8 (one line on slide 2) and shorten slide 4 to 1.5 min. Take questions from 18:00.
- **30 minutes:** extend slide 7 with a live baseline-vs-RAG comparison of two questions (2 min). Walk the CDC batch 002 replay on slide 6 (2 min). Allow 6 minutes for questions.

## Five-minute interview demo

For a technical interview rather than Demo Day: one problem, one architecture, three proofs.

| Time | Show | Say |
|---|---|---|
| 0:00–0:45 | README architecture diagram | "A governed lakehouse that tells dispatchers where to send a van and where not to, plus an assistant over the operating procedures. One bundle, DEV on AWS, PROD-style on an Azure trial, serverless only." |
| 0:45–2:00 | `evidence/2026-10-12_personal_dev_step2_pass.md` | "The first live run failed on three defects that offline tests could not see: a count copied from an earlier project, a missing task dependency, and an input file nobody landed. I fixed each in code with a regression test and recorded every failed run. The passing run reconciles 40 events through Silver to 13 actionable priorities, matching an independent calculation." |
| 2:00–3:15 | `evidence/2026-10-12_personal_dev_step3_retry_pass.md` | "Then I proved replays are safe, with 19 strict criteria. The first attempt failed one: a Delta MERGE that changes nothing still commits, so the CDC checkpoint moved over an empty version. I added a compare-first guard. Now a replay changes no data and no timestamps, rewrites no files, and leaves the checkpoint alone. Some shared tables still write a metadata-only commit, and I say so." |
| 3:15–4:15 | `evidence/2026-10-12_personal_dev_step4_batch003.md` | "CDC is Delta Change Data Feed over snapshots, not log-based CDC. After a live update-and-delete batch, Databricks ran an automatic OPTIMIZE. I stopped, reviewed my own first fix, and defined the checkpoint as 'scanned and reconciled'. The replay then moved it over the empty version with no events. Lakeflow, run once, matched the imperative tables row for row." |
| 4:15–5:00 | `docs/IMPLEMENTATION_PROGRESS.md` | "The assistant, dashboard, governance demo and Azure trial release all passed with evidence. The remaining optional integrations need additional infrastructure or identities. Nothing billable is left running, and estimated costs are explicitly separated from measured billing records." |

