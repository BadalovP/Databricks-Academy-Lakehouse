# LAB 09 — Classic-cluster requirement: reviewer package

**Status: committed and published to `feature/lab09-rest-api-automation`
(PR #23) for reviewer/mentor consideration. No Databricks action has been
taken as part of preparing this document.**

This package exists to let a reviewer/mentor judge, in one place, exactly
what has and has not been demonstrated against the literal "create
clusters" wording of the Lab 9 task, and to decide between the two
compliance options this project has documented since Phase 0.

---

## 1. The exact cluster-creation requirement

This repository does not contain the original academy assignment text as
a separate document — Lab 9 was scoped and built directly from the task
description given at project inception, and that description is not
itself checked into this repo. What *is* checked in, consistently, since
the first commit, is this project's own restatement of the requirement
(`README.md` §1 "Goal" and the "Lab requirement vs. Personal workspace
reality" section):

> "Build a production-like automation package that drives a Databricks
> lakehouse end to end through the Databricks Python SDK / REST API...
> authenticating, discovering/creating resources, **provisioning
> compute**, triggering and polling long-running work, and reconciling
> the result."

> "The Lab 9 task literally asks the automation to **create clusters**."

If the original assignment wording differs from this restatement in any
material way, that should be checked against the actual academy task
sheet, which this repository does not have a copy of.

> **Request to reviewer:** please confirm the exact wording of the
> original cluster-creation requirement from the academy assignment
> itself (not this project's restatement of it) — specifically, whether
> it requires a classic cluster to actually reach a running state, or
> whether demonstrating the implemented, tested, live-invoked code path
> against a workspace that cannot support classic compute is sufficient.
> Everything below is written to support either answer, but this project
> cannot resolve the question on its own without a copy of that original
> text.

## 2. Clusters API operations implemented (`src/lab09/compute.py`)

Every operation needed for a full create → verify → terminate cycle is
implemented, tested (mocked), and has been exercised live at least once
(the failing attempts below did reach the real API):

| Operation | Function | SDK call |
|:--|:--|:--|
| List Spark runtimes | `resolve_lts_spark_version()` | `clusters.spark_versions()` |
| List node types | `resolve_node_type()` | `clusters.list_node_types()` |
| List cluster policies | `resolve_policy_id()` | `cluster_policies.list()` |
| Build a cluster spec | `build_cluster_spec()` | (pure logic, combines the above) |
| **Create** | `start_cluster_create()` / `try_start_cluster_create()` | `clusters.create(**kwargs)` |
| **Poll / verify running** | `monitoring.poll_cluster_state()` | `clusters.get(cluster_id=...)`, looped explicitly, never `.result()` |
| **Terminate** | `terminate_cluster()` | `clusters.delete(cluster_id=...)` |
| Existence/state check before cleanup | `cluster_exists_and_active()` | `clusters.get(cluster_id=...)` |

The dedicated capability check, `preflight.py`'s `_run_cluster_create_probe()`,
wires these together as a create → poll → terminate probe: builds a spec,
calls `start_cluster_create()`, polls to `RUNNING` with a bounded timeout
(`cluster_timeout_seconds`, currently 900s), and calls
`compute.terminate_cluster()` in a `finally` block. **This is the same
code path a live demonstration in another workspace would reuse
verbatim; no new code is required for that** — but its cleanup behavior
is narrower than "unconditional" and is corrected here rather than
overstated:

- The `finally` block only calls `terminate_cluster()` **if a
  `cluster_id` was actually captured** (`if cluster_id: ...`). If
  `start_cluster_create()` itself raises before returning an id — exactly
  what happened in the real timeout in §3 below — there is no known id to
  terminate, and the current code does **not** search for a possibly
  orphaned cluster by name or tag afterward.
- A create call that times out client-side does not, by itself, prove
  nothing was created server-side: the request may have been accepted by
  the backend even though the client never received a successful
  response. **The creation outcome in that case is uncertain, not
  negative**, until independently checked (see §3's own verification,
  and §8's plan for how future attempts should check this explicitly).
- `terminate_cluster()` itself only issues the delete request
  (`clusters.delete(cluster_id=...)`) — it does not poll to confirm the
  cluster actually reaches `TERMINATED`. **A termination request being
  accepted is not the same claim as termination being confirmed.**

**Conclusion: the implementation is complete.** Nothing about the
"create clusters" requirement is unimplemented in code. The gap is
entirely in the *result* of calling it against this specific workspace.

## 3. The real cluster-creation attempt and its backend error

`preflight --probe-cluster-create` was run live (not simulated) against
the confirmed-safe Personal workspace, twice (once before, once after an
unrelated architecture-selection bug was found and fixed). Both attempts
failed identically:

```text
TimeoutError: Timed out after 0:05:00
  | caused by BadRequest: Current organization <org-id> does not have
    any associated worker environments
```

Root-caused by reading the installed `databricks-sdk`'s own source, not
assumed: the SDK's HTTP client retries a `BadRequest` containing "does not
have any associated worker environments" for its default 300-second retry
window before giving up — the `TimeoutError` is a symptom of the SDK's own
retry policy, not a hang in this project's code. Because `create()` itself
raised before returning a `cluster_id` (see §2's corrected cleanup
description), the code had no id to terminate and no way to know from the
exception alone whether a cluster object existed server-side. **That
"no cluster was created" conclusion rests on a separate, deliberate,
read-only verification** — `databricks clusters list` run independently
before and after both attempts, both times returning zero clusters — not
on the create call's own cleanup logic, which never ran here.

## 4. Evidence that classic compute is unavailable in this workspace

- The failure is a `BadRequest` from the Databricks control plane itself,
  not a client-side exception — it is the backend's own answer.
- The identity holds the `allow-cluster-create` entitlement (confirmed via
  `current-user me` before any attempt), ruling out a permissions
  explanation.
- A separate, real bug (`resolve_lts_spark_version`/`resolve_node_type`
  ignoring CPU architecture, risking an `aarch64` runtime paired with an
  x86_64 node type) was found and fixed during this same investigation.
  The corrected, architecturally-valid spec **still failed identically**,
  ruling out `ClusterSpec` correctness as the cause.
- "Does not have any associated worker environments" is an
  organization/workspace-level infrastructure statement from Databricks
  itself — this Personal workspace's organization has no backend capacity
  provisioned for classic (non-serverless) compute at all. This is not
  something any client-side code or configuration change can work around.
- Full detail (with identifiers masked for public consumption):
  `evidence/LIVE_VALIDATION_SUMMARY.md` §1, and (local-only, unsanitized,
  for this project's own traceability) `evidence/phase0_2026-09-24.json`.

## 5. The successful complete serverless `run-all` execution

Separately, and successfully, a single `python -m lab09.cli run-all
--compute-mode serverless_job` command has since completed **every other**
part of Lab 9 end to end, in one continuous invocation, with no manual
per-stage intervention:

- Reused the existing Lakeflow pipeline and the existing persistent Job
  (neither recreated).
- Landed a new month via the Files API without touching previously landed
  months.
- Drove the Lakeflow pipeline update to `COMPLETED` — zero error events,
  zero Databricks auto-retries.
- Ran the reconciliation Job to `SUCCESS` on Databricks-managed
  **serverless** compute (no `existing_cluster_id`, `new_cluster`, or
  `job_cluster_key` on its task).
- Populated all four output tables with real data; the reconciliation
  invariant (`bronze_rows == silver_valid_rows + rejected_rows`) held
  against that real data.

Full detail: `evidence/LIVE_VALIDATION_SUMMARY.md` §8.

## 6. The remaining distinction: implemented vs. demonstrated

| | Status |
|:--|:--|
| Cluster creation **code path exists, is complete, and is correct** | ✅ Yes — `compute.py`, exercised live (reached the real API, got a real backend answer) |
| Cluster creation **actually succeeds** against a real backend, reaching `RUNNING` | ❌ No — proven, live, twice, to be impossible on this specific workspace |
| Every *other* Lab 9 capability (auth, discovery, Volumes, Files API, incremental ingestion, Lakeflow pipeline management, persistent Job management, explicit polling, reconciliation, end-to-end `run-all`) | ✅ Yes — all proven live |

This is a workspace/infrastructure limitation, not an implementation gap.
The distinction matters for grading: the code that would create a cluster
is present, tested, and was actually invoked against a real API — it just
cannot succeed *here*.

## 7. Does existing evidence already cover everything except successful classic cluster creation?

**Yes.** Cross-checking §5's `run-all` result against Lab 9's own
"Completion requirements" (`README.md` §2) and the API/SDK operations
table (`README.md` §5): authentication, preflight, Volume creation, Files
API upload, incremental ingestion, workspace file upload, Lakeflow
pipeline management, persistent Job management, explicit polling, and
JSON reporting are all demonstrated live, together, in a single
`run-all` run. Only a **successful classic cluster reaching `RUNNING`
state** remains undemonstrated — and that is now well-evidenced as a
workspace limitation rather than an open task.

---

## 8. If a separate demonstration is required: minimal test plan

**As originally written below: not yet executed, and scoped to a
confirmed-non-production workspace only.** *(Update: this plan's
Clusters-API and Jobs-API steps have since actually been executed — but
against Azure PROD, not the confirmed-non-production workspace this plan
required, and with a manual restart between the create and Jobs-API
steps. See §9 below for exactly what ran, including that deviation,
before treating this section as either fulfilled or superseded.)* This
project will not select or guess a target workspace.

### Scope — deliberately narrower than `preflight --probe-cluster-create`

The existing `preflight --probe-cluster-create` CLI command also runs
catalog/schema/volume/Files-API checks scoped to *this* project's
`config/dev.yml` — which may not correspond to anything meaningful in a
different workspace. For a **minimal**, side-effect-free demonstration,
the plan below calls only the Clusters API primitives directly (the exact
same functions from §2), with **no Unity Catalog, Volume, pipeline, or
Job interaction of any kind** in the target workspace.

### Pre-conditions (all three must be explicitly approved by name — a human confirms, this project does not infer any of them)

1. **You must explicitly approve, in writing, all three of:** the exact
   `~/.databrickscfg` profile name, the exact resolved host, and the
   exact resolved identity. This project will not infer, guess, or
   proceed on a partial match — e.g. a correct profile name is not
   sufficient approval on its own if the host or identity has not also
   been separately confirmed.
2. `databricks auth env --profile <name>` and `current-user me
   --profile <name>` are run first (read-only) to resolve the actual
   host/identity, which are then shown to you for the approval in step 1
   — *before* any create call, not as a formality after the fact.
3. **Safeguard — never Azure PROD:** the resolved host is checked against
   `<the known Azure PROD host — see README.md "Security model">`. **This
   placeholder is for this published document only** (identifiers are
   masked here per the privacy policy this project follows). **The plan
   itself, when actually executed, must perform a real, executable
   string comparison against the literal Azure PROD hostname** — sourced
   at run time from locally held configuration already present in this
   repository/environment (this project's own `README.md` "Security
   model" section already states it, and it is independently resolvable
   from the operator's own `~/.databrickscfg`) — never hardcoded into a
   newly published or public artifact, and never guessed, approximated,
   or skipped. If the resolved host matches that real value, or does not
   exactly match a host you approved in step 1, the plan stops before
   step 5.
4. `databricks clusters list --profile <name>` is run first (read-only)
   to record every cluster's id and **state** (not just a count) as the
   pre-existing baseline. Terminated clusters may legitimately already be
   present in this listing — see step 12.

### The test itself (create → verify → terminate, one attempt)

5. Generate a **unique** cluster name and a unique identifying tag for
   this specific attempt (e.g. incorporating a timestamp and/or a random
   suffix) — not a static, reusable label. This is what lets step 9 find
   the right cluster unambiguously even if other clusters exist, and
   lets a human find it unambiguously for manual cleanup if step 11
   cannot confirm termination.
6. Resolve a Spark version and node type dynamically in the target
   workspace (`compute.resolve_lts_spark_version`,
   `compute.resolve_node_type` — read-only `clusters.spark_versions()` /
   `clusters.list_node_types()` calls, same architecture-aware logic
   already fixed and tested).
7. Build a minimal `ClusterSpec` using that unique name/tag: single-node
   (`num_workers=0`), no cluster policy unless one is explicitly named by
   you, `autotermination_minutes=20` set as a redundant safety net in
   addition to (not instead of) the explicit termination below.
8. Make **exactly one application-level call** to
   `compute.start_cluster_create()` — no application-level retry loop.
   Note: the installed SDK's own HTTP client may still perform its own
   **transport-level** retries beneath that single call (this is exactly
   what produced the 5-minute `TimeoutError` in §3) — this plan does not
   disable or fight that behavior, and does not count it as a second
   application-level attempt; it is inherent to the one call made.
9. **If step 8 raises without ever returning a `cluster_id`** (a create
   timeout or similar): do **not** conclude that nothing was created.
   Instead, call `clusters.list()` (read-only) and search for a cluster
   matching the unique name/tag from step 5. If one is found despite the
   client never receiving a successful response, treat it exactly as if
   step 8 had returned that id and continue at step 10. If none is
   found after this check, record that explicitly as the verified
   outcome (not merely assumed).
10. **If a `cluster_id` is known** (returned directly by step 8, or found
    via step 9's search): poll with `monitoring.poll_cluster_state()` to
    a bounded timeout (15 minutes, matching `config/dev.yml`'s existing
    `cluster_timeout_seconds`) to observe whether it reaches `RUNNING` —
    never an unbounded wait.
11. Request termination — call `compute.terminate_cluster()` — and then
    **independently verify the outcome with a dedicated, bounded,
    read-only polling loop that calls `clusters.get()` directly.**
    **Correction: this must *not* reuse `monitoring.poll_cluster_state()`
    as-is.** That function also stops on `RUNNING` (it was designed to
    wait for a cluster to *start*, not to confirm one has stopped) — a
    single call to it immediately after `delete()` could return right
    away on an observed `RUNNING` state (e.g. if the state read happens
    before the deletion has taken effect, or during a brief transitional
    window), which would be mistaken for "polling finished" when
    termination has not actually been confirmed at all. The dedicated
    loop for this step must instead:
    - keep polling — never stop — while the observed state is `RUNNING`
      or any transitional state such as `TERMINATING`;
    - stop **successfully** only when the observed state is exactly
      `TERMINATED`;
    - stop and flag **"requires investigation"** if the observed state is
      `ERROR` or `UNKNOWN`;
    - stop and flag **"requires investigation"** if the bounded timeout
      (e.g. up to 10 minutes) is reached without ever observing
      `TERMINATED`;
    - stop and flag **"requires investigation"** if `clusters.get()`
      itself raises an API error while polling — an error response must
      never be assumed to mean the cluster is already gone.

    A termination request being accepted by `delete()` is not, by itself,
    evidence of `TERMINATED`; only this loop's observed final state is.
12. **None of the four "requires investigation" outcomes above (`ERROR`,
    `UNKNOWN`, polling timeout, or an API error while polling) may be
    treated as proof that cleanup succeeded.** If any of them occurs:
    **stop.** Do not attempt any further automated action (no retry of
    the delete call, no alternate cleanup path, no assumption of success).
    Output, in full, everything a human needs to clean it up manually or
    investigate further: the exact cluster id, the unique name/tag from
    step 5, the profile name, the resolved host, the last observed state
    (or the API error itself, if that was the outcome), and the point in
    the loop where it stopped. This is a genuine outcome to report
    honestly, not to paper over.
13. Compare the **pre- and post-attempt sets of non-terminated (active)**
    clusters from steps 4 and this step's own `clusters list` call — not
    raw listing membership or counts. **A cluster that reached
    `TERMINATED` is not required to disappear from `clusters list`** (it
    may legitimately remain listed, in that state, for some retention
    window) — the correct check is that no cluster from this attempt
    remains in a non-terminated state, not that the listing shrinks back
    to its original size.
14. Record the outcome (states observed at each step, timing, the unique
    name/tag, whether termination was confirmed or step 12 was reached)
    as a new, appropriately sanitized evidence entry — following
    `evidence/phase0_2026-09-24.json`'s existing pattern.

### Explicit safeguards

- **No Azure PROD**: hard pre-check in step 3; the plan does not proceed
  without your explicit, named approval of profile, host, *and* identity
  together (step 1).
- **Best-effort cleanup, honestly reported**: explicit termination
  request plus independent read-only verification (step 11) *and* a
  20-minute `autotermination_minutes` server-side backstop (step 7) in
  case the explicit request itself cannot be issued or confirmed. If
  verification still fails, the plan stops and reports full manual-cleanup
  details (step 12) rather than assuming success.
- **No modification of existing resources**: the plan touches only the
  one newly created, uniquely-tagged cluster from this attempt. No
  catalog, schema, volume, pipeline, or Job in the target workspace is
  read, created, or modified — this is a Clusters-API-only probe,
  decoupled from the rest of Lab 9's config.
- **One application-level attempt only**: no application-level retry if
  step 8, 10, or 11 fails or is inconclusive; a failure or an unconfirmed
  termination is itself useful, reportable evidence, not something to
  retry past.
- **Cost-conscious**: smallest available non-deprecated node type,
  single-node, no workload ever runs on the cluster (it is never attached
  to a job or notebook execution) — total expected lifetime is minutes,
  bounded by the 15-minute poll timeout (step 10) plus the 10-minute
  termination-verification window (step 11), or the 20-minute
  autotermination backstop if those steps cannot run to completion.

---

## 9. Actual execution result (2026-09-27) — and an explicit deviation from §8's "No Azure PROD" safeguard

**The plan in §8 has since been executed** — with one material,
deliberate deviation from how it was scoped above, recorded here plainly
rather than silently updated away.

§8's "Explicit safeguards" state: *"No Azure PROD: hard pre-check in step
3; the plan does not proceed without your explicit, named approval of
profile, host, and identity together."* That safeguard assumed any
supplementary demonstration would run against a confirmed
**non-production** workspace. **What actually ran instead was executed
against Azure PROD itself** — the same workspace referenced elsewhere in
this project as "Azure PROD" — under a separate, specific, explicit
authorization obtained directly from that workspace's owner for this one
scoped test, given after being told the workspace would be treated as
Azure PROD regardless of profile name or group membership. It was not an
application of §8's plan as originally written; §8's own "No Azure PROD"
premise did not hold for this execution, and this document does not
represent otherwise.

**Controls actually followed, despite running in Azure PROD:**

- Authentication used a dedicated OAuth user-to-machine profile created
  solely for this test; the operator's pre-existing long-lived Personal
  Access Token for this workspace was never accessed, displayed, logged,
  or used.
- Only newly created, uniquely-named/tagged resources (one cluster, one
  Job, one notebook) were ever created, started, or modified.
- Every pre-existing resource in that workspace — including two other
  long-running personal clusters — was independently, repeatedly
  read-only-checked before and after every step and was never started,
  stopped, modified, or deleted by this project's automation at any
  point.
- Exactly one application-level cluster-create attempt was made after a
  real bug (a Photon-runtime tie-break in `resolve_lts_spark_version()`,
  the same class of bug as the aarch64 issue in §4) was found, root-caused
  from a genuine rejection, and fixed with regression-test coverage before
  any retry — no blind retry loop was ever run.
- Termination was independently, explicitly verified via the same
  dedicated polling loop specified in §8 step 11 (never treating an
  observed `RUNNING`/`TERMINATING` state as sufficient, only an exact
  `TERMINATED`), both after the initial provisioning attempt and after
  the Jobs-API test below.

**Result — and the manual restart in the middle, stated explicitly:** a
real classic cluster (`Personal Compute` policy, single-node,
`Standard_D4ds_v5`, a dynamically resolved standard non-Photon LTS
runtime, `SINGLE_USER`, 10-minute autotermination) was created by this
project's own automation. That automated attempt's own readiness poll
(8-minute bound) timed out while the cluster was still `PENDING` —
provisioning a node of this type in this workspace runs 5–8.5 minutes
based on historical data — so, per this script's own design, it requested
termination and independently confirmed `TERMINATED` without ever having
observed `RUNNING` itself. **The project's operator then manually
restarted that same cluster (same cluster id) via the Databricks UI,
outside any of this project's own code, and it reached `RUNNING`.** Only
after that manual restart, in a separate automated run against the
now-running cluster, did this project's own automation upload a trivial
notebook, create one temporary Job with `existing_cluster_id` pointing at
it, run it via `run_now()`, poll to completion, and retrieve output
matching the expected result exactly. It then requested termination a
second time, independently confirmed `TERMINATED`, and deleted the
temporary Job and notebook it had created. Full detail:
`evidence/LIVE_VALIDATION_SUMMARY.md` §9.

**What this does and does not settle:** this project's own automation
successfully exercised, live, cluster create, cluster terminate
(twice, both independently confirmed), and — against a cluster a human
had manually restarted — Job attachment via `existing_cluster_id`,
notebook execution, and confirmed termination. **The create-to-`RUNNING`
transition itself was not achieved by automation alone**: the automated
readiness poll timed out and the cluster was terminated before it was
ever observed `RUNNING`, and a human restarted it afterward. What this
does **not** settle on its own is (a) whether a demonstration run against
Azure PROD — under this specific, explicit, one-off owner authorization,
but outside the non-production scope §8 was originally written for — is
the right way to satisfy the requirement for grading purposes, versus
treating the serverless `run-all` proof (§5 above) as sufficient on the
Personal workspace where Lab 9 was otherwise built, and (b) whether the
manual restart between steps disqualifies this as a fully-automated
demonstration of the create-to-`RUNNING` leg specifically, even though
every step was exercised by this project's own tested code individually.
Both judgment calls are left to the reviewer/mentor, same as the
still-open Option A vs. Option B question below.

---

## Next step

This package is ready for a mentor/reviewer to read. **No decision has
been made on Option A vs. Option B** (see `README.md` "Lab requirement
vs. Personal workspace reality"). Section 9 above records that the
classic-cluster capability has now been demonstrated live — automated
create, automated terminate, a **manual restart by the project's operator
in between**, then fully automated Job attachment via
`existing_cluster_id`, notebook execution, and confirmed termination
against the manually-restarted cluster — against Azure PROD rather than
the confirmed non-production workspace this document originally scoped
§8 for. The requirement should be described as **demonstrated, with two
explicit deviations from the original §8 plan for the reviewer to
weigh (the Azure PROD workspace, and the manual restart between the
create and Jobs-API steps)** — not as cleanly and unconditionally
satisfied by automation alone, and not as still unproven.
