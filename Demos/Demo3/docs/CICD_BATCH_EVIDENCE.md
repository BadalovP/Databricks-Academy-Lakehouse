# CI/CD batch evidence - 2026-10-03

Everything below is verified against actual run IDs and API responses, not against the reports of
the runs. Where a claim could not be independently confirmed, that is said rather than smoothed
over.

**No Databricks data was mutated anywhere in this batch.** One Job ran, with its gate parameter
set to false, and it read nothing.

## 1. Jobs API dry-run - PROVEN

| Fact | Value |
|---|---|
| Job | `11834365763936` - `[azure] urbanflow_silver_gold_test` |
| Run | **`180887598258786`** |
| Result | `TERMINATED / SUCCESS` |
| Parameters | `run_transform=false`, `source_execution_id=urbanflow-20260929T195132Z-r3` |
| Tasks | `bronze_to_silver` SUCCESS, `silver_to_gold` SUCCESS |

Both tasks returned their explicit `DRY_RUN` output, so no Bronze read and no Delta write
occurred. Re-verified on 2026-10-03 with `databricks jobs get-run 180887598258786`, which
confirmed the state, the `run_transform=false` parameter and both task results.

Two further read-only `jobs submit` runs exercised the same API earlier:
`618116231829401` (whole-table verification) and `242979365945407` (dashboard SQL validation).

## 2. Idempotent Job-only deployment - PROVEN

| Stage | Plan result |
|---|---|
| Before | `0 to add, 2 to change, 0 to delete` |
| Deployment | one Job-only `bundle deploy`, Lakeflow excluded |
| After | **`0 to add, 0 to change, 0 to delete, 4 unchanged`** |

The two changes were explained, not mysterious: Job `974964732439608` gained `run_attempt_id` and
`landing_subdir`, and Job `860666167537092` gained `run_attempt_id` and `source_execution_id` -
all four being parameters this project added in earlier PRs.

**Independently re-confirmed on 2026-10-03** by running the same selected plan read-only from a
workstation that does hold local deployment state: `Plan: 0 to add, 0 to change, 0 to delete, 4
unchanged`. No deployment was repeated to produce that confirmation.

## 3. Protected GitHub environment - PROVEN, exercised twice

The `azure-release-approval` environment carries a `required_reviewers` rule naming `BadalovP`.
Captured before approving run 37083109424:

```
environment            azure-release-approval
environment_id         22307333695
wait_timer             0
reviewers              [{"type": "User", "login": "BadalovP"}]
jobs at that moment    static validation  -> success
                       approval gate      -> WAITING
                       live Azure job     -> did not exist yet
```

That last line is the substantive proof the gate works: the job which performs `azure/login` and
every `databricks` command **had not been instantiated**, so no Azure or Databricks authentication
had begun while the run waited. Approval created deployment `6821180840` on sha `f5011fa`.

The earlier run `37082229544` was held the same way and approved with the comment "Approved for
the bounded read-only UrbanFlow control-plane validation."

## 4. Workflow 37082229544 - a FAILED VALIDATION ATTEMPT, not a failed workload

This needs stating plainly because the word "failure" against an Azure job invites the wrong
conclusion.

**No Databricks workload ran. No data was read or written. Nothing was deployed.**

The run authenticated, validated the bundle, and then failed on an assertion that was simply
wrong for its environment: it required `databricks bundle plan` to report
`0 to add, 0 to change, 0 to delete, 4 unchanged` on a **fresh GitHub runner**. A fresh runner has
no local DAB deployment state, so the plan correctly described the four already-existing Jobs as
creates. The assertion was invalid, not the infrastructure.

PR #51 removed that assertion and replaced it with a comment explaining why idempotency is proved
by the separate authenticated before/deploy/after sequence in section 2 instead. Its companion
test now asserts `databricks bundle plan` is **absent** from the workflow, so the mistake cannot
return quietly.

## 5. Corrected workflow 37083109424 - PARTIAL, and still failing

Merged PR #51 (main `f5011fa`) and dispatched exactly one corrected run.

| Step | Result |
|---|---|
| Static CI (Ruff, Black, pytest, offline bundle validation) | **SUCCESS** |
| Protected environment WAITING, then approved | **SUCCESS** |
| `azure/login@v2` OIDC | **SUCCESS** |
| Pinned-host check and `databricks current-user me` | **SUCCESS** |
| `databricks bundle validate -t azure` | **SUCCESS** - "Validation OK!" |
| `databricks jobs get 404404108673495` | **FAILURE** |
| Overall | **FAILURE** |

The error:

```
Error: User *** does not have View or Admin or Manage Run or Owner
permissions on job 404404108673495
```

### Root cause, established read-only

The CI job authenticates as an Entra service principal, identified by the repository variable
`AZURE_CLIENT_ID`, which is a **different Databricks principal** from the workspace user. (The
literal client id is deliberately not reproduced here: this repository is public, and while a
client id is not a credential, publishing a service-principal and tenant identifier serves no
purpose. Read it from the repository variable when needed.) The four Jobs' ACL contains no entry
for it:

```
job 404404108673495 access control list
  parvinbadalov@softserve.academy  ->  IS_OWNER
  admins                           ->  CAN_MANAGE
```

So the service principal can authenticate and validate the bundle, because those need only
workspace access, but it cannot *read* a Job it has no ACL on.

**This is not a GitHub-side defect, so it was not retried.** It is a real Databricks permission
fact, and fixing it means changing a permission - which this batch is not authorized to do.

### Worth noting: the failure is partly good news

A CI principal that could already read everything would be a *worse* setup. This failure is
evidence that the pipeline authenticates as a distinct, least-privileged identity rather than
reusing a developer's credentials. The gap is narrow and the fix is correspondingly small.

### The proposed fix, for separate approval

Grant the CI service principal `CAN_VIEW` - read-only, cannot run or modify - on the four Jobs:

```bash
# Read the application id from the repository variable rather than pasting it into a public repo.
SP_APP_ID="$(gh variable get AZURE_CLIENT_ID --repo BadalovP/Databricks-Academy-Lakehouse)"

for id in 404404108673495 11834365763936 974964732439608 860666167537092; do
  databricks permissions update jobs "$id" --profile dev --json "{
    \"access_control_list\": [
      {\"service_principal_name\": \"$SP_APP_ID\",
       \"permission_level\": \"CAN_VIEW\"}
    ]}"
done
```

Two cautions that matter before anyone runs that:

- `permissions update` is a **PATCH**: it adds to the ACL rather than replacing it. The
  owner entry must be re-read afterwards to confirm it survived. Using `permissions set` instead
  would **replace** the whole ACL and could strip ownership.
- `CAN_VIEW` is the correct level. `CAN_MANAGE_RUN` would let CI trigger Jobs, which is exactly
  what this read-only validation path must not be able to do.

The alternative - dropping the per-Job assertions from the workflow - is **not** recommended: it
would make the validation weaker in order to make it pass, which is the wrong trade.

## 6. Post-deploy validation - PROVEN, with its limit stated

Read-only verification of the deployed state on GP1, reusing the existing Batch A evidence rather
than rerunning a workload, and independently corroborated.

| Table | Count |
|---|---|
| `bronze_station_status` | 2,520 |
| `silver_station_status` | 2,520 |
| `fact_station_availability` | 2,520 |
| `gold_station_shortage` | **657** |
| `gold_rebalancing_priority` | **657** |

Actionable Gold composition: **LOW_BIKES 278, LOW_DOCKS 374, LOW_BIKES_AND_DOCKS 5,
OUT_OF_SERVICE 0.** Great Expectations suites PASS against the live Silver and historical tables.

Independent corroboration performed 2026-10-03, no compute required:

- the Unity Catalog table inventory returns **exactly the 16 tables** the evidence claims, matching
  element for element;
- `databricks pipelines list-pipelines` returns **zero** UrbanFlow pipelines, so Lakeflow remains
  undeployed;
- the same counts were measured independently in the earlier whole-table verification run
  `618116231829401`.

**The limit:** this validation was performed as an authorized step, not as an automatic
post-deploy CI step. Automating it is blocked by the same ACL gap in section 5.

## 7. What this batch did NOT do

No second Databricks Job run. No deployment. No Lakeflow, serverless or SQL warehouse. No
dashboard publication, alert, RLS, mask, grant, ABAC, OPTIMIZE, VACUUM, CDF, deletion vector or
clustering change. No archive download or upload. No Event Hubs publish. No cluster lifecycle
change - GP1 was found RUNNING and left RUNNING.

## 8. Coverage effect

Three rows promoted to `Validated live` on the evidence above: idempotent deployment and
approvals, post-deploy validation, and the Jobs API trigger. **CI integration was deliberately NOT
promoted**, because the corrected workflow still fails; promoting a requirement on the strength of
a failed workflow is precisely the kind of overclaiming this project avoids.

The Jobs API row's *pipeline* trigger half also remains unproven, for the simple reason that no
pipeline exists to trigger.
