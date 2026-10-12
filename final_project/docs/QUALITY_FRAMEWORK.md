# Great Expectations or Soda: the decision, and the measurement behind it

The Academy requirement is "Great Expectations **or** Soda", so one suffices and adding both would
be duplication. The choice was settled by measurement rather than preference.

## The measurement

Both resolve against this project. The difference is what they do to it.

| Framework | Packages installed | Effect on pyspark |
|---|---|---|
| `soda-core-spark-df` 3.5.6 | 26 | Declares `pyspark>=3.4,<4.0`; pip selects 3.5.9, a downgrade from the 4.1.1 in use |
| base `great_expectations` 1.23.2 | 16 | **None.** pyspark does not appear in the base package's resolution |

Being precise about the constraint: Soda does not require exactly 3.5.9. Its current Spark adapter
requires `pyspark>=3.4,<4.0`, and pip selects the newest permitted release, 3.5.9. This project now
declares `pyspark>=3.5,<4.2`, which includes the 4.1.1 engine the suite actually uses. Installing
Soda there would therefore resolve Spark down to 3.5.9 solely to add a quality framework.

GE also publishes an optional `spark` extra with `pyspark>=2.3.2,<4.2`. UrbanFlow installs the base
package because Spark already exists in the development environment; either way, 4.1.1 is within
GE's supported constraint.

Reproduce with:

```bash
python -m pip install --dry-run soda-core-spark-df   # lists pyspark-3.5.9
python -m pip install --dry-run great_expectations   # lists no pyspark
```

## Why that settles it

Soda's Spark integration excludes Spark 4. Installing it would change the engine that **every
Spark test in this project runs against** - at the time of writing, the suite has over 100 tests
executing against a real local Spark session, including the station-join behaviour, the
three-way quality split and the schema-migration checks. Swapping Spark 4.1.1 for 3.5.9
underneath them would invalidate all of that evidence to gain a quality framework, which is a bad
trade whichever framework is better in the abstract.

The base Great Expectations 1.23.2 package installs 16 packages, none of them Spark, and was verified working
against pyspark 4.1.1 before being adopted: a deliberately null-containing frame produced
`success=False` with `unexpected_count=1`.

On 2026-10-03 the suites also ran through Databricks Connect against the existing persisted tables
on GP1. Silver passed 10/10 expectations and the named 40-row historical development sample passed
10/10, with zero failures and zero unexpected rows. The historical duration check derives its
expression and bounds from `historical.py`, so the external framework continues to validate the
production contract rather than redefine it. See
[the Batch A evidence](../evidence/BATCH_A_READ_VALIDATION.md).

So "the lightest option that satisfies the requirement" is Great Expectations - lightest in the
only sense that matters here, which is impact on what already works.

## What GE adds, given the project already has a quality layer

This is a fair question, because the project is not short of quality checks:

| Layer | What it does | What it does not do |
|---|---|---|
| `quality.py` | Routes every row to valid or quarantine with named failed rules | Produces no portable report |
| `pipeline/*.py` | 22 declarative Lakeflow expectations, all passing in update `ba6710ed-…` (2026-10-03) | Report only inside the pipeline event log |
| `reconcile_*` functions | Prove counts tie out between layers | Check totals, not per-column rules |
| **Great Expectations** | **A portable, per-expectation validation report** | Does not route or persist anything |

The gap GE fills is the report. A reviewer asking "which specific rules were checked, and what
did each one observe?" currently has to read code and run the pipeline. A GE validation answers
it directly, in JSON that drops into the same evidence files every other phase writes.

## The rules deliberately mirror the implementation

`src/urbanflow/expectations.py` restates rules that `silver.py` and `historical.py` already
enforce. It does **not** invent a second contract, and a test asserts the availability-status set
matches the implementation's.

That restraint is the point. A third-party framework that quietly disagrees with the pipeline it
validates is worse than having none at all: it produces confident green reports about the wrong
rules, and the disagreement surfaces only when someone compares them by hand.

## What the suites catch

The suites are tested against the specific defects this project has actually produced, not just
against clean data - passing on good rows proves very little.

| Expectation | The real failure it guards |
|---|---|
| `event_id` unique | The Silver MERGE key; a duplicate breaks idempotency |
| `execution_id` not null | An unassigned row is exactly what let 89 stale priority rows escape a scoped delete |
| `availability_status` in the known set | A new status must be a deliberate contract change |
| `num_bikes_available` >= 0 | A negative count is a parse or source defect |
| `start_station_id` is a **String** | Typed as a double, `5470.10` and `6740.10` silently stop matching any GBFS short_name, and the join quietly returns fewer rows while still looking plausible |
| `member_casual` in (member, casual) | A third value would skew every rider-mix figure |

## Cost and footprint

No infrastructure. `gx.get_context(mode="ephemeral")` keeps everything in memory: no project
directory, no datasource config files, no state surviving the call. GE is a dev and test
dependency, so it is installed by CI and available in a notebook, and it adds no Azure resource,
no warehouse and no running service.

## Status

`Implemented locally`. The suites run against real local Spark in CI. They have not been run
against the live Azure tables, which would be a natural, cheap addition to the next GP1 batch -
it is a read-only validation and needs no new resource.
