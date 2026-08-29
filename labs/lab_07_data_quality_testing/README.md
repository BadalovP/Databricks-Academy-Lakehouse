# Lab 07 - Data Quality Testing

Lab 07 demonstrates data quality testing for a Databricks lakehouse pipeline. It
validates Chicago business license data across ingestion, classification,
quarantine, SCD2 history, Gold aggregates, and scorecard/dashboard reporting.

## Architecture

Landing -> Bronze -> Classified/Quality -> Validated + Quarantine -> SCD2 -> Gold
-> Scorecard/Dashboard

Key flow:

- `business_license_landing` - raw prepared source data
- `business_license_bronze` - loaded landing data with source expectations
- `business_license_classified` - normalized records with quality status and reasons
- `business_license_validated` - trusted records, including WARN rows
- `business_license_quarantine` - records with critical quality failures
- `dim_license_scd2` - historical license dimension
- Gold outputs and dashboard tables - monitoring, scorecard, and reporting

## Quality States

- `VALID` - passes quality checks
- `WARN` - has non-blocking warnings, such as missing DBA name
- `QUARANTINE` - has critical failures and is excluded from trusted outputs

## Tests

Canonical test layout:

- `src/tests/` - native Lakeflow `TestPipeline` tests for the Databricks Pipeline
  Editor
- `tests/unit/` - local unit tests, including chispa DataFrame tests
- `tests/quality/` - local quality-rule tests
- `tests/integration/` - deployed integration test
- `tests/fixtures/` - reusable test datasets

The canonical native Lakeflow `TestPipeline` tests are:

- `src/tests/test_expectations.py`
- `src/tests/test_quarantine.py`
- `src/tests/test_gold_quality_flow.py`
- `src/tests/test_scd2_snapshot_flow.py`

These Lakeflow tests are intended for the Databricks Pipeline Editor. Use the
PREVIEW channel and Triggered mode. They do not run through normal local pytest.
All four have been validated successfully in the Pipeline Editor.

Local tests live under `tests/`. The currently validated local result is
`16 passed, 1 skipped`. The skipped test is
`tests/integration/test_deployed_medallion_e2e.py`, which requires a deployed
Lab 07 environment.

Chispa tests live in `tests/unit/test_chispa_dataframes.py`.

Fixtures in `tests/fixtures/` are used for snapshot evolution, duplicate
merge-source determinism, and late-arriving business states.

## Gold Outputs

Important Gold outputs:

- `license_quality_daily` - daily quality totals and quality score
- `license_quality_by_dimension` - affected records by quality dimension/status
- `license_status_summary` - trusted license counts by status
- `license_volume_daily` - trusted row volume by ingestion date

## Dashboard

The Lakeview dashboard definition is:

- `dashboards/lab07_dq_scorecard.lvdash.json`

It reports overall quality, daily trends, and issue-focused quality dimension
breakdowns.

## Useful Local Commands

Run local tests:

```bash
PYTHONPATH=labs/lab_07_data_quality_testing/src python -m pytest \
  labs/lab_07_data_quality_testing/tests -m "not remote" -v
```

Run the focused snapshot and merge-policy tests:

```bash
PYTHONPATH=labs/lab_07_data_quality_testing/src python -m pytest \
  labs/lab_07_data_quality_testing/tests/unit/test_snapshot_policy.py \
  labs/lab_07_data_quality_testing/tests/unit/test_merge_policy.py -v
```

Run Ruff on ordinary Python files:

```bash
python -m ruff check \
  labs/lab_07_data_quality_testing/src/lab07 \
  labs/lab_07_data_quality_testing/tests \
  labs/lab_07_data_quality_testing/tools
```

Do not lint raw Databricks notebooks as ordinary Python. The `pipeline/*.py`
files use the Databricks-provided `spark` global, so local Ruff checks for that
directory require ignoring `F821`.

Run Black checks:

```bash
python -m black --check \
  labs/lab_07_data_quality_testing/src/lab07 \
  labs/lab_07_data_quality_testing/tests \
  labs/lab_07_data_quality_testing/tools
```
