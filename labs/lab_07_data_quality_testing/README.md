# Lab 07 — Data Quality Testing

Comprehensive data quality testing for Databricks lakehouse applications.

## Architecture

Pipeline: API/Landing → Bronze → Quality → SCD2 → Gold → Monitoring

## Testing

**Lakeflow Tests**: `lakeflow_tests/` - End-to-end pipeline tests
**Chispa Tests**: `tests/unit/test_chispa_dataframes.py` - DataFrame unit tests

## Quality Classification

- VALID: Passes all checks
- WARN: Warnings only (e.g., missing DBA name)
- QUARANTINE: Critical failures

## Key Tables

- `business_license_validated` - Trusted data
- `business_license_quarantine` - Failed records
- `dim_license_scd2` - Historical tracking
- `license_quality_daily` - Metrics
- `lab07_quality_scorecard` - Overall quality

## Running

1. Run pipeline
2. Execute notebooks 00-11 in order
3. Run `lab07_10_quality_scorecard`
4. View `dashboards/lab07_dq_scorecard`

See notebook comments for details.
