# Lab 06 External V2 — Serverless Fix Pack

Replace these files in:

`labs/lab_06_gold_analytics_external/`

## Files

- `src/external_tables.py`
- `notebooks/lab06_01_dimensions.ipynb`
- `notebooks/lab06_02_fact_encounters.ipynb`
- `notebooks/lab06_03_fact_conditions.ipynb`
- `notebooks/lab06_04_aggregations.ipynb`
- `notebooks/lab06_05_register_shared_tables.ipynb`
- `notebooks/lab06_06_alert_metrics.ipynb`
- `notebooks/lab06_07_validation.ipynb`

All writes use explicit external Delta locations and Unity Catalog registration.

There are no `REFRESH TABLE`, `CACHE TABLE`, or `UNCACHE TABLE` commands.

Recommended test order:

1. `lab06_00_source_preparation` (already passed)
2. dev runner → `01_dimensions`
3. dev runner → `02_fact_encounters`
4. dev runner → `03_fact_conditions`
5. dev runner → `04_aggregations`
6. dev runner → `07_validation`
7. then run `05_register_shared_tables` and `06_alert_metrics` manually as needed

After individual tasks pass, run `FULL_EXTERNAL_BUILD`.
