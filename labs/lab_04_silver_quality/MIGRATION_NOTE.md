# Migration note

Replace:
- `schemas/source_schema.json`
- `schemas/silver_schema.json`

Remove the old generic:
- `schemas/product_dimension_schema.json`

Add:
- `schemas/bronze_schema.json`
- `schemas/product_scd1_schema.json`
- `schemas/product_scd2_schema.json`

Then update `lab04_00_config` to load all five JSON schemas before wiring strict schema validation into notebooks.
