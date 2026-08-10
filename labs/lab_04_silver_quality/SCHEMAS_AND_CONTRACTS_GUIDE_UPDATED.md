# Lab 04 schemas and data contracts

These files define the technical schemas and governance contracts used by the current Lab 4 implementation.

## Folder structure

```text
lab_04_silver_quality/
├── schemas/
│   ├── source_schema.json
│   ├── bronze_schema.json
│   ├── silver_schema.json
│   ├── product_scd1_schema.json
│   └── product_scd2_schema.json
└── contracts/
    ├── online_retail_v1.yml
    └── online_retail_v2.yml
```

## What each schema file represents

### `source_schema.json`
Physical Spark schema of the prepared eight-column Online Retail source before Bronze metadata is added.

The physical Spark representation uses:
- `Quantity` -> `long`
- `InvoiceDate` -> `timestamp`
- `UnitPrice` -> `double`

The JSON schema deliberately does not try to express business-quality policy. Null/blank rules and allowed evolution belong in the YAML contracts and reusable quality-rule code.

### `bronze_schema.json`
Persisted Bronze structure, including the original eight source fields plus technical lineage and ingestion metadata such as:
- `_source_row_number`
- `_record_hash`
- `_bronze_record_id`
- `_batch_id`
- `_contract_version`
- `_bronze_ingested_at`

### `silver_schema.json`
Current final Silver transaction structure used by `lab04_04_silver_merge`, including normalized names, business fields, lineage, quality metadata, derived date fields, and Silver audit timestamps.

### `product_scd1_schema.json`
Current-state product-dimension structure used by the SCD Type 1 notebook.

### `product_scd2_schema.json`
Historical product-dimension structure used by the SCD Type 2 notebook. It includes `product_version_sk`, `version_number`, temporal columns, and the current-row flag.

## Contracts

### `online_retail_v1.yml`
Baseline governance contract for the original Online Retail shape and quality policy.

### `online_retail_v2.yml`
Proposed/approved evolution definition used by the schema-evolution demonstration. It supersedes v1 and introduces:
- `loyalty_tier`
- `sales_channel`
- `Quantity` widening from `integer` to `long`

The YAML contract describes governance policy. The JSON schema describes the Spark/Delta technical structure. They are complementary rather than interchangeable.

## Loading a JSON schema

```python
import json
from pathlib import Path
from pyspark.sql.types import StructType

schema_json = json.loads(
    Path(schema_path).read_text(encoding="utf-8")
)
schema = StructType.fromJson(schema_json)
```

Recommended `lab04_00_config` variables:

```python
source_contract_schema
bronze_contract_schema
silver_contract_schema
product_scd1_contract_schema
product_scd2_contract_schema
```

## Recommended use by notebook

| Notebook | Schema/contract use |
|---|---|
| `01_Source_preparation` | `source_schema.json` + contract v1 |
| `02_Bronze_ingestion` | `bronze_schema.json` |
| `03_Silver_quality` | contract v1 + `src/quality_rules.py` |
| `04_Silver_MERGE` | `silver_schema.json` |
| `05_SCD_Type_1` | `product_scd1_schema.json` |
| `06_SCD_Type_2` | `product_scd2_schema.json` |
| `08_Schema_enforcement` | contract v1 baseline |
| `09_Schema_evolution` | contract v1 -> v2 |
| `12_Final_validation` | validate schemas and both contracts |

## Important

Do not add JSON schema files or YAML contract files as separate Databricks Job tasks.

`lab04_00_setup` owns permanent structural DDL. Runtime notebooks should load/validate these definitions and process data.
