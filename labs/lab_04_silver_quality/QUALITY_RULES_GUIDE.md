# Lab 04 reusable quality rules

This package contains the first reusable Lab 4 source-code component:

```text
lab_04_silver_quality/
└── src/
    ├── __init__.py
    └── quality_rules.py
```

## Why this file exists

`lab04_03_silver_quality.ipynb` demonstrates the rules interactively. The
module in `src/quality_rules.py` stores the same logic in one place so that a
notebook, a Databricks Job, and tests do not each implement a different version
of the rules.

The helper only transforms DataFrames. It does **not** create, overwrite, or
delete tables. The notebook remains responsible for writing valid data to the
Silver candidate path and rejected data to the quarantine table.

## Where to put the files

Copy the included `src` folder into:

```text
Databricks-Academy-Lakehouse/
└── labs/
    └── lab_04_silver_quality/
        └── src/
```

If `src/__init__.py` already exists, replace it with the included version.

## How to import it in Databricks

For notebooks stored in the same Git folder, add the Lab 4 folder to Python's
module search path once, then import the helpers:

```python
import sys

lab04_root = (
    "/Workspace/Users/parvinbadalov@yahoo.com/"
    "Databricks-Academy-Lakehouse/labs/lab_04_silver_quality"
)

if lab04_root not in sys.path:
    sys.path.append(lab04_root)

from src.quality_rules import (
    apply_online_retail_quality_rules,
    build_quality_metrics,
    build_rule_failure_summary,
    split_valid_and_quarantine,
)
```

Change only the email/path if your Git folder is stored elsewhere.

## How to use it in `lab04_03_silver_quality.ipynb`

After reading the selected Bronze batch into `batch_bronze_df`, run:

```python
quality_df = apply_online_retail_quality_rules(
    batch_bronze_df,
    contract_version=contract_version,
)

valid_quality_df, rejected_quality_df = split_valid_and_quarantine(quality_df)

display(build_rule_failure_summary(quality_df))
display(build_quality_metrics(quality_df))
```

The resulting metadata has a clear purpose:

| Column | Meaning |
|---|---|
| `_quality_reasons` | Every failed rule, not just the first one |
| `_quality_rule_count` | Number of failures on the row |
| `_quality_status` | `VALID` or `REJECTED` |
| `_quality_contract_version` | Contract version used for evaluation |
| `_quality_checked_at` | When the check ran |
| `_duplicate_rank` | First copy is 1; later exact copies are rejected |

## Expected behavior

- Valid records have an empty `_quality_reasons` array.
- Invalid records retain all rejection reasons for auditability.
- Exact duplicates after the first copy receive `DUPLICATE_BUSINESS_ROW`.
- Cancellations and returns are quarantined rather than silently discarded.
- Re-running the transformation produces the same classifications for the same
  source data; persistent idempotency is still enforced by the notebook's
  Delta `MERGE` keys.

## What comes next

The next reusable component is `src/merge_utils.py`. It will centralize the
idempotent Silver upsert and SCD helpers used by notebooks 04, 05, and 06.
