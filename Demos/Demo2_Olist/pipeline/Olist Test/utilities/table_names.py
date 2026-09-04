# Purpose: provide reusable table-name helper functions.


def qualified_table_name(
    catalog: str,
    schema: str,
    table: str
) -> str:
    """Return a three-level Unity Catalog table name."""

    values = {
        "catalog": catalog,
        "schema": schema,
        "table": table,
    }

    for value_name, value in values.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"{value_name} must be a non-empty string"
            )

    return f"{catalog}.{schema}.{table}"