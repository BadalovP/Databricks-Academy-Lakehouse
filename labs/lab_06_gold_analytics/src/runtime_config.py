"""Central runtime parameter loader for Lab 06.

Environment-specific values are owned by the Databricks Job (or another caller)
and passed into notebooks as parameters. Processing notebooks do not create
widgets themselves.

This keeps:
- Job YAML = source of runtime/environment values
- runtime_config.py = parameter reader / parser
- config.py = path and fully-qualified table-name builder
- notebooks = transformation logic
"""

from dataclasses import dataclass
from typing import Optional

from .config import Lab06Config


@dataclass(frozen=True)
class Lab06RuntimeContext:
    catalog: str
    schema: str
    volume_name: str
    config: Lab06Config

    run_validation: Optional[bool] = None

    date_start: Optional[str] = None
    date_end: Optional[str] = None
    rebuild_dim_date: Optional[bool] = None


def _required_widget(dbutils, name: str) -> str:
    """Read a required Databricks notebook/job parameter."""
    try:
        value = dbutils.widgets.get(name)
    except Exception as exc:
        raise RuntimeError(
            f"Missing required runtime parameter '{name}'. "
            "Run this notebook through the Lab 06 Job, or pass the parameter "
            "from a manual Databricks task."
        ) from exc

    if value is None or not str(value).strip():
        raise ValueError(
            f"Runtime parameter '{name}' must not be empty."
        )

    return str(value).strip()


def _to_bool(value: str, name: str) -> bool:
    normalized = str(value).strip().lower()

    if normalized not in {"true", "false"}:
        raise ValueError(
            f"Runtime parameter '{name}' must be 'true' or 'false', "
            f"got: {value!r}"
        )

    return normalized == "true"


def load_runtime_context(
    dbutils,
    *,
    include_validation: bool = False,
    include_date_config: bool = False,
) -> Lab06RuntimeContext:
    """Load the common Lab 06 runtime context.

    The function never creates widgets. Parameters must already be supplied by
    the Databricks Job/task or another explicit caller.
    """
    catalog = _required_widget(dbutils, "catalog")
    schema = _required_widget(dbutils, "schema")
    volume_name = _required_widget(dbutils, "volume_name")

    config = Lab06Config(
        catalog=catalog,
        schema=schema,
        volume_name=volume_name,
    )

    run_validation = None
    if include_validation:
        run_validation = _to_bool(
            _required_widget(dbutils, "run_validation"),
            "run_validation",
        )

    date_start = None
    date_end = None
    rebuild_dim_date = None

    if include_date_config:
        date_start = _required_widget(dbutils, "date_start")
        date_end = _required_widget(dbutils, "date_end")
        rebuild_dim_date = _to_bool(
            _required_widget(dbutils, "rebuild_dim_date"),
            "rebuild_dim_date",
        )

    return Lab06RuntimeContext(
        catalog=catalog,
        schema=schema,
        volume_name=volume_name,
        config=config,
        run_validation=run_validation,
        date_start=date_start,
        date_end=date_end,
        rebuild_dim_date=rebuild_dim_date,
    )
