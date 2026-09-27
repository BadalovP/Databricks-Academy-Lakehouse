"""Static (AST-based) checks on pipeline/*.py's Lakeflow decorators.

pipeline/bronze.py, silver.py, and gold.py reference `spark` at call time
and are not importable outside a live Spark/Lakeflow session (see
silver.py's own module docstring) -- these tests therefore never import
them. Instead they parse the source text with Python's `ast` module and
inspect the `@dp.table(...)` / `@dp.materialized_view(...)` decorator
calls directly, proving the required `table_properties` declaration exists
(or, for gold, deliberately does not) without needing Spark at all.

Context: live run against personal-yahoo (2026-09-25), pipeline
lab09_taxi_pipeline_v2, update d6290227-9422-4e86-90f9-2eed6463fb62 --
creating lab09_taxi_quarantine failed with
[DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT]: table feature "timestampNtz"
requires manual enablement. See pipeline/silver.py's comment and
README.md "Known limitations" for the full detail.
"""

from __future__ import annotations

import ast
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parents[1] / "pipeline"

TIMESTAMP_NTZ_PROPERTY = {"delta.feature.timestampNtz": "supported"}


def _parse(filename: str) -> ast.Module:
    source = (PIPELINE_DIR / filename).read_text(encoding="utf-8")
    return ast.parse(source, filename=filename)


def _find_function(module: ast.Module, name: str) -> ast.FunctionDef:
    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"Function {name!r} not found")


def _decorator_call(func_def: ast.FunctionDef, decorator_attr: str) -> ast.Call:
    """Find the `@dp.table(...)` / `@dp.materialized_view(...)` Call node
    decorating this function, identified by the attribute name actually
    called (e.g. "table" or "materialized_view"), independent of whatever
    the `pyspark.pipelines` import alias happens to be.
    """
    for dec in func_def.decorator_list:
        if (
            isinstance(dec, ast.Call)
            and isinstance(dec.func, ast.Attribute)
            and dec.func.attr == decorator_attr
        ):
            return dec
    raise AssertionError(f"No @...{decorator_attr}(...) decorator found on {func_def.name!r}")


def _table_properties_kwarg(call: ast.Call) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == "table_properties":
            return kw.value
    return None


def _resolve_dict_literal(module: ast.Module, node: ast.expr) -> dict:
    """Resolve a decorator keyword's value node to a plain dict, following
    one level of module-level `NAME = {...}` indirection if the decorator
    references a shared constant instead of an inline literal.
    """
    if isinstance(node, ast.Name):
        for stmt in module.body:
            if (
                isinstance(stmt, ast.Assign)
                and len(stmt.targets) == 1
                and isinstance(stmt.targets[0], ast.Name)
                and stmt.targets[0].id == node.id
            ):
                return ast.literal_eval(stmt.value)
        raise AssertionError(f"No module-level assignment found for name {node.id!r}")
    return ast.literal_eval(node)


def test_bronze_table_declares_timestamp_ntz_table_property():
    module = _parse("bronze.py")
    call = _decorator_call(_find_function(module, "lab09_taxi_bronze"), "table")
    value_node = _table_properties_kwarg(call)
    assert value_node is not None, "lab09_taxi_bronze must declare table_properties"
    assert _resolve_dict_literal(module, value_node) == TIMESTAMP_NTZ_PROPERTY


def test_silver_materialized_view_declares_timestamp_ntz_table_property():
    module = _parse("silver.py")
    call = _decorator_call(_find_function(module, "lab09_taxi_silver"), "materialized_view")
    value_node = _table_properties_kwarg(call)
    assert value_node is not None, "lab09_taxi_silver must declare table_properties"
    assert _resolve_dict_literal(module, value_node) == TIMESTAMP_NTZ_PROPERTY


def test_quarantine_materialized_view_declares_timestamp_ntz_table_property():
    """The exact table whose creation failed live -- see module docstring."""
    module = _parse("silver.py")
    call = _decorator_call(_find_function(module, "lab09_taxi_quarantine"), "materialized_view")
    value_node = _table_properties_kwarg(call)
    assert value_node is not None, "lab09_taxi_quarantine must declare table_properties"
    assert _resolve_dict_literal(module, value_node) == TIMESTAMP_NTZ_PROPERTY


def test_gold_materialized_view_does_not_declare_timestamp_ntz_table_property():
    """gold's actual output schema never retains a TIMESTAMP_NTZ column --
    tpep_pickup_datetime is only used to derive pickup_date (a DATE column)
    via F.to_date(), which is not itself a timestamp -- so it must not
    carry this property.
    """
    module = _parse("gold.py")
    call = _decorator_call(_find_function(module, "lab09_taxi_daily_summary"), "materialized_view")
    assert _table_properties_kwarg(call) is None


def test_pipeline_table_and_view_names_are_unchanged():
    """The fix must be purely declarative -- output table names must not
    have moved as a side effect of adding table_properties.
    """

    def _name_kwarg(call: ast.Call) -> str:
        for kw in call.keywords:
            if kw.arg == "name":
                return ast.literal_eval(kw.value)
        raise AssertionError("no name= kwarg found")

    bronze_module = _parse("bronze.py")
    bronze_call = _decorator_call(_find_function(bronze_module, "lab09_taxi_bronze"), "table")
    assert _name_kwarg(bronze_call) == "lab09_taxi_bronze"

    silver_module = _parse("silver.py")
    silver_call = _decorator_call(
        _find_function(silver_module, "lab09_taxi_silver"), "materialized_view"
    )
    assert _name_kwarg(silver_call) == "lab09_taxi_silver"
    quarantine_call = _decorator_call(
        _find_function(silver_module, "lab09_taxi_quarantine"), "materialized_view"
    )
    assert _name_kwarg(quarantine_call) == "lab09_taxi_quarantine"

    gold_module = _parse("gold.py")
    gold_call = _decorator_call(
        _find_function(gold_module, "lab09_taxi_daily_summary"), "materialized_view"
    )
    assert _name_kwarg(gold_call) == "lab09_taxi_daily_summary"
