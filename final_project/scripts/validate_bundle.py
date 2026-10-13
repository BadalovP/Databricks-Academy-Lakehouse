"""Validate databricks.yml offline against the schema shipped by the Databricks CLI."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import yaml
from jsonschema import validators

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _python_compatible_patterns(value: Any) -> Any:
    """Translate the two Go/ECMA Unicode classes used by the CLI schema."""
    if isinstance(value, dict):
        converted = {key: _python_compatible_patterns(item) for key, item in value.items()}
        if isinstance(converted.get("pattern"), str):
            converted["pattern"] = (
                converted["pattern"].replace(r"\p{L}", r"[^\W\d_]").replace(r"\p{N}", r"\d")
            )
        return converted
    if isinstance(value, list):
        return [_python_compatible_patterns(item) for item in value]
    return value


def load_cli_schema() -> dict[str, Any]:
    result = subprocess.run(
        ["databricks", "bundle", "schema", "--output", "json"],
        check=True,
        capture_output=True,
        encoding="utf-8",
    )
    return _python_compatible_patterns(json.loads(result.stdout))


def _merge(base: dict[str, Any], extra: dict[str, Any], origin: str) -> None:
    """Deep-merge one included file; a resource key defined twice is an error, as in the CLI."""
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _merge(base[key], value, origin)
        elif key in base:
            raise ValueError(f"{origin} redefines {key!r}")
        else:
            base[key] = value


def load_bundle() -> dict[str, Any]:
    """databricks.yml with every `include` file merged, so resource files are validated too."""
    bundle = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text(encoding="utf-8"))
    for pattern in bundle.get("include", []):
        paths = sorted(PROJECT_ROOT.glob(pattern))
        if not paths:
            raise ValueError(f"include pattern {pattern!r} matches no file")
        for path in paths:
            included = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            _merge(bundle, included, path.name)
    return bundle


def main() -> int:
    bundle = load_bundle()
    schema = load_cli_schema()
    validator_class = validators.validator_for(schema)
    errors = sorted(
        validator_class(schema).iter_errors(bundle),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        for error in errors:
            location = ".".join(str(part) for part in error.absolute_path) or "<root>"
            print(f"{location}: {error.message}")
        return 1

    print("databricks.yml and its resource files match the Databricks CLI bundle schema (offline).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
