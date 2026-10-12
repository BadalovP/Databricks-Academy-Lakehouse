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


def main() -> int:
    bundle_path = PROJECT_ROOT / "databricks.yml"
    bundle = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
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

    print("databricks.yml matches the Databricks CLI bundle schema (offline validation).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
