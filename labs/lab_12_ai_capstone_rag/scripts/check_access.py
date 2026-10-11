"""Read-only Lab 12 capability inventory. Requires a deliberately selected CLI profile."""

from __future__ import annotations

import argparse
import json
from typing import Any

# Mirrors the bundle targets in databricks.yml.
TARGETS = {
    "personal_dev": {
        "host": "https://dbc-1750318a-76a9.cloud.databricks.com",
        "schema": "parvinbadalov_lab12_rag_dev",
        "ai_search_endpoint": "lab12-rag-dev",
    },
    "azure_prod": {
        "host": "https://adb-7405618361895520.0.azuredatabricks.net",
        "schema": "parvinbadalov_lab12_rag_prod",
        "ai_search_endpoint": "parvinbadalov-lab12-rag-prod",
    },
}


def _safe_error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {str(exc)[:300]}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=sorted(TARGETS))
    parser.add_argument(
        "--profile",
        help="Databricks CLI profile; omit to use Azure CLI auth (for azure_prod).",
    )
    args = parser.parse_args(argv)
    target = TARGETS[args.target]
    args.expected_host = target["host"]
    args.catalog, args.schema = "dbr_dev", target["schema"]
    args.ai_search_endpoint = target["ai_search_endpoint"]
    args.ai_search_index = f"dbr_dev.{target['schema']}.document_chunks_index"

    from databricks.sdk import WorkspaceClient

    if args.profile:
        client = WorkspaceClient(profile=args.profile)
    else:
        client = WorkspaceClient(host=target["host"], auth_type="azure-cli")
    actual_host = client.config.host.rstrip("/")
    expected_host = args.expected_host.rstrip("/")
    if actual_host != expected_host:
        raise SystemExit(f"Refusing read-only checks: profile host {actual_host!r} != {expected_host!r}")

    checks: dict[str, Any] = {
        "target": args.target,
        "host": actual_host,
        "profile": args.profile or "azure-cli",
        "mutations": False,
    }
    operations = {
        "identity": lambda: client.current_user.me().user_name,
        "catalog_visible": lambda: client.catalogs.get(args.catalog).name,
        "schema_visible": lambda: client.schemas.get(f"{args.catalog}.{args.schema}").full_name,
        "embedding_endpoint_visible": lambda: client.serving_endpoints.get(
            "databricks-gte-large-en"
        ).name,
        "chat_endpoint_visible": lambda: client.serving_endpoints.get(
            "databricks-gpt-oss-20b"
        ).name,
        "ai_search_endpoint_visible": lambda: client.vector_search_endpoints.get_endpoint(
            args.ai_search_endpoint
        ).name,
        "ai_search_index_visible": lambda: client.vector_search_indexes.get_index(
            args.ai_search_index
        ).name,
    }
    for name, operation in operations.items():
        try:
            checks[name] = {"status": "PASS", "value": operation()}
        except Exception as exc:  # each permission is reported independently
            checks[name] = {"status": "UNVERIFIED", "detail": _safe_error(exc)}
    print(json.dumps(checks, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

