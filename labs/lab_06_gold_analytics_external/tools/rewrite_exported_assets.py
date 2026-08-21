"""Rewrite the existing Lab 06 exported Dashboard/Genie assets for External V2.

Run from the repository root after V1 assets exist.

This does not call Databricks APIs. It copies the serialized files and replaces
the V1 Unity Catalog schema with the External V2 schema.
"""

from pathlib import Path
import argparse


def rewrite_text(src: Path, dst: Path, old_schema: str, new_schema: str, title_suffix: str) -> None:
    text = src.read_text(encoding="utf-8")
    text = text.replace(old_schema + ".", new_schema + ".")
    text = text.replace(
        "Healthcare Operations & Cost Analytics",
        "Healthcare Operations & Cost Analytics" + title_suffix,
    )
    text = text.replace(
        "Lab 06 — Healthcare Analytics Genie",
        "Lab 06 External V2 — Healthcare Analytics Genie",
    )
    text = text.replace(
        "Lab 06 - Healthcare Analytics Genie",
        "Lab 06 External V2 - Healthcare Analytics Genie",
    )
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(text, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--catalog", default="dbr_dev")
    parser.add_argument("--source-schema", default="parvinbadalov")
    parser.add_argument("--target-schema", default="parvinbadalov_lab06_ext")
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    v1 = root / "labs" / "lab_06_gold_analytics"
    v2 = root / "labs" / "lab_06_gold_analytics_external"

    old = f"{args.catalog}.{args.source_schema}"
    new = f"{args.catalog}.{args.target_schema}"

    dashboard_src = (
        v1 / "dashboards" / "healthcare_operations_cost_analytics.lvdash.json"
    )
    dashboard_dst = (
        v2 / "dashboards" / "healthcare_operations_cost_analytics_external.lvdash.json"
    )

    genie_src = (
        v1 / "genie" / "lab_06_healthcare_analytics_genie.geniespace.json"
    )
    genie_dst = (
        v2 / "genie" / "lab06_external_healthcare_analytics.geniespace.json"
    )

    if dashboard_src.exists():
        rewrite_text(
            dashboard_src, dashboard_dst, old, new, " - External V2"
        )
        print(f"Created: {dashboard_dst}")
    else:
        print(f"Dashboard source not found: {dashboard_src}")

    if genie_src.exists():
        rewrite_text(
            genie_src, genie_dst, old, new, ""
        )
        print(f"Created: {genie_dst}")
    else:
        print(f"Genie source not found: {genie_src}")

    print("")
    print("Next:")
    print("1. Inspect the generated assets.")
    print("2. Rename the matching resources/*.yml.template files to *.yml.")
    print("3. Run databricks bundle validate before deployment.")


if __name__ == "__main__":
    main()
