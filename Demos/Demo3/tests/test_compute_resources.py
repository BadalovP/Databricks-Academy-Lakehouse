from __future__ import annotations

from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_notebook_job_uses_existing_cluster_and_defaults_to_dry_run() -> None:
    resource = yaml.safe_load((PROJECT_ROOT / "resources/jobs.yml").read_text(encoding="utf-8"))
    job = resource["resources"]["jobs"]["urbanflow_bounded_stream_test"]
    task = job["tasks"][0]

    assert task["existing_cluster_id"] == "${var.compute_cluster_id}"
    assert "new_cluster" not in task
    assert job["parameters"][0] == {"name": "run_stream", "default": "false"}
    assert task["notebook_task"]["notebook_path"].endswith("03_eventhubs_to_bronze.py")
    assert "schedule" not in job


def test_lakeflow_uses_managed_serverless_compute_without_gp_cluster() -> None:
    path = PROJECT_ROOT / "resources/pipelines.yml"
    text = path.read_text(encoding="utf-8")
    pipeline = yaml.safe_load(text)["resources"]["pipelines"]["urbanflow_pipeline"]

    assert pipeline["serverless"] is True
    assert pipeline["continuous"] is False
    assert "existing_cluster_id" not in text
    assert "0702-132442-toro5spu" not in text
    assert "0702-171207-xo9bbc0y" not in text
    assert (
        not (PROJECT_ROOT / "pipeline/bronze.py")
        .read_text(encoding="utf-8")
        .startswith("# Databricks notebook source")
    )


def test_storage_bootstrap_is_isolated_and_non_destructive() -> None:
    text = (PROJECT_ROOT / "sql/00_prepare_urbanflow_storage.sql").read_text(encoding="utf-8")

    assert "dbr_dev.parvinbadalov_urbanflow" in text
    assert "urbanflow_landing" in text
    assert "IF NOT EXISTS" in text
    assert "DROP " not in text.upper()
    assert "REPLACE " not in text.upper()
