"""The Lab 10 static validator accepts the real repository and rejects each unsafe change.

Standard library only (``python3 -m unittest``), so CI needs no extra packages. Each negative test
starts from the real file and applies one change that must be refused.
"""

from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path

LAB = Path(__file__).resolve().parents[1]
REPO = LAB.parents[1]
_spec = importlib.util.spec_from_file_location("validator", LAB / "scripts" / "validate_lab10_static.py")
validator = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(validator)

WORKFLOWS = REPO / ".github" / "workflows"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _notebooks() -> dict[str, str]:
    return {
        path.name: validator.notebook_source(path.name, _read(path))
        for path in sorted(LAB.glob("*.ipynb"))
    }


def _workflows() -> tuple[str, str, list[str]]:
    cd = _read(WORKFLOWS / "lab10_azure_prod_cd.yml")
    run = _read(WORKFLOWS / "lab10_azure_prod_run.yml")
    return cd, run, [_read(p) for p in sorted(WORKFLOWS.glob("lab10_*.yml"))]


def _terraform() -> tuple[str, str]:
    root = LAB / "terraform_azure_prod"
    return _read(root / "versions.tf"), _read(root / "main.tf")


class RealRepositoryPasses(unittest.TestCase):
    def test_every_check_passes(self) -> None:
        validator.check_notebooks(_notebooks())
        validator.check_terraform(*_terraform())
        validator.check_workflows(*_workflows())
        self.assertEqual(validator.find_secret_material(validator._repository_texts()), [])


class CdcContract(unittest.TestCase):
    def _reject(self, notebook: str, old: str, new: str) -> None:
        sources = _notebooks()
        self.assertIn(old, sources[notebook], f"fixture text not found: {old!r}")
        sources[notebook] = sources[notebook].replace(old, new)
        with self.assertRaises(AssertionError):
            validator.check_notebooks(sources)

    def test_checkpoint_before_reconciliation_is_rejected(self) -> None:
        sources = _notebooks()
        cdc = sources["02_incremental_cdc_sync.ipynb"]
        checkpoint = cdc.index("MERGE INTO {CHECKPOINT}")
        # Move the checkpoint MERGE to the top: it would advance before reconciliation.
        sources["02_incremental_cdc_sync.ipynb"] = cdc[checkpoint:] + "\n" + cdc[:checkpoint]
        with self.assertRaises(AssertionError):
            validator.check_notebooks(sources)

    def test_missing_delete_handling_is_rejected(self) -> None:
        self._reject("02_incremental_cdc_sync.ipynb", "WHEN MATCHED AND s._change_type = 'delete'", "")

    def test_missing_reverse_reconciliation_is_rejected(self) -> None:
        self._reject("02_incremental_cdc_sync.ipynb", "assert reverse_differences == 0", "")

    def test_destructive_setup_is_rejected(self) -> None:
        self._reject("00_setup.ipynb", "CREATE SCHEMA IF NOT EXISTS", "DROP TABLE x; CREATE SCHEMA IF NOT EXISTS")

    def test_setup_without_change_data_feed_is_rejected(self) -> None:
        self._reject("00_setup.ipynb", "delta.enableChangeDataFeed = true", "delta.enableChangeDataFeed = false")

    def test_final_validation_without_time_travel_is_rejected(self) -> None:
        self._reject("03_final_validation.ipynb", "VERSION AS OF {checkpoint}", "")

    def test_notebook_with_outputs_is_rejected(self) -> None:
        path = LAB / "01_federation_validation.ipynb"
        notebook = json.loads(_read(path))
        code_cell = next(cell for cell in notebook["cells"] if cell["cell_type"] == "code")
        code_cell["outputs"] = [{"output_type": "stream", "name": "stdout", "text": ["x"]}]
        with self.assertRaises(AssertionError):
            validator.notebook_source(path.name, json.dumps(notebook))

    def test_notebook_with_a_syntax_error_is_rejected(self) -> None:
        notebook = {"nbformat": 4, "cells": [{"cell_type": "code", "source": ["def broken(:"], "outputs": []}]}
        with self.assertRaises(SyntaxError):
            validator.notebook_source("broken.ipynb", json.dumps(notebook))


class TerraformContract(unittest.TestCase):
    def _reject_main(self, old: str, new: str) -> None:
        versions, main_tf = _terraform()
        self.assertIn(old, main_tf, f"fixture text not found: {old!r}")
        with self.assertRaises(AssertionError):
            validator.check_terraform(versions, main_tf.replace(old, new, 1))

    def test_wrong_workspace_is_rejected(self) -> None:
        versions, main_tf = _terraform()
        wrong = versions.replace(validator.EXPECTED_HOST, validator.FORBIDDEN_HOSTS[0])
        with self.assertRaises(AssertionError):
            validator.check_terraform(wrong, main_tf)

    def test_cluster_is_rejected(self) -> None:
        self._reject_main("  task {", '  task {\n    existing_cluster_id = "0000-000000-abcdefgh"')

    def test_serverless_auto_retry_left_on_is_rejected(self) -> None:
        self._reject_main("    disable_auto_optimization = true\n", "")

    def test_schedule_is_rejected(self) -> None:
        self._reject_main("  task {", "  schedule {\n    quartz_cron_expression = \"0 0 * * * ?\"\n  }\n  task {")


class WorkflowContract(unittest.TestCase):
    def _reject(self, workflow: str, old: str, new: str) -> None:
        cd, run, others = _workflows()
        target = cd if workflow == "cd" else run
        self.assertIn(old, target, f"fixture text not found: {old!r}")
        changed = target.replace(old, new)
        if workflow == "cd":
            cd = changed
        else:
            run = changed
        with self.assertRaises(AssertionError):
            validator.check_workflows(cd, run, others + [changed])

    def test_push_trigger_is_rejected(self) -> None:
        self._reject("cd", "\non:\n", "\non:\n  push:\n    branches: [main]\n")

    def test_azure_login_is_rejected(self) -> None:
        self._reject("run", "      - name: Install Databricks CLI", "      - uses: azure/login@abc\n      - name: Install Databricks CLI")

    def test_pat_auth_is_rejected(self) -> None:
        self._reject("cd", "DATABRICKS_AUTH_TYPE: github-oidc", "DATABRICKS_TOKEN: ${{ secrets.X }}")

    def test_other_environment_is_rejected(self) -> None:
        self._reject("cd", "environment: lab10-azure", "environment: azure-prod")

    def test_missing_forbidden_host_guard_is_rejected(self) -> None:
        self._reject("run", "FORBIDDEN_HOST_DEV:", "UNUSED_HOST_DEV:")

    def test_principal_check_without_user_name_is_rejected(self) -> None:
        self._reject("cd", " // .userName", "")

    def test_mutable_action_tag_is_rejected(self) -> None:
        cd, run, others = _workflows()
        with self.assertRaises(AssertionError):
            validator.check_workflows(cd, run, others + ["      - uses: actions/checkout@v5\n"])

    def test_unsaved_plan_apply_is_rejected(self) -> None:
        self._reject("cd", "terraform apply -input=false -no-color lab10-azure-prod.tfplan", "terraform apply -input=false")


class SecretScan(unittest.TestCase):
    def test_tokens_and_keys_are_found(self) -> None:
        findings = validator.find_secret_material({
            "a.tf": "token = dapi" + "0123456789abcdef0123456789ab",
            # Built from pieces so this test file does not itself contain a key header.
            "b.yml": "-----BEGIN " + "PRIVATE KEY-----",
            "c.md": "ghp_" + "A" * 36,
        })
        self.assertEqual(len(findings), 3)

    def test_clean_text_has_no_findings(self) -> None:
        self.assertEqual(validator.find_secret_material({"a.md": "DATABRICKS_AUTH_TYPE=github-oidc"}), [])


if __name__ == "__main__":
    unittest.main()
