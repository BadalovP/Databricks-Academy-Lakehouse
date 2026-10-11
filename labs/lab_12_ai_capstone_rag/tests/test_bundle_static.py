"""The static bundle validator accepts the real bundle and rejects each unsafe change."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("validator", ROOT / "scripts" / "validate_bundle_static.py")
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)


def _load():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    resources = yaml.safe_load((ROOT / "resources" / "lab12_job.job.yml").read_text(encoding="utf-8"))
    return bundle, resources


def test_real_bundle_passes() -> None:
    assert validator.check(*_load()) == []


def _parameter(resources, name):
    job = resources["resources"]["jobs"]["lab12_rag_workflow"]
    return next(item for item in job["parameters"] if item["name"] == name)


def _mutate(edit):
    bundle, resources = _load()
    bundle, resources = copy.deepcopy(bundle), copy.deepcopy(resources)
    edit(bundle, resources)
    return validator.check(bundle, resources)


@pytest.mark.parametrize(
    "description, edit",
    [
        ("default target", lambda b, r: b["targets"]["azure_prod"].update(default=True)),
        ("wrong host", lambda b, r: b["targets"]["personal_dev"]["workspace"].update(
            host="https://adb-7405618361895520.0.azuredatabricks.net")),
        ("shared schema", lambda b, r: b["targets"]["azure_prod"]["variables"].update(
            schema="parvinbadalov_lab12_rag_dev")),
        ("wheel artifact", lambda b, r: b.update(artifacts={"default": {"type": "whl"}})),
        ("broad sync include", lambda b, r: b["sync"]["include"].append("config/**")),
        ("venv not excluded", lambda b, r: b["sync"].update(exclude=[])),
        ("task libraries", lambda b, r: r["resources"]["jobs"]["lab12_rag_workflow"]["tasks"][0].update(
            libraries=[{"whl": "../dist/*.whl"}])),
        ("cluster", lambda b, r: r["resources"]["jobs"]["lab12_rag_workflow"]["tasks"][0].update(
            existing_cluster_id="0702-132442-toro5spu")),
        ("serverless auto-retry", lambda b, r: r["resources"]["jobs"]["lab12_rag_workflow"]["tasks"][1].pop(
            "disable_auto_optimization")),
        ("explicit retries", lambda b, r: r["resources"]["jobs"]["lab12_rag_workflow"]["tasks"][1].update(
            max_retries=2)),
        ("schedule",lambda b, r: r["resources"]["jobs"]["lab12_rag_workflow"].update(
            schedule={"quartz_cron_expression": "0 0 * * * ?"})),
        ("live flag on", lambda b, r: r["resources"]["jobs"]["lab12_rag_workflow"]["parameters"][-1].update(
            default="true")),
        ("shadowed parameter", lambda b, r: r["resources"]["jobs"]["lab12_rag_workflow"]["tasks"][1][
            "notebook_task"].update(base_parameters={"catalog": "other"})),
        ("targeted IDs by default", lambda b, r: _parameter(r, "eval_question_ids").update(
            default="q01,q09,q12,q13")),
        ("baseline off by default", lambda b, r: _parameter(r, "eval_include_baseline").update(
            default="false")),
        ("question limit lowered", lambda b, r: _parameter(r, "eval_question_limit").update(
            default="4")),
    ],
)
def test_unsafe_changes_are_rejected(description, edit) -> None:
    assert _mutate(edit), f"validator accepted: {description}"


def test_corpus_is_not_ignored_by_git() -> None:
    import subprocess

    docs = sorted((ROOT / "data" / "sample_docs").glob("*.md"))
    assert len(docs) == 6
    result = subprocess.run(
        ["git", "check-ignore", *[str(path) for path in docs]], capture_output=True, text=True, cwd=ROOT
    )
    if result.returncode == 128:
        pytest.skip("not inside a git checkout")
    assert result.stdout.strip() == "", f"corpus files are git-ignored: {result.stdout}"
