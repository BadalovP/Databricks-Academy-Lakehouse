"""The RAG Job steps moved out of notebooks 20-23: corpus, index plan, readiness gate, summaries.

Spark, AI Search and model endpoints are replaced by small fakes; these tests prove the control flow
and the guards, not Databricks behaviour.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from urbanflow.rag import pipeline
from urbanflow.rag.cli import build_offline_service
from urbanflow.rag.config import load_config
from urbanflow.rag.evaluation import load_cases, run_evaluation


@pytest.fixture()
def cfg(project_root):
    return load_config(project_root / "config" / "rag.yml")


@pytest.fixture()
def cases(project_root):
    return load_cases(project_root / "config" / "rag_evaluation_questions.yml")


class _Spark:
    def __init__(self, rows: int) -> None:
        self.rows = rows

    def table(self, name: str):
        return SimpleNamespace(count=lambda: self.rows)


class _Manager:
    def __init__(self, index_status: str) -> None:
        self.index_status = index_status
        self.calls: list[str] = []

    def inspect(self, spec):
        self.calls.append("inspect")
        return {"endpoint_status": "ONLINE", "index_status": self.index_status}

    def verify(self, spec, *, expected_rows, timeout_seconds, poll_seconds):
        self.calls.append("verify")
        return {"indexed_rows": expected_rows}

    def provision(self, spec, **kwargs):
        self.calls.append("provision")
        return {"index_status": "ready", **kwargs}


def test_the_corpus_is_the_eight_mobility_procedures(cfg, project_root) -> None:
    documents, chunks = pipeline.load_corpus(cfg, project_root)
    assert len(documents) == 8
    assert len(chunks) >= len(documents)
    assert len({chunk.chunk_id for chunk in chunks}) == len(chunks)


def test_an_empty_corpus_is_refused(cfg, tmp_path) -> None:
    (tmp_path / "knowledge_base").mkdir()
    with pytest.raises(Exception):  # noqa: B017 - loader or guard, either must stop the task
        pipeline.load_corpus(cfg, tmp_path)


def test_the_index_plan_always_uses_this_targets_schema(cfg) -> None:
    spec = pipeline.index_spec(
        cfg,
        "dbr_dev",
        "parvinbadalov_urbanflow_final_prod",
        endpoint="urbanflow-final-search",
        embedding_endpoint="databricks-gte-large-en",
        index="",
    )
    assert (
        spec.source_table_name == "dbr_dev.parvinbadalov_urbanflow_final_prod.rag_document_chunks"
    )
    assert spec.index_name.startswith("dbr_dev.parvinbadalov_urbanflow_final_prod.")
    assert spec.primary_key == "chunk_id"


@pytest.mark.parametrize("endpoint, embedding", [("", "e"), ("s", " ")])
def test_the_index_plan_requires_both_endpoints(cfg, endpoint, embedding) -> None:
    with pytest.raises(ValueError, match="required"):
        pipeline.index_spec(
            cfg, "c", "s", endpoint=endpoint, embedding_endpoint=embedding, index="i"
        )


def _spec(cfg):
    return pipeline.index_spec(
        cfg, "workspace", "urbanflow_final_dev", endpoint="e", embedding_endpoint="m", index=""
    )


def test_inspection_without_an_index_is_a_dry_run_and_never_verifies(cfg) -> None:
    manager = _Manager("absent")
    result = pipeline.inspect_index(_Spark(10), manager, _spec(cfg))
    assert result["status"] == "DRY_RUN"
    assert manager.calls == ["inspect"]


def test_an_existing_index_must_pass_the_readiness_gate(cfg) -> None:
    manager = _Manager("present")
    result = pipeline.inspect_index(_Spark(42), manager, _spec(cfg))
    assert result["status"] == "READY_READ_ONLY"
    assert result["source_rows"] == 42
    assert manager.calls == ["inspect", "verify"]


def test_provisioning_refuses_an_empty_source_table(cfg) -> None:
    manager = _Manager("absent")
    with pytest.raises(RuntimeError, match="empty"):
        pipeline.provision_index(_Spark(0), cfg, "c", "s", manager, _spec(cfg))
    assert manager.calls == []


def test_provisioning_waits_are_bounded_below_the_task_timeout(cfg, monkeypatch) -> None:
    monkeypatch.setattr(pipeline, "table_versions", lambda *args: {"chunks": 3})
    manager = _Manager("absent")
    result = pipeline.provision_index(_Spark(5), cfg, "c", "s", manager, _spec(cfg))
    assert result["status"] == "READY"
    assert result["endpoint_timeout_seconds"] + result["index_timeout_seconds"] < 3600
    assert result["table_versions_after"] == {"chunks": 3}


def test_offline_summary_is_labelled_offline_and_scores_every_question(cases) -> None:
    summary = pipeline.offline_summary(cases, build_offline_service(), include_baseline=True)
    assert summary["mode"] == "offline"
    assert summary["question_ids"] == [case.question_id for case in cases]
    assert summary["rag"]["questions"] == summary["baseline"]["questions"] == len(cases)
    no_baseline = pipeline.offline_summary(cases, build_offline_service(), include_baseline=False)
    assert no_baseline["baseline"] == "not_run"


def test_live_summary_and_review_follow_the_run_without_a_baseline(cases) -> None:
    subset = cases[:3]
    run = run_evaluation(subset, build_offline_service().answer, None, "fake-live")
    summary = pipeline.live_summary(run, subset, False, workspace="w", index="i")
    assert summary["mode"] == "live" and summary["baseline"] == "not_run"
    assert summary["generation"]["baseline"] == "not_run"
    records = pipeline.review_records(run, subset, False)
    assert [record["question_id"] for record in records] == ["q01", "q02", "q03"]
    assert all(record["baseline"] == "not_run" for record in records)
    assert records[0]["retrieved"][0]["rank"] == 1


def test_ingest_stops_when_the_table_does_not_match_the_corpus(cfg, project_root, monkeypatch):
    documents, chunks = pipeline.load_corpus(cfg, project_root)
    monkeypatch.setattr(pipeline, "create_lab_tables", lambda *a, **k: None)
    monkeypatch.setattr(pipeline, "merge_documents", lambda *a, **k: {})
    monkeypatch.setattr(pipeline, "merge_chunks", lambda *a, **k: {})
    monkeypatch.setattr(pipeline, "change_data_feed_enabled", lambda *a: True)
    monkeypatch.setattr(
        pipeline,
        "table_snapshot",
        lambda spark, table, key: {
            "row_count": 1,
            "distinct_keys": 1,
            "fingerprint": "stale",
            "null_ingested_at": 0,
        },
    )
    with pytest.raises(RuntimeError, match="Reconciliation failed"):
        pipeline.ingest_and_reconcile(None, cfg, "c", "s", documents, chunks)
