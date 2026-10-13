"""The temporary AI Search cleanup guard: it deletes in time and never disarms too early."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "ai_search_cleanup_guard",
    Path(__file__).resolve().parents[1] / "scripts" / "ai_search_cleanup_guard.py",
)
guard = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = guard  # dataclasses resolve their module by name
_SPEC.loader.exec_module(guard)

MINUTE = 60_000
CREATED = 1_000_000_000_000
EXPIRES = CREATED + 240 * MINUTE


@pytest.mark.parametrize(
    "age, action",
    [(0, "none"), (60, "none"), (149, "none"), (150, "delete"), (200, "delete")],
)
def test_the_endpoint_is_deleted_once_it_reaches_150_minutes(age, action) -> None:
    decision = guard.decide(
        endpoint_created_ms=CREATED, now_ms=CREATED + age * MINUTE, expires_at_ms=EXPIRES
    )
    assert decision.action == action


def test_a_30_minute_schedule_deletes_within_three_hours_of_creation() -> None:
    """Whatever the phase of the schedule, the first delete happens before the endpoint is 180 min old."""
    for offset in range(0, 30):
        checks = [CREATED + (offset + 30 * n) * MINUTE for n in range(12)]
        first = next(
            t
            for t in checks
            if guard.decide(endpoint_created_ms=CREATED, now_ms=t, expires_at_ms=EXPIRES).action
            == "delete"
        )
        assert first - CREATED <= 180 * MINUTE


def test_the_guard_never_pauses_before_the_endpoint_could_exist() -> None:
    before_expiry = guard.decide(
        endpoint_created_ms=None, now_ms=EXPIRES - 1, expires_at_ms=EXPIRES
    )
    assert before_expiry.action == "none"
    after_expiry = guard.decide(endpoint_created_ms=None, now_ms=EXPIRES, expires_at_ms=EXPIRES)
    assert after_expiry.action == "pause"


def test_an_endpoint_still_present_at_expiry_is_deleted_even_if_young() -> None:
    decision = guard.decide(
        endpoint_created_ms=EXPIRES - 10 * MINUTE, now_ms=EXPIRES, expires_at_ms=EXPIRES
    )
    assert decision.action == "delete"


def test_the_guard_can_touch_only_the_final_project_endpoint_and_index() -> None:
    assert guard.ENDPOINT == "urbanflow-final-dev"
    assert guard.INDEX == "workspace.urbanflow_final_dev.rag_document_chunks_index"
    source = Path(guard.__file__).read_text(encoding="utf-8")
    assert "lab12" not in source.lower()
    assert source.count("delete_endpoint(") == 1 and "delete_endpoint(ENDPOINT)" in source
    assert source.count("delete_index(") == 1 and "delete_index(INDEX)" in source


def test_only_not_found_errors_count_as_missing() -> None:
    assert guard._missing(RuntimeError("RESOURCE_DOES_NOT_EXIST: endpoint does not exist"))
    assert guard._missing(RuntimeError("404 Not Found"))
    assert not guard._missing(PermissionError("PERMISSION_DENIED"))
