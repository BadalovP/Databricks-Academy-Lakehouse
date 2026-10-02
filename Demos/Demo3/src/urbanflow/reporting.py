"""Secret-free execution evidence for the bounded UrbanFlow streaming test."""

from __future__ import annotations

import json
import re
import uuid
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

# Evidence paths are built from these tokens, so the charset is deliberately narrow: no slash,
# no whitespace, no dot-dot, nothing that could escape the reports directory or confuse a shell.
# A colon is excluded too - `replace_execution_scope` tolerates one in a business execution id,
# but it has no place in a filename and no id this project uses contains one.
_PATH_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$")
_OPAQUE_TOKEN_FRAGMENT = re.compile(r"[A-Za-z0-9]{24,}")

# Named rather than free-form, so a typo creates an error instead of a new orphan directory.
EVIDENCE_PHASES: frozenset[str] = frozenset(
    {"first_streaming_test", "silver_gold", "historical", "weather"}
)


def sanitize_path_token(value: str, *, label: str) -> str:
    """Validate one evidence-path component and reject credential-shaped opaque values.

    The narrow path charset prevents traversal and shell metacharacters. It does not, by itself,
    reject a plain alphanumeric bearer token. Long opaque runs are therefore refused too.
    UrbanFlow execution IDs are structured, hyphen-separated values and Databricks run IDs fit
    within a signed 64-bit integer, so this rule rejects credential-shaped input without blocking
    either legitimate form.
    """
    token = str(value).strip()
    if not _PATH_TOKEN.fullmatch(token):
        raise ValueError(
            f"{label} {value!r} is not safe for an evidence path: it must start with a letter or "
            "digit and contain only letters, digits, dot, underscore, plus or hyphen."
        )
    if ".." in token:
        raise ValueError(f"{label} {value!r} must not contain '..'.")
    if _OPAQUE_TOKEN_FRAGMENT.search(token):
        raise ValueError(
            f"{label} is an opaque credential-shaped value and cannot be used in an evidence path."
        )
    return token


def resolve_attempt_id(
    *,
    job_run_id: str | None = None,
    now: datetime | None = None,
    unique_token: str | None = None,
) -> str:
    """Identify ONE execution attempt, distinctly from the business execution it belongs to.

    This exists because of a real loss. Evidence paths were keyed only on `execution_id`, which is
    a *business* lineage key: the corrected Silver-to-Gold run and its idempotency repeat share
    one, by design, because they process the same Bronze snapshot. The repeat therefore overwrote
    the first run's report in the Volume, and the first run's figures survived only because they
    had been read before the repeat happened.

    The fix separates the two concepts rather than weakening either. `execution_id` keeps meaning
    "which data", and is still what the Delta MERGE and the execution-scoped deletes key on, so
    reruns stay idempotent at the data layer. The attempt id means "which run", and appears only
    in filenames.

    The Databricks job run id is preferred because it is unique, already recorded by the platform
    and links the evidence straight back to the run page. A UTC timestamp plus a UUID is the
    fallback for an interactive run, where no job run id exists. The UUID matters because two
    interactive attempts can start inside the same second and must still retain separate reports.
    """
    if job_run_id and str(job_run_id).strip():
        candidate = str(job_run_id).strip()
        # A job parameter that was never substituted arrives literally as "{{job.run_id}}".
        if not candidate.startswith("{{"):
            if not candidate.isdecimal():
                raise ValueError("job_run_id must be the numeric Databricks run ID.")
            return sanitize_path_token(f"run-{candidate}", label="job_run_id")
    moment = now or datetime.now(UTC)
    discriminator = unique_token or str(uuid.uuid4())
    return sanitize_path_token(
        f"ts-{moment.strftime('%Y%m%dT%H%M%S.%fZ')}-{discriminator}",
        label="attempt timestamp",
    )


def evidence_report_path(
    volume_root: str,
    *,
    phase: str,
    execution_id: str,
    attempt_id: str,
    suffix: str,
) -> str:
    """Build a unique, deterministic evidence path for one attempt of one execution.

    The shape is `<volume_root>/reports/<phase>/<execution_id>/<attempt_id>.<suffix>.json`.
    `execution_id` becomes a DIRECTORY, so every attempt at the same business execution collects
    beside its siblings instead of replacing them, and the run history for a given snapshot is
    readable by listing one directory.

    Deterministic by construction: the same four inputs always produce the same path, so a report
    can be located later without searching.
    """
    root = str(volume_root).rstrip("/")
    if not root:
        raise ValueError("volume_root must be non-empty.")
    if phase not in EVIDENCE_PHASES:
        raise ValueError(
            f"Unknown evidence phase {phase!r}; expected one of {sorted(EVIDENCE_PHASES)}."
        )
    execution = sanitize_path_token(execution_id, label="execution_id")
    attempt = sanitize_path_token(attempt_id, label="attempt_id")
    extension = sanitize_path_token(suffix, label="suffix")
    return f"{root}/reports/{phase}/{execution}/{attempt}.{extension}.json"


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return value


def _is_rejected(row: Mapping[str, Any]) -> bool:
    """A Bronze row is unusable when the payload failed to parse or carries no event ID."""
    return bool(row.get("parse_error")) or not str(row.get("event_id") or "").strip()


def build_bronze_execution_report(
    records: Iterable[Mapping[str, Any]],
    *,
    execution_id: str,
    expected_published_events: int,
    expected_source_last_updated: int,
    query_terminated: bool,
    query_progress: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Reconcile one execution ID without including credentials or raw payloads."""
    rows = [dict(record) for record in records]
    # Safety-critical filter: the shared Event Hub also holds messages from other producers and
    # from earlier runs, and the consumer starts at the earliest offset, so this single line is
    # what makes every count below belong to this execution only. Never remove it.
    matching_rows = [row for row in rows if str(row.get("execution_id")) == execution_id]
    rejected_rows = [row for row in matching_rows if _is_rejected(row)]
    accepted_rows = [row for row in matching_rows if not _is_rejected(row)]
    event_ids = [str(row["event_id"]) for row in accepted_rows]
    distinct_event_ids = sorted(set(event_ids))
    source_timestamps = {
        int(row["source_last_updated"])
        for row in accepted_rows
        if row.get("source_last_updated") is not None
    }

    partition_rows: dict[int, list[int]] = defaultdict(list)
    for row in matching_rows:
        partition = row.get("kafka_partition")
        offset = row.get("kafka_offset")
        if partition is not None and offset is not None:
            partition_rows[int(partition)].append(int(offset))
    offset_ranges = [
        {
            "partition": partition,
            "minimum_offset": min(offsets),
            "maximum_offset": max(offsets),
            "row_count": len(offsets),
        }
        for partition, offsets in sorted(partition_rows.items())
    ]

    duplicate_rows = len(event_ids) - len(distinct_event_ids)
    collection_timestamp_rows = sum(
        1 for row in accepted_rows if str(row.get("collected_at") or "").strip()
    )
    broker_timestamp_rows = sum(
        1 for row in accepted_rows if row.get("kafka_timestamp") is not None
    )
    source_timestamp_rows = sum(
        1 for row in accepted_rows if row.get("source_last_updated") is not None
    )
    passed = all(
        (
            query_terminated,
            len(matching_rows) == expected_published_events,
            len(distinct_event_ids) == expected_published_events,
            duplicate_rows == 0,
            len(rejected_rows) == 0,
            source_timestamps == {expected_source_last_updated},
            collection_timestamp_rows == expected_published_events,
            broker_timestamp_rows == expected_published_events,
            source_timestamp_rows == expected_published_events,
        )
    )
    return {
        "report_type": "urbanflow_bronze_execution",
        "execution_id": execution_id,
        "status": "PASS" if passed else "FAIL",
        "query_terminated": query_terminated,
        "expected_published_events": expected_published_events,
        "consumed_rows": len(matching_rows),
        "accepted_rows": len(accepted_rows),
        "rejected_rows": len(rejected_rows),
        "distinct_event_ids": len(distinct_event_ids),
        "duplicate_rows": duplicate_rows,
        "expected_source_last_updated": expected_source_last_updated,
        "observed_source_last_updated": sorted(source_timestamps),
        "collection_timestamp_rows": collection_timestamp_rows,
        "source_timestamp_rows": source_timestamp_rows,
        "broker_timestamp_rows": broker_timestamp_rows,
        "event_ids": distinct_event_ids,
        "offset_ranges": offset_ranges,
        "query_progress": _json_safe(dict(query_progress or {})),
    }


def reconcile_producer_and_bronze(
    producer_report: Mapping[str, Any], bronze_report: Mapping[str, Any]
) -> dict[str, Any]:
    """Compare producer and Bronze reports after a live run, entirely offline."""
    producer_ids = {str(value) for value in producer_report.get("event_ids", [])}
    bronze_ids = {str(value) for value in bronze_report.get("event_ids", [])}
    execution_ids_match = producer_report.get("execution_id") == bronze_report.get("execution_id")
    # A missing count stays negative so an incomplete report can never look reconciled.
    published = int(producer_report.get("published_events", -1))
    consumed = int(bronze_report.get("consumed_rows", -1))
    duplicate_bronze_rows = int(bronze_report.get("duplicate_rows", 0))
    bronze_status = str(bronze_report.get("status", "MISSING"))
    missing = sorted(producer_ids - bronze_ids)
    unexpected = sorted(bronze_ids - producer_ids)
    passed = all(
        (
            execution_ids_match,
            published >= 0,
            published == consumed,
            not missing,
            not unexpected,
            duplicate_bronze_rows == 0,
            bronze_status == "PASS",
        )
    )
    return {
        "report_type": "urbanflow_end_to_end_reconciliation",
        "execution_id": producer_report.get("execution_id"),
        "status": "PASS" if passed else "FAIL",
        "execution_ids_match": execution_ids_match,
        "published_events": published,
        "consumed_rows": consumed,
        "duplicate_bronze_rows": duplicate_bronze_rows,
        "bronze_status": bronze_status,
        "missing_event_ids": missing,
        "unexpected_event_ids": unexpected,
    }


def write_json_report(report: Mapping[str, Any], path: str | Path) -> Path:
    """Write deterministic JSON evidence to an already authorized path."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(_json_safe(dict(report)), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return destination
