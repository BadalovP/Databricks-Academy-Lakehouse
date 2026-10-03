"""UrbanFlow Silver, Quarantine and duplicate Lakeflow declarations.

Nothing here reimplements a business rule. All three tables come out of the single call to
`urbanflow.silver.split_silver_and_quarantine` that the notebook path and the unit tests also
use. A Lakeflow pipeline and a notebook that disagree about what "LOW_BIKES" means is the most
expensive kind of drift in a medallion project, and the only real defence is having exactly
one implementation of the rule.

**Why these read Bronze in batch rather than as a stream.** The deduplication keeps the first
arrival of a repeated `event_id`, decided by a deterministic ordering over Kafka timestamp,
partition and offset. That is a non-time-based window, and Spark rejects those on a streaming
DataFrame. The options were to rewrite the dedup as a streaming `dropDuplicates` - different
semantics, non-deterministic tie-break, unbounded state without a watermark - or to read
Bronze in batch and keep the exact rule. Keeping the rule won: these are materialized views
recomputed on each pipeline update, which for one snapshot of ~2,520 observations is cheap and
entirely correct. Bronze itself stays a streaming table, so ingestion is still incremental.

This module is only ever executed by a running Lakeflow pipeline. `pyspark.pipelines` is not
importable outside one and `spark` is injected as a global, which is why the test suite asserts
on this file's source and the bundle validates it statically, rather than importing it.
"""

from pyspark import pipelines as dp

from urbanflow.silver import split_silver_and_quarantine

# Expectations are warnings, not drop-or-fail rules, and that is deliberate. A dropping
# expectation on Silver would delete the very rows the quarantine table exists to preserve,
# and Bronze would stop reconciling to Silver plus Quarantine plus duplicates. Declared this
# way, the pipeline event log carries per-rule pass and fail counts while every row still
# lands somewhere accountable.
SILVER_EXPECTATIONS = {
    "event_id_present": "event_id IS NOT NULL",
    "station_id_present": "station_id IS NOT NULL",
    "observed_at_present": "observed_at IS NOT NULL",
    "bikes_not_negative": "num_bikes_available >= 0",
    "docks_not_negative": "num_docks_available >= 0",
    "status_is_known": (
        "availability_status IN "
        "('AVAILABLE','LOW_BIKES','LOW_DOCKS','LOW_BIKES_AND_DOCKS','OUT_OF_SERVICE')"
    ),
    # An out-of-service station must never also be reported as an actionable shortage. This is
    # the exact defect the first live Phase 2 run produced, so it is asserted in the pipeline
    # as well as in the unit tests.
    "out_of_service_is_not_actionable": (
        "availability_status <> 'OUT_OF_SERVICE' OR (is_low_bikes = false AND is_low_docks = false)"
    ),
}

QUARANTINE_EXPECTATIONS = {
    # A quarantined row without a reason is unexplained rather than quarantined.
    "has_failure_reason": "failed_rules IS NOT NULL AND size(failed_rules) > 0",
}

DUPLICATE_EXPECTATIONS = {
    "duplicate_has_event_id": "event_id IS NOT NULL",
}


def _split(session):
    """Run the shared split once, with thresholds from the pipeline configuration."""
    low_bikes = int(session.conf.get("urbanflow.low_bike_threshold", "2"))
    low_docks = int(session.conf.get("urbanflow.low_dock_threshold", "2"))
    bronze = session.read.table("bronze_station_status")
    return split_silver_and_quarantine(
        bronze,
        low_bike_threshold=low_bikes,
        low_dock_threshold=low_docks,
    )


@dp.materialized_view(
    name="silver_station_status",
    comment=(
        "Deduplicated, contract-checked station observations with operational flags and "
        "availability status. Business rules are shared with the notebook path."
    ),
    table_properties={"quality": "silver", "project": "urbanflow"},
)
@dp.expect_all(SILVER_EXPECTATIONS)
def silver_station_status():
    """One row per accepted station observation, first arrival winning for a repeated ID."""
    return _split(spark).silver  # noqa: F821 - spark is a Lakeflow-injected global


@dp.materialized_view(
    name="quarantine_station_status",
    comment=(
        "Rows rejected by the Silver contract, each carrying the rules it failed. Preserved "
        "rather than dropped so Bronze always reconciles to Silver plus Quarantine plus "
        "duplicates."
    ),
    table_properties={"quality": "quarantine", "project": "urbanflow"},
)
@dp.expect_all(QUARANTINE_EXPECTATIONS)
def quarantine_station_status():
    """Every rejected row with its reasons. A growing quarantine is a finding, not a failure."""
    return _split(spark).quarantine  # noqa: F821 - spark is a Lakeflow-injected global


@dp.materialized_view(
    name="duplicate_station_status",
    comment=(
        "Later copies of an already-seen event_id. Recorded rather than discarded so the "
        "Bronze-to-Silver row counts stay provable."
    ),
    table_properties={"quality": "quarantine", "project": "urbanflow"},
)
@dp.expect_all(DUPLICATE_EXPECTATIONS)
def duplicate_station_status():
    """Redeliveries of an observation already in Silver."""
    return _split(spark).duplicates  # noqa: F821 - spark is a Lakeflow-injected global


__all__ = [
    "DUPLICATE_EXPECTATIONS",
    "QUARANTINE_EXPECTATIONS",
    "SILVER_EXPECTATIONS",
    "duplicate_station_status",
    "quarantine_station_status",
    "silver_station_status",
]
