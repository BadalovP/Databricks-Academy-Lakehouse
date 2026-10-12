from urbanflow.cdc import ChangeEvent, ChangeType, apply_cdf, reconcile


def event(
    maintenance_id: str,
    change_type: ChangeType,
    version: int,
    *,
    status: str = "OPEN",
) -> ChangeEvent:
    return ChangeEvent(
        maintenance_id=maintenance_id,
        station_id=f"station-{maintenance_id}",
        change_type=change_type,
        commit_version=version,
        commit_timestamp=f"2026-10-12T10:{version:02d}:00Z",
        values={"status": status, "severity": "MEDIUM", "updated_at": f"t{version}"},
    )


def test_cdf_handles_insert_update_delete_and_preimage() -> None:
    events = [
        event("m1", ChangeType.INSERT, 1),
        event("m1", ChangeType.UPDATE_PREIMAGE, 2),
        event("m1", ChangeType.UPDATE_POSTIMAGE, 2, status="CLOSED"),
        event("m2", ChangeType.INSERT, 2),
        event("m2", ChangeType.DELETE, 3),
    ]
    result = apply_cdf({}, events, checkpoint=-1)
    assert set(result.target) == {"m1"}
    assert result.target["m1"]["status"] == "CLOSED"
    assert (result.inserted, result.updated, result.deleted, result.ignored_preimages) == (
        2,
        1,
        1,
        1,
    )
    assert result.checkpoint == 3
    assert len({item.change_id for item in result.history}) == len(result.history)


def test_repeat_from_checkpoint_is_idempotent() -> None:
    events = [event("m1", ChangeType.INSERT, 4)]
    first = apply_cdf({}, events, checkpoint=3)
    second = apply_cdf(first.target, events, checkpoint=first.checkpoint)
    assert second.target == first.target
    assert second.changed == 0
    assert second.history == ()


def test_reconciliation_reports_exact_differences() -> None:
    source = {"m1": {"station_id": "s1", "status": "OPEN", "severity": "LOW", "updated_at": "t"}}
    target = {"m2": {"station_id": "s2", "status": "OPEN", "severity": "LOW", "updated_at": "t"}}
    result = reconcile(source, target)
    assert result.missing_in_target == ("m1",)
    assert result.unexpected_in_target == ("m2",)
    assert not result.matches
