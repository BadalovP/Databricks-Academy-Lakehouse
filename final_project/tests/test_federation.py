import pytest

from urbanflow.federation import FederationSource, select_statement, validate_maintenance_columns


def test_federation_query_is_select_only_and_fully_quoted() -> None:
    statement = select_statement(FederationSource("urbanflow_ops", "public.station_maintenance"))
    assert statement.startswith("SELECT maintenance_id")
    assert "FROM `urbanflow_ops`.`public`.`station_maintenance`" in statement
    assert all(token not in statement.upper() for token in ("INSERT", "UPDATE", "DELETE", "MERGE"))


def test_federation_identifiers_fail_closed() -> None:
    with pytest.raises(ValueError):
        _ = FederationSource("ops; DROP CATALOG x", "public.maintenance").qualified_table


def test_federation_contract_lists_missing_columns() -> None:
    with pytest.raises(ValueError, match="severity"):
        validate_maintenance_columns(
            {"maintenance_id", "station_id", "status", "updated_at"}
        )

