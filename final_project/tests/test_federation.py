import re

import pytest

from urbanflow.federation import FederationSource, select_statement, validate_maintenance_columns


def test_federation_query_is_select_only_and_fully_quoted() -> None:
    statement = select_statement(FederationSource("urbanflow_ops", "public.station_maintenance"))
    assert statement.startswith("SELECT maintenance_id")
    assert "FROM `urbanflow_ops`.`public`.`station_maintenance`" in statement
    # Word boundaries: the column updated_at must not count as an UPDATE statement.
    assert not re.search(r"(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER)", statement.upper())


def test_federation_identifiers_fail_closed() -> None:
    with pytest.raises(ValueError):
        _ = FederationSource("ops; DROP CATALOG x", "public.maintenance").qualified_table


def test_federation_contract_lists_missing_columns() -> None:
    with pytest.raises(ValueError, match="severity"):
        validate_maintenance_columns({"maintenance_id", "station_id", "status", "updated_at"})
