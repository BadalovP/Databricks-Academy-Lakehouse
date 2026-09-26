from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied

from lab09 import volumes


def _autospec_client() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


def _cfg() -> dict:
    return {
        "catalog": "dbr_dev",
        "schema": "parvinbadalov",
        "volume": "lab09_landing",
        "pipeline": {"target_schema": "lab09"},
    }


def test_ensure_volume_reuses_existing_volume_without_creating():
    client = _autospec_client()
    existing = MagicMock()
    existing.full_name = "dbr_dev.parvinbadalov.lab09_landing"
    client.volumes.read.return_value = existing

    result = volumes.ensure_volume(client, _cfg())

    assert result is existing
    client.volumes.create.assert_not_called()


def test_ensure_volume_creates_when_missing():
    """The defect this guards against: the old NOT_FOUND string check never
    matched the real error_code (RESOURCE_DOES_NOT_EXIST), so a genuinely
    missing volume would have raised instead of being created.
    """
    client = _autospec_client()
    client.volumes.read.side_effect = NotFound(
        "volume does not exist", error_code="RESOURCE_DOES_NOT_EXIST"
    )
    created = MagicMock()
    client.volumes.create.return_value = created

    result = volumes.ensure_volume(client, _cfg())

    assert result is created
    client.volumes.create.assert_called_once()
    _, kwargs = client.volumes.create.call_args
    assert kwargs["catalog_name"] == "dbr_dev"
    assert kwargs["schema_name"] == "parvinbadalov"
    assert kwargs["name"] == "lab09_landing"


def test_ensure_volume_propagates_permission_errors_without_creating():
    client = _autospec_client()
    client.volumes.read.side_effect = PermissionDenied("not authorized")

    with pytest.raises(PermissionDenied):
        volumes.ensure_volume(client, _cfg())

    client.volumes.create.assert_not_called()


# --- ensure_output_schema (dedicated pipeline OUTPUT schema) ----------------


def test_ensure_output_schema_reuses_existing_schema_without_creating():
    client = _autospec_client()
    existing = MagicMock()
    existing.full_name = "dbr_dev.lab09"
    client.schemas.get.return_value = existing

    result = volumes.ensure_output_schema(client, _cfg())

    assert result is existing
    client.schemas.get.assert_called_once_with("dbr_dev.lab09")
    client.schemas.create.assert_not_called()


def test_ensure_output_schema_creates_when_missing():
    client = _autospec_client()
    client.schemas.get.side_effect = NotFound(
        "schema does not exist", error_code="RESOURCE_DOES_NOT_EXIST"
    )
    created = MagicMock()
    client.schemas.create.return_value = created

    result = volumes.ensure_output_schema(client, _cfg())

    assert result is created
    client.schemas.create.assert_called_once()
    _, kwargs = client.schemas.create.call_args
    assert kwargs["catalog_name"] == "dbr_dev"
    assert kwargs["name"] == "lab09"


def test_ensure_output_schema_propagates_permission_errors_without_creating():
    client = _autospec_client()
    client.schemas.get.side_effect = PermissionDenied("not authorized")

    with pytest.raises(PermissionDenied):
        volumes.ensure_output_schema(client, _cfg())

    client.schemas.create.assert_not_called()


def test_ensure_output_schema_is_idempotent_across_repeated_calls():
    client = _autospec_client()
    existing = MagicMock()
    existing.full_name = "dbr_dev.lab09"
    client.schemas.get.return_value = existing

    volumes.ensure_output_schema(client, _cfg())
    volumes.ensure_output_schema(client, _cfg())

    assert client.schemas.get.call_count == 2
    client.schemas.create.assert_not_called()


def test_ensure_output_schema_never_touches_the_landing_schema():
    """The landing Volume's schema (cfg["schema"]) must stay untouched --
    only the pipeline's separate target_schema is ensured here.
    """
    client = _autospec_client()
    client.schemas.get.side_effect = NotFound("missing", error_code="RESOURCE_DOES_NOT_EXIST")
    client.schemas.create.return_value = MagicMock()

    volumes.ensure_output_schema(client, _cfg())

    client.schemas.get.assert_called_once_with("dbr_dev.lab09")
    _, kwargs = client.schemas.create.call_args
    assert kwargs["name"] != "parvinbadalov"
