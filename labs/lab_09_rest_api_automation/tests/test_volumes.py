from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied

from lab09 import volumes


def _autospec_client() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


def _cfg() -> dict:
    return {"catalog": "dbr_dev", "schema": "parvinbadalov", "volume": "lab09_landing"}


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
