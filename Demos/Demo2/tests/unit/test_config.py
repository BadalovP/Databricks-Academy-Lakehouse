import pytest

from demo2.config import validate_runtime_configuration


def configured(**overrides):
    values = {
        "bundle_target": "azure_dev",
        "catalog": "dbr_dev",
        "schema": "parvinbadalov",
        "volume_name": "demo2_ecommerce",
        "expected_catalog": "dbr_dev",
        "expected_schema": "parvinbadalov",
        "expected_volume_name": "demo2_ecommerce",
    }
    values.update(overrides)
    return values


def test_explicit_dev_configuration_is_allowed():
    validate_runtime_configuration(**configured())


def test_explicit_prod_configuration_is_allowed():
    validate_runtime_configuration(
        **configured(
            bundle_target="azure_prod",
            catalog="prod_catalog",
            schema="prod_schema",
            volume_name="prod_volume",
            expected_catalog="prod_catalog",
            expected_schema="prod_schema",
            expected_volume_name="prod_volume",
        )
    )


@pytest.mark.parametrize(
    "field",
    [
        "bundle_target",
        "catalog",
        "schema",
        "volume_name",
        "expected_catalog",
        "expected_schema",
        "expected_volume_name",
    ],
)
def test_missing_required_configuration_fails_closed(field):
    with pytest.raises(RuntimeError, match="Missing required Demo 2 configuration"):
        validate_runtime_configuration(**configured(**{field: ""}))


def test_unknown_target_fails_closed():
    with pytest.raises(RuntimeError, match="Unsupported Demo 2 bundle target"):
        validate_runtime_configuration(**configured(bundle_target="unexpected"))


def test_runtime_override_fails_closed():
    with pytest.raises(RuntimeError, match="does not match the explicit bundle target"):
        validate_runtime_configuration(**configured(schema="other_schema"))
