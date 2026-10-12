from dataclasses import replace

import pytest

from lab11.config import MAX_LIVE_RECORDS, ConfigError, Credentials, identifier, run_identifier


def test_zerobus_endpoints_follow_the_documented_format(config):
    assert (
        config.target("azure_dev").zerobus_endpoint
        == "https://7405604503619901.zerobus.eastus.azuredatabricks.net"
    )
    assert (
        config.target("personal_dev").zerobus_endpoint
        == "https://7474653929863069.zerobus.us-east-2.cloud.databricks.com"
    )


def test_targets_use_separate_isolated_schemas(config):
    names = {t.bronze_table for t in config.targets.values()}
    assert names == {
        "workspace.lab_11_zerobus.bronze_iot_events",
        "dbr_dev.parvinbadalov_lab11_zerobus.bronze_iot_events",
    }


def test_default_run_stays_within_the_live_limit(config):
    assert config.producer.total_deliveries == 30 <= MAX_LIVE_RECORDS


def test_oversized_run_is_refused(config):
    with pytest.raises(ConfigError, match="lab limit"):
        replace(config.producer, devices=10).validate()


def test_missing_credentials_name_the_variables_and_the_privileges():
    with pytest.raises(ConfigError) as error:
        Credentials.from_env({"ZEROBUS_CLIENT_ID": "abc"})
    message = str(error.value)
    assert "set ZEROBUS_CLIENT_SECRET." in message
    assert "MODIFY" in message and "service principal" in message


def test_credentials_never_reveal_the_secret():
    credentials = Credentials.from_env(
        {"ZEROBUS_CLIENT_ID": "11112222-aaaa", "ZEROBUS_CLIENT_SECRET": "super-secret-value"}
    )
    assert "super-secret-value" not in repr(credentials)
    assert "super-secret-value" not in str(credentials)


@pytest.mark.parametrize("bad", ["", "a-b", "x;DROP", "dé", "1abc"])
def test_identifiers_must_be_plain_ascii(bad):
    with pytest.raises(ConfigError):
        identifier(bad, "schema")


@pytest.mark.parametrize("bad", ["", "a b", "x';--", "-lead"])
def test_run_ids_are_safe_to_embed_in_sql(bad):
    with pytest.raises(ConfigError):
        run_identifier(bad)


def test_backoff_is_exponential_and_capped(config):
    transport = config.transport
    assert [transport.backoff(n) for n in (1, 2, 3, 4, 5)] == [1.0, 2.0, 4.0, 8.0, 8.0]
