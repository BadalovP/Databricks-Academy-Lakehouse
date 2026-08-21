from src.config import Lab06ExternalConfig


def make_config():
    return Lab06ExternalConfig(
        catalog="dbr_dev",
        source_schema="parvinbadalov",
        source_volume_name="lab06_gold_analytics",
        target_schema="parvinbadalov_lab06_ext",
        external_gold_root="abfss://container@account.dfs.core.windows.net/lab06_gold_external_v2",
    )


def test_source_and_target_are_separate():
    config = make_config()
    assert config.source_schema == "parvinbadalov"
    assert config.target_schema == "parvinbadalov_lab06_ext"


def test_external_table_path_is_sibling_storage():
    config = make_config()
    assert config.table_path("fact_encounters").endswith(
        "/lab06_gold_external_v2/fact_encounters"
    )
    assert "/Volumes/" not in config.table_path("fact_encounters")


def test_fully_qualified_target_table():
    config = make_config()
    assert (
        config.fact_encounters
        == "dbr_dev.parvinbadalov_lab06_ext.fact_encounters"
    )
