from __future__ import annotations

import pytest

from lab12.delta_store import create_lab_tables, qualified_name


class FakeSpark:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def sql(self, query: str) -> None:
        self.queries.append(query)


TABLES = {
    "documents": "documents",
    "chunks": "document_chunks",
    "evaluation_results": "evaluation_results",
    "monitoring": "query_monitoring",
}


def test_qualified_name_quotes_valid_identifiers() -> None:
    assert qualified_name("dbr_dev", "lab12", "chunks") == "`dbr_dev`.`lab12`.`chunks`"


def test_qualified_name_rejects_injection() -> None:
    with pytest.raises(ValueError):
        qualified_name("dbr_dev", "lab12; DROP SCHEMA", "chunks")


def test_table_creation_requires_opt_in() -> None:
    with pytest.raises(PermissionError):
        create_lab_tables(FakeSpark(), catalog="dbr_dev", schema="lab12", tables=TABLES)


def test_table_ddl_is_non_destructive_and_enables_cdf() -> None:
    spark = FakeSpark()
    create_lab_tables(
        spark, catalog="dbr_dev", schema="lab12", tables=TABLES, allow_create=True
    )
    text = "\n".join(spark.queries).upper()
    assert "CREATE TABLE IF NOT EXISTS" in text
    assert "DELTA.ENABLECHANGEDATAFEED = TRUE" in text
    assert "DROP " not in text
    assert "REPLACE " not in text

