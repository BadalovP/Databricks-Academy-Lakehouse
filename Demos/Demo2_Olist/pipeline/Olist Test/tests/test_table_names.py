# Purpose: unit-test the reusable table-name utility.

from pathlib import Path
import sys

import pytest


# The current directory is the tests folder.
# Add the sibling utilities folder to Python's import path.
UTILITIES_DIR = Path.cwd().parent / "utilities"
sys.path.insert(0, str(UTILITIES_DIR))

from table_names import qualified_table_name


def test_qualified_table_name():
    """Protect construction of three-level Unity Catalog table names."""

    result = qualified_table_name(
        "dbr_dev",
        "parvinbadalov",
        "gold_fact_order_items",
    )

    assert result == "dbr_dev.parvinbadalov.gold_fact_order_items"


def test_qualified_table_name_rejects_empty_value():
    """Reject incomplete identifiers before a table query is attempted."""

    with pytest.raises(ValueError):
        qualified_table_name(
            "dbr_dev",
            "",
            "gold_fact_order_items",
        )