"""Test-collection setup shared across the LAB 09 test suite.

scripts/ holds standalone operator scripts that are deliberately not part of
the installed `lab09` package (see scripts/validate_classic_e2e.py's module
docstring for why) -- this makes them importable by their tests the same
way `lab09` itself is importable, without sys.path hacks inside every test
module that needs one.
"""

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
