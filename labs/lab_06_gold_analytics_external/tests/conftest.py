import sys
from pathlib import Path


try:
    LAB_ROOT = Path(__file__).resolve().parents[1]
except NameError:
    current_dir = Path.cwd()

    LAB_ROOT = (
        current_dir.parent
        if current_dir.name == "tests"
        else current_dir
    )


if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))