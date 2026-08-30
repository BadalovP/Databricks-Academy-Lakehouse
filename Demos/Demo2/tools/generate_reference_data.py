"""Write deterministic customers and products to a chosen root."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from demo2.data_generation import write_reference_data  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("output_root", type=Path)
    args = parser.parse_args()
    for path in write_reference_data(args.output_root):
        print(path)


if __name__ == "__main__":
    main()
