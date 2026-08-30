"""Write the deterministic V1 or V2 order landing file."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from demo2.data_generation import (  # noqa: E402
    generate_v1_orders,
    generate_v2_orders,
    write_json_lines,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("v1", "v2"))
    parser.add_argument("output_path", type=Path)
    args = parser.parse_args()
    rows = generate_v1_orders() if args.phase == "v1" else generate_v2_orders()
    write_json_lines(args.output_path, rows)
    print(f"wrote {len(rows)} rows to {args.output_path}")


if __name__ == "__main__":
    main()
