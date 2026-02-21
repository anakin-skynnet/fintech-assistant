#!/usr/bin/env python3
"""
Generate sample closure Excel files for end-to-end testing.
Output files match config/closure_schema.yaml: amount, currency, account_code,
description, value_date, business_unit. Use for upload to UC volume or SharePoint.
"""
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd


# Schema-aligned columns and sample valid data
COLUMNS = [
    "amount",
    "currency",
    "account_code",
    "description",
    "value_date",
    "business_unit",
]


def make_sample_rows(business_unit: str, num_rows: int = 5, base_date: str = "2025-02-15") -> list[dict]:
    """Generate valid sample rows for one BU."""
    rows = []
    for i in range(1, num_rows + 1):
        rows.append({
            "amount": 10000.0 * i + 500,
            "currency": "BRL",
            "account_code": f"ACC-{business_unit}-{i:04d}",
            "description": f"Sample closure line {i} for {business_unit}",
            "value_date": base_date,
            "business_unit": business_unit,
        })
    return rows


def write_excel(path: Path, rows: list[dict]) -> None:
    """Write one Excel file with header row 1 and data rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=COLUMNS)
    df.to_excel(path, index=False, sheet_name="Closure", engine="openpyxl")


def main():
    parser = argparse.ArgumentParser(description="Generate sample closure Excel files for e2e testing.")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "samples" / "closure_excel",
        help="Output directory for Excel files",
    )
    parser.add_argument(
        "--bus",
        nargs="+",
        default=["BU_A", "BU_B", "BU_C"],
        help="Business units (one file per BU)",
    )
    parser.add_argument("--rows", type=int, default=5, help="Rows per file")
    parser.add_argument("--date", default="2025-02-15", help="Value date yyyy-MM-dd")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    for bu in args.bus:
        rows = make_sample_rows(bu, num_rows=args.rows, base_date=args.date)
        # File name pattern similar to what BUs might upload (e.g. closure_bu_a_202502.xlsx)
        period = args.date[:7].replace("-", "")  # 202502
        path = args.out_dir / f"closure_{bu.lower()}_{period}.xlsx"
        write_excel(path, rows)
        print(f"Wrote {path} ({len(rows)} rows)")

    print(f"Done. Upload files from {args.out_dir} to the UC volume or SharePoint for e2e testing.")


if __name__ == "__main__":
    main()
