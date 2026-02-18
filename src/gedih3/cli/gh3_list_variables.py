#!/usr/bin/env python
"""
List column names and types from an H3 database.

Reads the parquet schema from the H3 database and displays columns
with their data types. Supports filtering by product suffix and grep
pattern, with table, JSON, or CSV output.

Author: Tiago de Conto
Package: gedih3
"""

import argparse
import os
import sys


def get_cmd_args():
    """Parse command line arguments"""
    p = argparse.ArgumentParser(
        description="List column names and types from an H3 database"
    )

    p.add_argument(
        "-d", "--database",
        dest="database",
        type=str,
        default=None,
        help="path to H3 database [default: GH3_DEFAULT_H3_DIR]"
    )

    p.add_argument(
        "-p", "--product",
        dest="product",
        type=str,
        default=None,
        help="filter columns by product suffix (e.g., L2A → columns ending in _l2a)"
    )

    p.add_argument(
        "-g", "--grep",
        dest="grep",
        type=str,
        default=None,
        help="filter columns by pattern (case-insensitive)"
    )

    p.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="output in JSON format"
    )

    p.add_argument(
        "--csv",
        dest="csv_output",
        action="store_true",
        help="output in CSV format"
    )

    p.add_argument(
        "--no-header",
        dest="no_header",
        action="store_true",
        help="suppress headers in output"
    )

    from gedih3.cliutils import add_storage_args
    add_storage_args(p)

    return p.parse_args()


def main():
    args = get_cmd_args()

    from gedih3.cliutils import setup_storage
    setup_storage(args)

    # Resolve database path
    if args.database is None:
        from gedih3.config import GH3_DEFAULT_H3_DIR
        args.database = GH3_DEFAULT_H3_DIR

    from gedih3.utils import smart_exists
    if not smart_exists(args.database):
        print(f"Error: Database not found: {args.database}", file=sys.stderr)
        sys.exit(1)

    # Read schema
    try:
        from gedih3.utils import read_schema
        schema_df = read_schema(args.database)
    except Exception as e:
        print(f"Error reading schema: {e}", file=sys.stderr)
        sys.exit(1)

    # Apply product suffix filter (-p L2A → keep columns ending in _l2a)
    if args.product:
        suffix = f"_{args.product.lower()}"
        mask = schema_df['column'].str.lower().str.endswith(suffix)
        schema_df = schema_df[mask]

    # Apply grep filter
    if args.grep:
        mask = schema_df['column'].str.contains(args.grep, case=False)
        schema_df = schema_df[mask]

    columns = list(zip(schema_df['column'], schema_df['dtype'].astype(str)))

    # JSON output
    if args.json_output:
        import json
        data = [{"name": c[0], "dtype": c[1]} for c in columns]
        print(json.dumps(data, indent=2))
        return

    # CSV output
    if args.csv_output:
        print("name,dtype")
        for col in columns:
            print(f"{col[0]},{col[1]}")
        return

    # Default table output
    if not args.no_header:
        print()
        print(f"H3 Database Schema: {args.database}")
        print("=" * 70)
        print()
        print(f"{'Column':<40} {'Type':<30}")
        print("-" * 70)

    for col in columns:
        print(f"{col[0]:<40} {col[1]:<30}")

    if not args.no_header:
        print("-" * 70)
        print(f"Total: {len(columns)} columns")
        print()


if __name__ == '__main__':
    main()
