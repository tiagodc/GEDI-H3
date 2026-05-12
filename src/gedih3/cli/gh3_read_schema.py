#!/usr/bin/env python

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""
Inspect schema of parquet, feather, geopackage, HDF5 files, or H3 databases.

Read and display column names and data types from output datasets,
useful for understanding extracted data structure. Supports single
files, dataset directories, and H3 databases (auto-detects format).
When no path is given, reads from the default H3 database.
"""

import argparse
import os
import sys


def get_cmd_args():
    """Parse command line arguments"""
    p = argparse.ArgumentParser(
        description="Inspect schema of parquet, feather, geopackage, HDF5 files, or H3 databases"
    )

    p.add_argument(
        "path",
        type=str,
        nargs='?',
        default=None,
        help="path to file or directory to inspect [default: GH3_DEFAULT_H3_DIR]"
    )

    p.add_argument(
        "-p", "--product",
        dest="product",
        type=str,
        default=None,
        help="filter columns by product suffix (e.g., L2A → columns ending in _l2a)"
    )

    p.add_argument(
        "--grep",
        dest="grep",
        type=str,
        default=None,
        help="filter columns by pattern (case-insensitive)"
    )

    p.add_argument(
        "-g", "--group",
        dest="group",
        type=str,
        default=None,
        help="for HDF5 files, specific group/beam to inspect (e.g., 'BEAM0101')"
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


def _detect_file_type(path):
    """Detect display name for the file/directory type."""
    from gedih3.utils import smart_exists, smart_isdir, smart_join
    path_lower = path.lower()
    if path_lower.endswith(('.h5', '.hdf5')):
        return "HDF5"
    if smart_isdir(path):
        from gedih3.config import BUILD_LOG_FILENAME
        build_log = smart_join(path, BUILD_LOG_FILENAME)
        if smart_exists(build_log):
            return "H3 Database"
        from gedih3.cliutils import detect_dataset_format
        fmt = detect_dataset_format(path)
        return {'parquet': 'Parquet', 'feather': 'Feather', 'gpkg': 'GeoPackage'}.get(fmt, fmt)
    ext = os.path.splitext(path)[1].lstrip('.').lower()
    return {'parquet': 'Parquet', 'feather': 'Feather', 'gpkg': 'GeoPackage'}.get(ext, ext)


def _format_shape(rows, cols):
    """Format HDF5 dataset shape as a string."""
    return f"({rows},)" if cols == 1 else f"({rows}, {cols})"


def _print_table(schema_df, file_type, path, group=None, no_header=False):
    """Print schema as a formatted terminal table."""
    is_hdf5 = 'path' in schema_df.columns

    if not no_header:
        print()
        print(f"{file_type} Schema: {path}")
        if group:
            print(f"Group: {group}")
        print("=" * 70)
        print()

    if is_hdf5:
        if not no_header:
            print(f"{'Dataset Path':<40} {'Type':<15} {'Shape':<15}")
            print("-" * 70)
        for _, row in schema_df.iterrows():
            shape = _format_shape(row['rows'], row['cols'])
            print(f"{row['path']:<40} {str(row['dtype']):<15} {shape:<15}")
    else:
        if not no_header:
            print(f"{'Column':<40} {'Type':<30}")
            print("-" * 70)
        for _, row in schema_df.iterrows():
            print(f"{row['column']:<40} {str(row['dtype']):<30}")

    if not no_header:
        print("-" * 70)
        print(f"Total: {len(schema_df)} {'datasets' if is_hdf5 else 'columns'}")
        print()


def _print_json(schema_df):
    """Print schema as JSON."""
    import json
    is_hdf5 = 'path' in schema_df.columns
    if is_hdf5:
        data = [{"name": row['path'], "dtype": str(row['dtype']),
                 "shape": _format_shape(row['rows'], row['cols'])}
                for _, row in schema_df.iterrows()]
    else:
        data = [{"name": row['column'], "dtype": str(row['dtype'])}
                for _, row in schema_df.iterrows()]
    print(json.dumps(data, indent=2))


def _print_csv(schema_df):
    """Print schema as CSV."""
    is_hdf5 = 'path' in schema_df.columns
    if is_hdf5:
        print("name,dtype,shape")
        for _, row in schema_df.iterrows():
            print(f"{row['path']},{row['dtype']},{_format_shape(row['rows'], row['cols'])}")
    else:
        print("name,dtype")
        for _, row in schema_df.iterrows():
            print(f"{row['column']},{row['dtype']}")


def main():
    args = get_cmd_args()

    from gedih3.cliutils import setup_storage
    setup_storage(args)

    # Resolve default path from environment
    if args.path is None:
        from gedih3.config import GH3_DEFAULT_H3_DIR
        args.path = GH3_DEFAULT_H3_DIR

    from gedih3.utils import smart_exists
    if not smart_exists(args.path):
        print(f"Error: Path not found: {args.path}", file=sys.stderr)
        sys.exit(1)

    try:
        from gedih3.utils import read_schema
        schema_df = read_schema(args.path, root=args.group)
        # Sort by name column for user-friendly output
        sort_col = 'path' if 'path' in schema_df.columns else 'column'
        schema_df = schema_df.sort_values(sort_col).reset_index(drop=True)
        file_type = _detect_file_type(args.path)
    except Exception as e:
        print(f"Error reading schema: {e}", file=sys.stderr)
        sys.exit(1)

    # Apply column filters (only for non-HDF5 schemas)
    if 'column' in schema_df.columns:
        if args.product:
            suffix = f"_{args.product.lower()}"
            mask = schema_df['column'].str.lower().str.endswith(suffix)
            schema_df = schema_df[mask]
        if args.grep:
            mask = schema_df['column'].str.contains(args.grep, case=False)
            schema_df = schema_df[mask]

    if args.json_output:
        _print_json(schema_df)
    elif args.csv_output:
        _print_csv(schema_df)
    else:
        _print_table(schema_df, file_type, args.path, args.group, args.no_header)


if __name__ == '__main__':
    main()
