#!/usr/bin/env python
"""
Inspect schema of parquet, feather, geopackage, or HDF5 files.

Read and display column names and data types from output datasets,
useful for understanding extracted data structure. Supports single
files and dataset directories (auto-detects format from metadata).

Author: Tiago de Conto
Package: gedih3
"""

import argparse
import os
import sys


def get_cmd_args():
    """Parse command line arguments"""
    p = argparse.ArgumentParser(
        description="Inspect schema of parquet, feather, geopackage, or HDF5 files"
    )

    p.add_argument(
        "path",
        type=str,
        help="path to file or directory to inspect"
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

    return p.parse_args()


def _detect_file_type(path):
    """Detect display name for the file/directory type."""
    path_lower = path.lower()
    if path_lower.endswith(('.h5', '.hdf5')):
        return "HDF5"
    if os.path.isdir(path):
        from gedih3.config import BUILD_LOG_FILENAME
        build_log = os.path.join(path, BUILD_LOG_FILENAME)
        if os.path.exists(build_log):
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

    if not os.path.exists(args.path):
        print(f"Error: Path not found: {args.path}", file=sys.stderr)
        sys.exit(1)

    try:
        from gedih3.utils import read_schema
        schema_df = read_schema(args.path, root=args.group)
        file_type = _detect_file_type(args.path)
    except Exception as e:
        print(f"Error reading schema: {e}", file=sys.stderr)
        sys.exit(1)

    if args.json_output:
        _print_json(schema_df)
    elif args.csv_output:
        _print_csv(schema_df)
    else:
        _print_table(schema_df, file_type, args.path, args.group, args.no_header)


if __name__ == '__main__':
    main()
