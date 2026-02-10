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


def read_hdf5_schema(path: str, group: str = None) -> list:
    """
    Read schema from an HDF5 file.

    Parameters
    ----------
    path : str
        Path to HDF5 file
    group : str, optional
        Specific group/beam to inspect

    Returns
    -------
    list of tuples
        List of (dataset_path, data_type, shape) tuples
    """
    import h5py

    columns = []

    with h5py.File(path, 'r') as f:
        def visitor(name, obj):
            if isinstance(obj, h5py.Dataset):
                dtype = str(obj.dtype)
                shape = str(obj.shape)
                columns.append((name, dtype, shape))

        if group:
            if group in f:
                f[group].visititems(visitor)
            else:
                raise ValueError(f"Group '{group}' not found in {path}")
        else:
            f.visititems(visitor)

    return columns


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


def main():
    args = get_cmd_args()

    if not os.path.exists(args.path):
        print(f"Error: Path not found: {args.path}", file=sys.stderr)
        sys.exit(1)

    # Determine file type and read schema
    path_lower = args.path.lower()
    is_hdf5 = False

    try:
        if path_lower.endswith(('.h5', '.hdf5')):
            # HDF5 has its own schema format (path, dtype, shape)
            columns = read_hdf5_schema(args.path, args.group)
            file_type = "HDF5"
            is_hdf5 = True
        else:
            # Use package-level read_schema for parquet, feather, gpkg, and directories
            from gedih3.utils import read_schema
            schema_df = read_schema(args.path)

            # Determine display name
            if os.path.isdir(args.path):
                from gedih3.cliutils import detect_dataset_format
                fmt = detect_dataset_format(args.path)
                file_type = {'parquet': 'Parquet', 'feather': 'Feather', 'gpkg': 'GeoPackage'}.get(fmt, fmt)
            else:
                ext = os.path.splitext(args.path)[1].lstrip('.').lower()
                file_type = {'parquet': 'Parquet', 'feather': 'Feather', 'gpkg': 'GeoPackage'}.get(ext, ext)

            columns = list(zip(schema_df['column'], schema_df['dtype'].astype(str)))

    except Exception as e:
        print(f"Error reading schema: {e}", file=sys.stderr)
        sys.exit(1)

    # JSON output
    if args.json_output:
        import json
        if is_hdf5:
            data = [{"name": c[0], "dtype": c[1], "shape": c[2]} for c in columns]
        else:
            data = [{"name": c[0], "dtype": c[1]} for c in columns]
        print(json.dumps(data, indent=2))
        return

    # CSV output
    if args.csv_output:
        if is_hdf5:
            print("name,dtype,shape")
            for col in columns:
                print(f"{col[0]},{col[1]},{col[2]}")
        else:
            print("name,dtype")
            for col in columns:
                print(f"{col[0]},{col[1]}")
        return

    # Default table output
    if not args.no_header:
        print()
        print(f"{file_type} Schema: {args.path}")
        if args.group:
            print(f"Group: {args.group}")
        print("=" * 70)
        print()

    if is_hdf5:
        if not args.no_header:
            print(f"{'Dataset Path':<40} {'Type':<15} {'Shape':<15}")
            print("-" * 70)
        for col in columns:
            print(f"{col[0]:<40} {col[1]:<15} {col[2]:<15}")
    else:
        if not args.no_header:
            print(f"{'Column':<40} {'Type':<30}")
            print("-" * 70)
        for col in columns:
            print(f"{col[0]:<40} {col[1]:<30}")

    if not args.no_header:
        print("-" * 70)
        print(f"Total: {len(columns)} {'datasets' if is_hdf5 else 'columns'}")
        print()


if __name__ == '__main__':
    main()
