#!/usr/bin/env python
"""
Inspect schema of parquet or geopackage files.

Read and display column names and data types from output datasets,
useful for understanding extracted data structure.

Author: Tiago de Conto
Package: gedih3
"""

import argparse
import os
import sys


def read_parquet_schema(path: str) -> list:
    """
    Read schema from a parquet file or directory.

    Parameters
    ----------
    path : str
        Path to parquet file or directory containing parquet files

    Returns
    -------
    list of tuples
        List of (column_name, data_type) tuples
    """
    import pyarrow.parquet as pq

    if os.path.isdir(path):
        # Try to read _metadata first
        meta_path = os.path.join(path, '_metadata')
        if os.path.exists(meta_path):
            schema = pq.read_schema(meta_path)
        else:
            # Find first parquet file
            import glob
            files = glob.glob(os.path.join(path, '**', '*.parquet'), recursive=True)
            if not files:
                raise FileNotFoundError(f"No parquet files found in {path}")
            schema = pq.read_schema(files[0])
    else:
        schema = pq.read_schema(path)

    return [(field.name, str(field.type)) for field in schema]


def read_geopackage_schema(path: str) -> list:
    """
    Read schema from a geopackage file.

    Parameters
    ----------
    path : str
        Path to geopackage file

    Returns
    -------
    list of tuples
        List of (column_name, data_type) tuples
    """
    import fiona

    with fiona.open(path) as src:
        schema = src.schema
        columns = [(name, dtype) for name, dtype in schema['properties'].items()]
        if 'geometry' in schema:
            columns.append(('geometry', schema['geometry']))
        return columns


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
        description="Inspect schema of parquet, geopackage, or HDF5 files"
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
        if path_lower.endswith('.gpkg') or path_lower.endswith('.geopackage'):
            columns = read_geopackage_schema(args.path)
            file_type = "GeoPackage"
        elif path_lower.endswith('.h5') or path_lower.endswith('.hdf5'):
            columns = read_hdf5_schema(args.path, args.group)
            file_type = "HDF5"
            is_hdf5 = True
        else:
            # Assume parquet (file or directory)
            columns = read_parquet_schema(args.path)
            file_type = "Parquet"

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
