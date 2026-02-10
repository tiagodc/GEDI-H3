#! python
DEBUG=False

"""
GEDI H3/EGI Data Extraction Tool

Extract and filter GEDI shots from H3-indexed parquet database with spatial,
temporal, and quality filters. Supports H3 or EGI output indexing/partitioning.

Author: Tiago de Conto
Package: gedih3
"""

import os
import sys
import argparse


def get_cmd_args():
    """Parse command line arguments for GEDI data extraction"""
    from gedih3.cliutils import add_dask_args, add_verbosity_args, add_product_args, parse_egi_levels

    p = argparse.ArgumentParser(
        description="Extract and filter GEDI shots with H3 or EGI spatial indexing"
    )

    # Database/output configuration
    p.add_argument("-d", "--database", dest="database", type=str, default=None,
                   help="path to H3 database directory")
    p.add_argument("-o", "--output", dest="output", required=not DEBUG, type=str,
                   help="output directory or file path")
    p.add_argument("-f", "--format", dest="format", type=str, default='parquet',
                   help="output format [default=parquet]")
    p.add_argument("-m", "--merge", dest="merge", action='store_true',
                   help="merge all partitions into single file")

    # Indexing options
    p.add_argument("-egi", "--egi", dest="egi", type=parse_egi_levels, default=None,
                   nargs='?', const=(1, 12),
                   help="EGI indexing: bare flag defaults to 1:12, or 'index[:partition]' e.g., '1' or '6:12'")
    p.add_argument("--egi-shuffle", dest="egi_shuffle", action='store_true',
                   help="Use shuffle-based EGI extraction (gh3_load + egi_extract) instead of direct loading")

    # Spatial/temporal filtering
    p.add_argument("-r", "--region", dest="region", type=str, default=None,
                   help="vector file, bbox 'W,S,E,N', or ISO3 code")
    p.add_argument("-t0", "--time-start", dest="time_start", type=str, default=None,
                   help="start date [YYYY-MM-DD]")
    p.add_argument("-t1", "--time-end", dest="time_end", type=str, default=None,
                   help="end date [YYYY-MM-DD]")

    # Variable selection
    p.add_argument("-l", "--list", dest="list", nargs='+', type=str, default=None,
                   help="variables to export (space-separated or file path)")
    add_product_args(p)

    # Options
    p.add_argument("-g", "--geo", dest="geo", action='store_true',
                   help="export as georeferenced points")
    p.add_argument("-t", "--time", dest="add_datetime", action='store_true',
                   help="add datetime column to output")
    p.add_argument("-q", "--query", dest="query", type=str, default=None,
                   help="pandas query string for filtering")
    p.add_argument("-y", "--quality", dest="quality", action='store_true',
                   help="apply quality filtering")

    # Dask and verbosity
    add_dask_args(p)
    add_verbosity_args(p)

    return p.parse_args()

def main():
    if DEBUG:
        sys.path.insert(0, os.path.abspath('./src/'))

    args = get_cmd_args()

    if DEBUG:
        args.output = '/gpfs/data1/vclgp/decontot/repos/gedih3/tmp/tmp/maryland'
        args.region = '/gpfs/data1/vclgp/decontot/data/vector/other_boundaries/md.shp'
        args.l2a = ['rh_098']
        args.l2b = ['pai_z_000']
        args.l4a = ['agbd']
        args.l4c = ['wsci']
        args.add_datetime = True
        args.quality = True
        args.database = '/gpfs/data1/vclgp/data/iss_gedi/h3_mock/database'
        args.cores = 20
        args.port = 9994
        args.egi = (1, 12)  # (index_level, partition_level)

    # Import cli_exception_handler early for wrapping the main logic
    from gedih3.cliutils import cli_exception_handler

    with cli_exception_handler(args):
        import glob
        import numpy as np
        import pandas as pd
        import geopandas as gpd
        from dask.distributed import Client, progress

        import gedih3.gh3driver as gh3
        from gedih3.cliutils import (collect_columns, build_query_string, parse_region,
                                     parse_dask_args, setup_logging, print_banner,
                                     print_success, configure_database_path, h3_col_name)

        # Parse EGI levels if specified
        use_egi = args.egi is not None
        if use_egi:
            egi_index_level, egi_partition_level = args.egi
        else:
            egi_index_level, egi_partition_level = None, None

        # Setup logging and print banner
        logger = setup_logging(args, __name__)
        title = "GEDI EGI Data Extraction Tool" if use_egi else "GEDI H3 Data Extraction Tool"
        print_banner(title, logger=logger)

        # Configure database path
        configure_database_path(args, logger=logger)

        # Verify database exists
        if not os.path.exists(args.database):
            logger.error(f"Database directory not found: {args.database}")
            logger.error("Please specify a valid database path with -d/--database")
            sys.exit(1)

        # Read metadata
        if not os.path.exists(os.path.join(args.database, "gedih3_build_log.json")):
            raise FileNotFoundError("Could not read database metadata. Invalid database?")

        # Parse region
        region = None
        if args.region:
            logger.info(f"Parsing region: {args.region}")
            region = parse_region(args.region)

        # Collect columns
        logger.info("Collecting variables...")
        columns = collect_columns(args)

        # EGI indexing works best with Point geometry from GeoDataFrame
        # Ensure geometry column is loaded so we have coordinate information
        if use_egi and 'geometry' not in columns:
            columns.append('geometry')

        if len(columns) > 0:
            logger.info(f"  Total variables: {len(columns)}")
        else:
            raise ValueError("No variables selected for extraction. Please specify variables with -l/--list or product-specific options.")

        # Build query
        query_str = build_query_string(args)
        if query_str:
            logger.info(f"Query filter: {query_str}")

        dask_kwargs = parse_dask_args(args)

        with Client(**dask_kwargs) as client:
            logger.info(f"Dask dashboard available at: {client.dashboard_link}")

            # Determine partition column and process data
            if use_egi:
                from gedih3.egi.config import egi_col_name, get_resolution

                index_res = get_resolution(egi_index_level)
                partition_res = get_resolution(egi_partition_level)
                logger.info(f"  Index level: {egi_index_level} (~{index_res:.0f}m)")
                logger.info(f"  Partition level: {egi_partition_level} (~{partition_res:.0f}m)")

                egi_part_col = egi_col_name(egi_partition_level)

                if args.egi_shuffle:
                    # Shuffle-based approach: gh3_load + egi_extract
                    # More reliable but slower for large datasets
                    logger.info("Loading H3 data then converting to EGI (shuffle-based)...")
                    ddf_h3 = gh3.gh3_load(
                        columns=columns,
                        region=region,
                        query=query_str,
                        gh3_dir=args.database
                    )
                    logger.info(f"  Loaded {ddf_h3.npartitions} H3 partitions")
                    logger.info("  Converting to EGI (shuffling data)...")
                    ddf = gh3.egi_extract(
                        ddf_h3,
                        index_level=egi_index_level,
                        partition_level=egi_partition_level,
                        add_geometry=True
                    )
                else:
                    # Direct loading approach: egi_load
                    # Faster but may have issues with complex geometries
                    logger.info("Loading data directly into EGI partitions (no shuffle)...")
                    ddf = gh3.egi_load(
                        columns=columns,
                        region=region,
                        query=query_str,
                        gh3_dir=args.database,
                        index_level=egi_index_level,
                        partition_level=egi_partition_level
                    )

                logger.info(f"  Loaded {ddf.npartitions} EGI partitions")

                part_col = egi_part_col
            else:
                # H3 mode - load normally
                logger.info("Loading data from H3 database...")
                ddf = gh3.gh3_load(
                    columns=columns,
                    region=region,
                    query=query_str,
                    gh3_dir=args.database
                )
                logger.info(f"  Loaded {ddf.npartitions} partitions")
                part = gh3.gh3_read_meta('h3_partition_level', gh3_root_dir=args.database)
                part_col = h3_col_name(part)

            # Export - use simplified flat file structure (not hive-partitioned)
            logger.info("Exporting data...")
            logger.info(f"  Output format: simplified flat files by {part_col}")

            os.makedirs(args.output, exist_ok=True)

            if args.merge:
                # Merge all partitions into single file
                logger.info("  Merging all partitions...")
                result_df = ddf.compute()
                opath = gh3.gh3_export_part(
                    result_df,
                    odir=args.output,
                    fmt=args.format,
                    is_file_path=True,
                    part_col=part_col
                )
                ofiles = [opath] if opath else []
            else:
                # Export each partition as separate file named by partition ID
                # For EGI: after set_index shuffle, each unique EGI partition value is in
                # exactly one Dask partition (no collision), but a Dask partition may contain
                # multiple EGI partition values (needs splitting at export time).
                # For H3: each Dask partition corresponds to one H3 partition directory.
                write_task = ddf.map_partitions(
                    gh3.gh3_export_part,
                    odir=args.output,
                    fmt=args.format,
                    part_col=part_col,
                    group_by_partition=use_egi,  # Split by EGI partition within each Dask partition
                    meta=pd.Series(dtype=str)
                )

                write_task = write_task.persist()
                progress(write_task)

                ofiles = glob.glob(f"{args.output}/*.{args.format}")

            if len(ofiles) == 0:
                raise RuntimeError("No output files were created.")

            # Write simplified dataset metadata
            logger.info("Writing dataset metadata")
            index_type = 'egi' if use_egi else 'h3'
            index_level = egi_index_level if use_egi else gh3.gh3_read_meta('h3_resolution_level', gh3_root_dir=args.database)
            meta_kwargs = {}
            if use_egi:
                meta_kwargs['egi_index_level'] = egi_index_level
                meta_kwargs['egi_partition_level'] = egi_partition_level
            gh3.gh3_write_dataset_meta(
                opath=args.output,
                index_type=index_type,
                index_level=index_level,
                columns=columns,
                source_database=args.database,
                query_filter=query_str,
                tool='gh3_extract',
                file_format=args.format,
                **meta_kwargs
            )

            print_success(f"Data exported to {args.output}", logger=logger)


if __name__ == '__main__':
    main()
