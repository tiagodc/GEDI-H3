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
    from gedih3.cliutils import add_dask_args, add_verbosity_args, add_product_args

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
    p.add_argument("-egi", "--egi-level", dest="egi_level", type=int, default=None,
                   help="add EGI index at level [1-12]")

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

    try:
        import glob
        import pandas as pd
        from dask.distributed import Client, progress

        import gedih3.gh3driver as gh3
        from gedih3.cliutils import (collect_columns, build_query_string, parse_region,
                                     parse_dask_args, setup_logging, print_banner,
                                     print_success, configure_database_path, h3_col_name)

        # Setup logging and print banner
        use_egi = args.egi_level is not None
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

            # Load data
            logger.info("Loading data from H3 database...")
            ddf = gh3.gh3_load(
                columns=columns,
                region=region,
                query=query_str,
                gh3_dir=args.database
            )

            logger.info(f"  Loaded {ddf.npartitions} partitions")

            # Determine partition column
            if use_egi:
                from gedih3 import egi
                from gedih3.egi.config import egi_col_name

                logger.info(f"Adding EGI index at level {args.egi_level}...")
                target_res = egi.get_resolution(args.egi_level)
                logger.info(f"  EGI resolution: ~{target_res:.0f}m pixels")

                # Add EGI index to each partition
                ddf = ddf.map_partitions(
                    egi.egi_dataframe,
                    level=args.egi_level,
                    set_index=False,
                    meta=ddf._meta
                )
                part_col = egi_col_name(args.egi_level)
            else:
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
                write_task = ddf.map_partitions(
                    gh3.gh3_export_part,
                    odir=args.output,
                    fmt=args.format,
                    part_col=part_col,
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
            index_level = args.egi_level if use_egi else gh3.gh3_read_meta('h3_resolution_level', gh3_root_dir=args.database)
            gh3.gh3_write_dataset_meta(
                opath=args.output,
                index_type=index_type,
                index_level=index_level,
                columns=columns,
                source_database=args.database,
                query_filter=query_str,
                tool='gh3_extract'
            )

            print_success(f"Data exported to {args.output}", logger=logger)

    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(130)

    except Exception as e:
        # Use print here since logger may not be configured yet
        print(f"\n\nERROR: {type(e).__name__}: {e}")
        if args.verbose >= 2:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
