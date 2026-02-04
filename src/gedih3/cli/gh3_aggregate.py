#! python
DEBUG=False

"""
GEDI H3/EGI Data Aggregation Tool

Aggregate GEDI shots from H3-indexed parquet database to coarser spatial
resolutions. Supports H3 hexagonal aggregation or EGI (EASE Grid Index)
square pixel aggregation for GEDI L4B compatibility.

Author: Tiago de Conto
Package: gedih3
"""

import os
import sys
import argparse

TIME_UNITS = ['years', 'months', 'weeks', 'days']

def get_cmd_args():
    """Parse command line arguments for GEDI data aggregation"""
    from gedih3.cliutils import add_dask_args, add_verbosity_args, add_product_args, parse_egi_levels

    p = argparse.ArgumentParser(
        description="Aggregate GEDI shots to H3 hexagons or EGI square pixels",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Database/output configuration
    p.add_argument("-d", "--database", dest="database", type=str, default=None,
                   help="path to H3 database or simplified dataset directory")
    p.add_argument("-o", "--output", dest="output", required=not DEBUG, type=str,
                   help="output directory or file path")
    p.add_argument("-f", "--format", dest="format", type=str, default='parquet',
                   help="output format [default=parquet]")
    p.add_argument("-m", "--merge", dest="merge", action='store_true',
                   help="merge all partitions into single file")
    p.add_argument("-H", "--hive", dest="hive", action='store_true',
                   help="export in hive-partitioned directory structure")

    # Rasterization option
    p.add_argument("-R", "--rasterize", dest="rasterize", action='store_true',
                   help="also export data as GeoTIFF rasters after aggregation")
    p.add_argument("--compress", dest="compress", type=str, default='LZW',
                   choices=['LZW', 'ZSTD', 'DEFLATE', 'PACKBITS', 'NONE'],
                   help="GeoTIFF compression [default=LZW]")

    # Aggregation options
    p.add_argument("-h3", "--h3-level", dest="h3_level", type=int, default=None,
                   help="aggregate to H3 level [0-15]")
    p.add_argument("-egi", "--egi", dest="egi", type=parse_egi_levels, default=None,
                   help="EGI aggregation as 'level[:partition]' e.g., '6' or '6:12'")
    p.add_argument("-a", "--aggregate", dest="aggregate", type=str, default="mean",
                   help="aggregation function: mean, sum, count, etc. [default=mean]")

    # Spatial/temporal filtering
    p.add_argument("-r", "--region", dest="region", type=str, default=None,
                   help="vector file, bbox 'W,S,E,N', or ISO3 code")
    p.add_argument("-t0", "--time-start", dest="time_start", type=str, default=None,
                   help="start date [YYYY-MM-DD]")
    p.add_argument("-t1", "--time-end", dest="time_end", type=str, default=None,
                   help="end date [YYYY-MM-DD]")
    p.add_argument("-ti", "--time_interval", dest="time_interval", type=int, default=0,
                   help="generate time-series outputs at interval")
    p.add_argument("-tu", "--time_units", dest="time_units", type=str, default='years',
                   choices=TIME_UNITS, help="time interval units [default=years]")

    # Variable selection
    p.add_argument("-l", "--list", dest="list", nargs='+', type=str, default=None,
                   help="variables to aggregate (space-separated or file path)")
    add_product_args(p)

    # Filtering
    p.add_argument("-q", "--query", dest="query", type=str, default=None,
                   help="pandas query string for filtering")
    p.add_argument("-y", "--quality", dest="quality", action='store_true',
                   help="apply quality filtering")

    # Dask and verbosity
    add_dask_args(p)
    add_verbosity_args(p)

    return p.parse_args()

def main():
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

    # Validate aggregation level arguments
    if args.h3_level is None and args.egi is None:
        print("ERROR: Must specify either -h3/--h3-level or -egi for aggregation target")
        sys.exit(1)
    if args.h3_level is not None and args.egi is not None:
        print("ERROR: Cannot specify both -h3/--h3-level and -egi. Choose one.")
        sys.exit(1)

    use_egi = args.egi is not None
    if use_egi:
        egi_agg_level, egi_partition_level = args.egi
    else:
        egi_agg_level, egi_partition_level = None, None

    try:
        import glob
        import pandas as pd
        from dask.distributed import Client, progress

        import gedih3.gh3driver as gh3
        from gedih3.utils import is_hive_directory
        from gedih3.cliutils import (collect_columns, build_query_string, parse_region,
                                     parse_dask_args, parse_file_format, setup_logging,
                                     print_banner, print_success, configure_database_path,
                                     load_data_from_source, get_numeric_columns, h3_col_name)

        # Setup logging and print banner
        logger = setup_logging(args, __name__)
        title = "GEDI EGI Data Aggregation Tool" if use_egi else "GEDI H3 Data Aggregation Tool"
        print_banner(title, logger=logger)

        # Configure database path
        configure_database_path(args, logger=logger)

        # Verify database exists
        if not os.path.exists(args.database):
            logger.error(f"Database directory not found: {args.database}")
            sys.exit(1)

        # Parse format
        args.format = parse_file_format(args)

        # Parse region
        region = None
        if args.region:
            logger.info(f"Parsing region: {args.region}")
            region = parse_region(args.region)

        # Collect columns
        logger.info("Collecting variables...")
        columns = collect_columns(args)

        # EGI needs geometry for coordinate access
        if use_egi and 'geometry' not in columns:
            columns.append('geometry')

        if len(columns) == 0:
            raise ValueError("No variables selected. Use -l/--list or product options.")
        logger.info(f"  Total variables: {len(columns)}")

        # Build query
        query_str = build_query_string(args)
        if query_str:
            logger.info(f"Query filter: {query_str}")

        dask_kwargs = parse_dask_args(args)

        with Client(**dask_kwargs) as client:
            logger.info(f"Dask dashboard: {client.dashboard_link}")

            # Load data
            logger.info("Loading data...")
            ddf = load_data_from_source(args.database, columns, region, query_str, logger)
            logger.info(f"  Loaded {ddf.npartitions} partitions")

            logger.info("Aggregating data...")
            from_hive = is_hive_directory(args.database, match_str=r'h3_.+=.+')
            numeric_columns = get_numeric_columns(ddf)

            if use_egi:
                # EGI (EASE Grid) aggregation
                from gedih3 import egi
                target_res = egi.get_resolution(egi_agg_level)
                partition_res = egi.get_resolution(egi_partition_level)
                logger.info(f"  Target: EGI level {egi_agg_level} (~{target_res:.0f}m pixels)")
                if egi_partition_level != egi_agg_level:
                    logger.info(f"  Partition: EGI level {egi_partition_level} (~{partition_res:.0f}m)")

                aggdf = gh3.egi_aggregate(
                    ddf,
                    target_level=egi_agg_level,
                    agg=args.aggregate,
                    columns=numeric_columns,
                    add_geometry=True,
                    partition_level=egi_partition_level,
                    repartition=not args.merge
                )
                # Use partition level for file organization
                part_col = egi.egi_col_name(egi_partition_level if not args.merge else egi_agg_level)
                export_func = gh3.egi_export_part
            else:
                # H3 (hexagon) aggregation
                logger.info(f"  Target: H3 level {args.h3_level}")

                aggdf = gh3.gh3_aggregate(
                    ddf,
                    target_res=args.h3_level,
                    agg=args.aggregate,
                    columns=numeric_columns,
                    add_geometry=True,
                    repartition=not args.merge
                )
                part = gh3.gh3_read_meta('h3_partition_level', gh3_root_dir=args.database)
                part_col = h3_col_name(part)
                export_func = gh3.gh3_export_part

            # Export
            logger.info("Exporting data...")

            os.makedirs(args.output, exist_ok=True)

            if args.merge:
                # Merge all partitions into single file
                logger.info("  Merging all partitions...")
                aggdf = aggdf.compute()
                opath = export_func(aggdf, odir=args.output, fmt=args.format, is_file_path=True)
                print_success(f"Merged file exported to {opath}", logger=logger)

            elif args.hive:
                # Hive-style partitioning (for backwards compatibility)
                logger.info("  Using hive-style partitioning...")
                write_task = aggdf.to_parquet(args.output,
                                              write_metadata_file=True,
                                              write_index=True,
                                              overwrite=True,
                                              compression='zstd',
                                              partition_on=[part_col],
                                              compute=False)
                write_task = write_task.persist()
                progress(write_task)

                ofiles = glob.glob(f"{args.output}/**/*.parquet", recursive=True)
                if len(ofiles) == 0:
                    raise RuntimeError("No output files were created.")
                print_success(f"{len(ofiles)} files exported to {args.output}", logger=logger)

            else:
                # Simplified flat file structure (default)
                logger.info("  Output format: simplified flat files")
                write_task = aggdf.map_partitions(export_func,
                                                  odir=args.output,
                                                  fmt=args.format,
                                                  meta=pd.Series(dtype=str))
                write_task = write_task.persist()
                progress(write_task)

                ofiles = glob.glob(f"{args.output}/*.{args.format}")
                if len(ofiles) == 0:
                    raise RuntimeError("No output files were created.")

                # Write simplified dataset metadata
                logger.info("Writing dataset metadata...")
                index_type = 'egi' if use_egi else 'h3'
                index_level = egi_agg_level if use_egi else args.h3_level
                meta_kwargs = {}
                if use_egi:
                    meta_kwargs['egi_aggregation_level'] = egi_agg_level
                    meta_kwargs['egi_partition_level'] = egi_partition_level
                gh3.gh3_write_dataset_meta(
                    opath=args.output,
                    index_type=index_type,
                    index_level=index_level,
                    columns=list(aggdf.columns),
                    source_database=args.database,
                    aggregation=args.aggregate,
                    tool='gh3_aggregate',
                    **meta_kwargs
                )
                print_success(f"{len(ofiles)} files exported to {args.output}", logger=logger)

            # Optional: Rasterize the aggregated data
            if args.rasterize:
                logger.info("Rasterizing aggregated data to GeoTIFF...")

                raster_dir = os.path.join(args.output, 'rasters')
                os.makedirs(raster_dir, exist_ok=True)

                from gedih3 import raster

                if use_egi:
                    from gedih3 import egi
                    rasterize_func = egi.rasterize_partition
                else:
                    rasterize_func = raster.rasterize_h3_partition

                # Re-load the computed aggregated data for rasterization
                # (at this point aggdf might be computed or lazy depending on path)
                if hasattr(aggdf, 'compute'):
                    # Lazy - rasterize partitions directly
                    raster.rasterize_and_export_partitions(
                        aggdf, raster_dir, rasterize_func,
                        columns=None,  # Auto-detect from aggregated data
                        compress=args.compress,
                        show_progress=True
                    )
                else:
                    # Already computed (merged case) - rasterize single GeoDataFrame
                    xras = rasterize_func(aggdf, columns=None)
                    if isinstance(xras, pd.Series) and len(xras) > 0:
                        # Handle multiple tiles in one GeoDataFrame
                        for i, tile_xras in enumerate(xras):
                            if hasattr(tile_xras, 'data_vars') and len(tile_xras.data_vars) > 0:
                                raster.export_raster(tile_xras, os.path.join(raster_dir, f'tile_{i}.tif'), compress=args.compress)
                    elif hasattr(xras, 'data_vars') and len(xras.data_vars) > 0:
                        raster.export_raster(xras, os.path.join(raster_dir, 'merged.tif'), compress=args.compress)

                raster_files = glob.glob(f"{raster_dir}/*.tif")
                print_success(f"{len(raster_files)} raster files exported to {raster_dir}", logger=logger)

    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(130)

    except Exception as e:
        print(f"\n\nERROR: {type(e).__name__}: {e}")
        if args.verbose >= 2:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()