#! python
"""
GEDI H3/EGI Rasterization Tool

Rasterize spatially-indexed GEDI data to GeoTIFF format with support for:
- H3 hexagon data (with bilinear interpolation)
- EGI square pixel data (native alignment)
- Time-series raster generation
- Compression and tiling options

Author: Tiago de Conto
Package: gedih3
"""

import os
import sys
import argparse
import logging

DEBUG = False
TIME_UNITS = ['years', 'months', 'weeks', 'days']


def get_cmd_args():
    """Parse command line arguments for GEDI rasterization"""
    p = argparse.ArgumentParser(
        description="Rasterize spatially-indexed GEDI data to GeoTIFF format",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Input configuration
    p.add_argument("-d", "--database", dest="database", required=not DEBUG, type=str,
                   help="path to H3/EGI database directory or parquet file")

    # Output configuration
    p.add_argument("-o", "--output", dest="output", required=not DEBUG, type=str,
                   help="output directory (for tiles) or file path (for merged)")
    p.add_argument("-m", "--merge", dest="merge", required=False, action='store_true',
                   help="merge all partitions into single output file")
    p.add_argument("--compress", dest="compress", type=str, default='LZW',
                   choices=['LZW', 'ZSTD', 'DEFLATE', 'PACKBITS', 'NONE'],
                   help="GeoTIFF compression method [default: LZW]")

    # Spatial options
    p.add_argument("-r", "--region", dest="region", required=False, type=str, default=None,
                   help="spatial filter: vector file, bbox 'W,S,E,N', or ISO3 code")
    p.add_argument("-h3", "--h3-level", dest="h3_level", type=int, default=None,
                   help="aggregate to H3 level before rasterization [0-15]")
    p.add_argument("-egi", "--egi-level", dest="egi_level", type=int, default=None,
                   help="aggregate to EGI level before rasterization [1-12]")

    # Aggregation options
    p.add_argument("-a", "--aggregate", dest="aggregate", type=str, default="mean",
                   help="aggregation function: 'mean', 'sum', 'count', etc. [default: mean]")

    # Variable selection
    p.add_argument("-l", "--list", dest="list", nargs='+', type=str, default=None,
                   help="variables to include in raster (space-separated)")

    # Temporal options
    p.add_argument("-t0", "--time-start", dest="time_start", type=str, default=None,
                   help="start date for filtering [YYYY-MM-DD]")
    p.add_argument("-t1", "--time-end", dest="time_end", type=str, default=None,
                   help="end date for filtering [YYYY-MM-DD]")
    p.add_argument("-ti", "--time-interval", dest="time_interval", type=int, default=0,
                   help="generate time-series outputs at this interval")
    p.add_argument("-tu", "--time-units", dest="time_units", type=str, default='years',
                   choices=TIME_UNITS, help="time interval units [default: years]")

    # Data filtering
    p.add_argument("-q", "--query", dest="query", required=False, type=str, default=None,
                   help="pandas query string for filtering")
    p.add_argument("-y", "--quality", dest="quality", required=False, action='store_true',
                   help="apply quality filtering (quality_flag_l2a == 1)")

    # Computation settings
    p.add_argument("-s", "--dask-scheduler", dest="dask_scheduler", required=False, type=str, default=None,
                   help="dask scheduler address")

    from gedih3.utils import get_system_resources
    cpus, ram, storage = get_system_resources()
    n = max(1, cpus // 4)
    m = int(max(1, ram / n))

    p.add_argument("-N", "--cores", dest="cores", required=False, type=int, default=n,
                   help=f"number of CPU cores [default: {n}]")
    p.add_argument("-T", "--threads", dest="threads", required=False, type=int, default=1,
                   help="threads per CPU core [default: 1]")
    p.add_argument("-M", "--memory", dest="memory", required=False, type=int, default=m,
                   help=f"memory per worker in GB [default: {m}]")
    p.add_argument("-P", "--port", dest="port", required=False, type=int, default=8787,
                   help="Dask dashboard port [default: 8787]")

    # Verbosity
    p.add_argument("-v", "--verbose", dest="verbose", action="count", default=0,
                   help="increase verbosity (-v, -vv)")
    p.add_argument("-Q", "--quiet", dest="quiet", required=False, action='store_true',
                   help="suppress output except errors")

    return p.parse_args()


def main():
    args = get_cmd_args()

    if DEBUG:
        args.database = '/gpfs/data1/vclgp/data/iss_gedi/h3_mock/database'
        args.output = '/gpfs/data1/vclgp/decontot/repos/gedih3/tmp/raster_test'
        args.list = ['agbd_l4a']
        args.egi_level = 6
        args.cores = 4
        args.port = 9995

    try:
        import glob
        import pandas as pd
        from dask.distributed import Client, progress

        from gedih3 import __version__ as _gh3_version
        import gedih3.gh3driver as gh3
        from gedih3 import raster
        from gedih3.cliutils import collect_columns, build_query_string, parse_region, parse_dask_args
        from gedih3.config import GH3_DEFAULT_H3_DIR
        from gedih3.logging_config import configure_logging, get_logger

        # Configure logging
        if args.quiet:
            log_level = logging.ERROR
        elif args.verbose >= 2:
            log_level = logging.DEBUG
        elif args.verbose == 1:
            log_level = logging.INFO
        else:
            log_level = logging.INFO

        configure_logging(level=log_level, verbose=args.verbose >= 1)
        logger = get_logger(__name__)

        logger.info("")
        logger.info("=" * 70)
        logger.info(" GEDI Rasterization Tool".center(70))
        logger.info(f" gedih3 v{_gh3_version}".center(70))
        logger.info("=" * 70)
        logger.info("")

        # Determine indexing type
        use_egi = args.egi_level is not None
        if args.h3_level is not None and args.egi_level is not None:
            logger.error("Cannot specify both -h3 and -egi. Choose one.")
            sys.exit(1)

        target_level = args.egi_level if use_egi else (args.h3_level or 6)

        if use_egi:
            from gedih3 import egi
            logger.info(f"Mode: EGI (EASE Grid) rasterization")
            logger.info(f"Target level: {target_level} (~{egi.get_resolution(target_level):.0f}m)")
        else:
            logger.info(f"Mode: H3 hexagon rasterization")
            logger.info(f"Target level: {target_level}")

        # Configure database
        if not args.database:
            args.database = GH3_DEFAULT_H3_DIR

        logger.info(f"Database: {args.database}")

        if not os.path.exists(args.database):
            logger.error(f"Database not found: {args.database}")
            sys.exit(1)

        # Parse region
        region = None
        if args.region:
            logger.info(f"Parsing region: {args.region}")
            region = parse_region(args.region)

        # Collect columns
        logger.info("Collecting variables...")
        columns = args.list if args.list else None

        # EGI aggregation works best with Point geometry from GeoDataFrame
        # Ensure geometry column is loaded so we have coordinate information
        if use_egi:
            if columns is None:
                columns = ['geometry']
            elif 'geometry' not in columns:
                columns.append('geometry')

        # Build query
        query_parts = []
        if args.query:
            query_parts.append(args.query)
        if args.quality:
            query_parts.append("quality_flag_l2a == 1")

        query_str = ' and '.join(query_parts) if query_parts else None
        if query_str:
            logger.info(f"Query filter: {query_str}")

        # Time-series mode
        use_timeseries = args.time_interval > 0

        if use_timeseries:
            if not args.time_start or not args.time_end:
                logger.error("Time-series mode requires both -t0 and -t1")
                sys.exit(1)
            logger.info(f"Time-series mode: {args.time_interval} {args.time_units}")
            logger.info(f"  From: {args.time_start}")
            logger.info(f"  To: {args.time_end}")

        dask_kwargs = parse_dask_args(args)

        with Client(**dask_kwargs) as client:
            logger.info(f"Dask dashboard: {client.dashboard_link}")

            # Load data
            logger.info("Loading data...")
            ddf = gh3.gh3_load(
                columns=columns,
                region=region,
                query=query_str,
                gh3_dir=args.database
            )

            logger.info(f"  Loaded {ddf.npartitions} partitions")

            if use_timeseries:
                # Time-series rasterization
                logger.info("Generating time-series rasters...")

                # Ensure datetime column exists
                if 'datetime' not in ddf.columns:
                    time_cols = [c for c in ddf.columns if 'time' in c.lower()]
                    if time_cols:
                        ddf = ddf.map_partitions(
                            raster.convert_delta_time_to_datetime,
                            delta_time_col=time_cols[0]
                        )

                for t0, t1, suffix in raster.generate_time_windows(
                    args.time_start, args.time_end, args.time_interval, args.time_units
                ):
                    logger.info(f"Processing: {suffix}")

                    # Filter by time
                    time_query = f"datetime >= '{t0}' and datetime < '{t1}'"
                    time_ddf = ddf.query(time_query)

                    # Check if data exists
                    n_rows = time_ddf.map_partitions(len).compute().sum()
                    if n_rows == 0:
                        logger.info(f"  No data for {suffix}, skipping")
                        continue

                    # Aggregate - use numeric columns only
                    numeric_columns = [col for col in time_ddf.columns if time_ddf[col].dtype.kind in 'biufc']

                    if use_egi:
                        aggdf = gh3.egi_aggregate(
                            time_ddf,
                            target_level=target_level,
                            agg=args.aggregate,
                            columns=numeric_columns,
                            add_geometry=True
                        )
                        rasterize_func = lambda gdf, columns=None: egi.geodf_to_raster(gdf, columns=columns)
                    else:
                        aggdf = gh3.gh3_aggregate(
                            time_ddf,
                            target_res=target_level,
                            agg=args.aggregate,
                            columns=numeric_columns,
                            add_geometry=True
                        )
                        rasterize_func = raster.rasterize_h3_partition

                    # Export
                    if args.merge:
                        output_path = os.path.join(args.output, f"{suffix}.tif")
                    else:
                        output_path = os.path.join(args.output, suffix)

                    os.makedirs(os.path.dirname(output_path) if args.merge else output_path, exist_ok=True)

                    if args.merge:
                        raster.merge_and_export_rasters(
                            aggdf, output_path, rasterize_func,
                            columns=columns, compress=args.compress
                        )
                    else:
                        raster.rasterize_and_export_partitions(
                            aggdf, output_path, rasterize_func,
                            columns=columns, compress=args.compress
                        )

                    logger.info(f"  Exported to {output_path}")

            else:
                # Single rasterization
                logger.info("Aggregating data...")

                # Get numeric columns only for aggregation
                numeric_columns = [col for col in ddf.columns if ddf[col].dtype.kind in 'biufc']

                if use_egi:
                    from gedih3 import egi
                    aggdf = gh3.egi_aggregate(
                        ddf,
                        target_level=target_level,
                        agg=args.aggregate,
                        columns=numeric_columns,
                        add_geometry=True
                    )
                    rasterize_func = lambda gdf, columns=None: egi.geodf_to_raster(gdf, columns=columns)
                else:
                    aggdf = gh3.gh3_aggregate(
                        ddf,
                        target_res=target_level,
                        agg=args.aggregate,
                        columns=numeric_columns,
                        add_geometry=True
                    )
                    rasterize_func = raster.rasterize_h3_partition

                logger.info("Rasterizing...")

                os.makedirs(args.output if not args.merge else os.path.dirname(args.output), exist_ok=True)

                if args.merge:
                    output_path = args.output if args.output.endswith('.tif') else f"{args.output}.tif"
                    raster.merge_and_export_rasters(
                        aggdf, output_path, rasterize_func,
                        columns=columns, compress=args.compress, show_progress=True
                    )
                    logger.info(f"Exported to {output_path}")
                else:
                    paths = raster.rasterize_and_export_partitions(
                        aggdf, args.output, rasterize_func,
                        columns=columns, compress=args.compress, show_progress=True
                    )
                    logger.info(f"Exported {len([p for p in paths if p])} files to {args.output}")

            logger.info("")
            logger.info("=" * 70)
            logger.info(" SUCCESS: Rasterization complete".center(70))
            logger.info("=" * 70)
            logger.info("")

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
