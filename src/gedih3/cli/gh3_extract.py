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
import logging

def get_cmd_args():
    """Parse command line arguments for GEDI data extraction"""
    p = argparse.ArgumentParser(
        description="Extract and filter GEDI shots with H3 or EGI spatial indexing"
    )

    # Database configuration
    p.add_argument("-d", "--database", dest="database", required=False, type=str, default=None,
                   help="path to H3 database directory [default from config or environment]")

    # Output configuration
    p.add_argument("-o", "--output", dest="output", required=not DEBUG, type=str,
                   help="output directory or file path")
    p.add_argument("-f", "--format", dest="format", required=False, type=str, default='parquet',
                   help="output file format [default = parquet]")
    p.add_argument("-m", "--merge", dest="merge", required=False, action='store_true',
                   help="merge all partitions and export to single file")

    # Output indexing options
    p.add_argument("-egi", "--egi-level", dest="egi_level", type=int, required=False, default=None,
                   help="add EGI index and partition output by EGI level [1-12, GEDI baseline=6]")

    # Spatial filtering
    p.add_argument("-r", "--region", dest="region", required=False, type=str, default=None,
                   help="path to vector (.shp, .gpkg, .kml, etc.) or raster (.tif, .vrt) file with ROI, or bounding box as 'W,S,E,N', or ISO3 country code")

    # Variable selection by product
    p.add_argument("-l", "--list", dest="list", nargs='+', type=str, default=None,
                   help="flat list (space-separated) or file path of variables to export from the GEDI H3 database (need to specify product suffix, e.g. '_l2a')")

    p.add_argument("-l1b", "--l1b", dest="l1b", nargs='+', type=str, default=None,
                   help="GEDI L1B variables to export [space-separated list]")
    p.add_argument("-l2a", "--l2a", dest="l2a", nargs='+', type=str, default=None,
                   help="GEDI L2A variables to export [space-separated list]")
    p.add_argument("-l2b", "--l2b", dest="l2b", nargs='+', type=str, default=None,
                   help="GEDI L2B variables to export [space-separated list]")
    p.add_argument("-l4a", "--l4a", dest="l4a", nargs='+', type=str, default=None,
                   help="GEDI L4A variables to export [space-separated list]")
    p.add_argument("-l4c", "--l4c", dest="l4c", nargs='+', type=str, default=None,
                   help="GEDI L4C variables to export [space-separated list]")

    # Geometry options
    p.add_argument("-g", "--geo", dest="geo", required=False, action='store_true',
                   help="export as georeferenced points (requires lat/lon columns)")

    # Temporal filtering
    p.add_argument("-t", "--time", dest="add_datetime", required=False, action='store_true',
                   help="add human-readable 'datetime' column to output")
    p.add_argument("-t0", "--time-start", dest="time_start", type=str, default=None,
                   help="start date to filter shots [YYYY-MM-DD]")
    p.add_argument("-t1", "--time-end", dest="time_end", type=str, default=None,
                   help="end date to filter shots [YYYY-MM-DD]")

    # Data filtering
    p.add_argument("-q", "--query", dest="query", required=False, type=str, default=None,
                   help="pandas query string for filtering - e.g. 'quality_flag_l2a == 1 & agbd_l4a > 50'")
    p.add_argument("-y", "--quality", dest="quality", required=False, action='store_true',
                   help="apply quality filtering (quality_flag_l2a == 1)")

    # Computation settings
    p.add_argument("-s", "--dask-scheduler", dest="dask_scheduler", required=False, type=str, default=None,
                   help="dask scheduler address (overrides local cluster settings)")

    from gedih3.utils import get_system_resources
    cpus, ram, storage = get_system_resources()
    n = max(1, cpus // 4)
    m = int(max(1, ram / n))

    p.add_argument("-N", "--cores", dest="cores", required=False, type=int, default=n,
                   help=f"number of CPU cores to use [default = {n}]")
    p.add_argument("-T", "--threads", dest="threads", required=False, type=int, default=1,
                   help="number of threads per CPU core [default = 1]")
    p.add_argument("-M", "--memory", dest="memory", required=False, type=int, default=m,
                   help=f"memory limit per worker in GB [default = {m}]")
    p.add_argument("-P", "--port", dest="port", required=False, type=int, default=8787,
                   help="port for Dask dashboard [default = 8787]")

    # Verbosity options
    p.add_argument("-v", "--verbose", dest="verbose", action="count", default=0,
                   help="increase output verbosity (-v for INFO, -vv for DEBUG)")
    p.add_argument("-Q", "--quiet", dest="quiet", required=False, action='store_true',
                   help="suppress all output except errors")

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
        from dask.distributed import Client, progress

        from gedih3 import __version__ as _gh3_version
        import gedih3.gh3driver as gh3
        from gedih3.cliutils import collect_columns, build_query_string, parse_region, parse_dask_args
        from gedih3.config import GH3_DEFAULT_H3_DIR
        from gedih3.logging_config import configure_logging, get_logger

        # Configure logging based on verbosity flags
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

        # Determine output indexing mode
        use_egi = args.egi_level is not None

        logger.info("")
        logger.info("=" * 70)
        if use_egi:
            logger.info(" GEDI EGI Data Extraction Tool".center(70))
        else:
            logger.info(" GEDI H3 Data Extraction Tool".center(70))
        logger.info(f" gedih3 v{_gh3_version}".center(70))
        logger.info("=" * 70)
        logger.info("")

        # Configure database path
        if args.database:
            gh3.gh3_set_db_path(args.database)
        else:
            args.database = GH3_DEFAULT_H3_DIR

        logger.info(f"Database: {args.database}")

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
                part_col = f'h3_{part:02d}'

            # Export
            logger.info("Exporting data...")
            logger.info(f"  Partitioning by: {part_col}")

            write_task = ddf.to_parquet(args.output,
                                        write_metadata_file=True,
                                        write_index=True,
                                        overwrite=True,
                                        compression='zstd',
                                        partition_on=[part_col],
                                        compute=False
                                        )

            write_task = write_task.persist()
            progress(write_task)

            ofiles = glob.glob(f"{args.output}/**/*.parquet", recursive=True)

            if len(ofiles) == 0:
                raise RuntimeError("No output files were created.")

            logger.info("Writing dataset metadata")
            gh3.gh3_write_meta(opath=args.output, tool='gh3_extract', filter=query_str)

            logger.info("")
            logger.info("=" * 70)
            logger.info(f" SUCCESS: Data exported to {args.output}")
            logger.info("=" * 70)
            logger.info("")

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
