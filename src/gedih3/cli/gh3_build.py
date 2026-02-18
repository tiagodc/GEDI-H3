#! python

import argparse

def get_cmd_args():
    from gedih3.cliutils import add_dask_args, add_verbosity_args, add_product_args

    p = argparse.ArgumentParser(description="Build H3-indexed GEDI database from SOC files")

    # Spatial/temporal filtering
    p.add_argument("-r", "--region", dest="region", type=str, default=None,
                   help="vector file, bbox 'W,S,E,N', or ISO3 country code")
    p.add_argument("-d0", "--date-start", dest="date_start", type=str, default=None,
                   help="start date [YYYY-MM-DD]")
    p.add_argument("-d1", "--date-end", dest="date_end", type=str, default=None,
                   help="end date [YYYY-MM-DD]")

    # H3 configuration
    p.add_argument("-h3r", "--h3-resolution", dest="h3_resolution", type=int, default=12,
                   help="H3 index level [0-15, default=12]")
    p.add_argument("-h3p", "--h3-partition", dest="h3_partition", type=int, default=3,
                   help="H3 partition level [0-15, default=3]")

    # GEDI product variables
    add_product_args(p)

    # I/O paths
    p.add_argument("-o", "--output", dest="output", type=str, default=None,
                   help="output directory for H3 database")
    p.add_argument("-i", '--indir', dest="indir", type=str, default=None,
                   help="path to local GEDI SOC files")
    p.add_argument("-t", '--tmpdir', dest="tmpdir", type=str, default=None,
                   help="temporary directory for intermediate files")
    p.add_argument("-s3", "--s3", dest="s3", action='store_true',
                   help="build directly from NASA DAACs S3 storage")
    p.add_argument("--gedi-version", dest="version", type=int, default=None,
                   help="GEDI data version [default=latest available]")

    # Dask and verbosity
    add_dask_args(p, profile='build')
    add_verbosity_args(p)

    return p.parse_args()

def main():
    args = get_cmd_args()

    import os
    import sys
    import warnings
    from gedih3.config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_TMP_DIR
    from gedih3.cliutils import parse_gedi_args, parse_dask_args, parse_region, setup_logging, print_banner, print_success
    from gedih3.utils import get_system_resources
    from gedih3.gh3builder import build_h3db
    from gedih3.logger import H3BuildLogger
    from dask.distributed import Client

    # Setup logging and print banner
    logger = setup_logging(args, __name__)
    print_banner("GEDI H3 Database Builder Tool", logger=logger)

    if args.output is None:
        args.output = GH3_DEFAULT_H3_DIR
    os.makedirs(args.output, exist_ok=True)

    if args.tmpdir is None:
        args.tmpdir = os.path.join(args.output, '.tmp')
    os.makedirs(args.tmpdir, exist_ok=True)

    # Log detected resources and Dask configuration
    cpus, ram, storage = get_system_resources(disk_path=args.output)
    logger.info(f"System: {cpus} CPUs, {ram:.1f} GB RAM, {storage:.1f} GB free disk at {args.output}")
    logger.info(f"Dask config: {args.cores} workers, {args.threads} threads/worker, {args.memory} GB/worker")
    if storage < 10:
        logger.warning(f"Low disk space ({storage:.1f} GB free) — build may fail writing parquet output")

    # Determine source mode
    # -i/--indir → local mode; otherwise → S3 streaming (default)
    soc_source = args.indir  # None means S3 mode

    if args.s3 and args.indir:
        logger.warning("Both --indir and --s3 specified. Ignoring --s3, using local files.")
    elif args.s3 and not args.indir:
        logger.info("Note: --s3 is now the default when --indir is not specified.")

    product_vars = parse_gedi_args(args)
    spatial = parse_region(args.region) if args.region is not None else None
    temporal = None
    if args.date_start or args.date_end:
        temporal = (args.date_start, args.date_end)

    h3_logger = H3BuildLogger(
        product_vars=product_vars,
        spatial=spatial,
        temporal=temporal,
        res=args.h3_resolution,
        part=args.h3_partition,
        version=args.version,
        dir=args.output,
        source_mode='local' if soc_source else 's3',
    )

    if not h3_logger.product_vars and not h3_logger.updating:
        raise ValueError(
            "No GEDI product selected - please select at least one of "
            "--l1b, --l2a, --l2b, --l4a, --l4c, or use -l/--detail-level"
        )
    if h3_logger.get_spatial() is None:
        logger.warning("No spatial filter provided - processing global data")

    if h3_logger.updating:
        logger.info("Build log exists, checking for updates")
        if h3_logger.new_spatial is not None:
            logger.info("Spatial filter updated")
        if h3_logger.new_temporal is not None:
            logger.info("Temporal filter updated")
        if h3_logger.new_product_vars is not None:
            logger.info("Product variables updated")

    source_label = f"local: {soc_source}" if soc_source else "NASA S3 streaming"
    logger.info(f"Building GEDI H3 database at {args.output} (source: {source_label})")
    h3_logger.save_log('PARTITIONING')

    # Build version_kwargs for local file filtering
    version_kwargs = None
    if soc_source and h3_logger.gedi_version is not None:
        version_kwargs = {'version': h3_logger.gedi_version}

    dask_kwargs = parse_dask_args(args)

    try:
        with Client(**dask_kwargs) as client:
            warnings.filterwarnings("ignore", message=r"Sending large graph of size.*", category=UserWarning, module="distributed.client")
            def _suppress_pandas_perf_warnings():
                import warnings
                import pandas as pd
                warnings.filterwarnings("ignore", message=r"DataFrame is highly fragmented.*", category=pd.errors.PerformanceWarning)

            client.run(_suppress_pandas_perf_warnings)

            logger.info(f"Dask dashboard available at: {client.dashboard_link}")
            try:
                h3_files = build_h3db(
                    product_vars=h3_logger.get_product_vars(),
                    spatial=h3_logger.get_spatial(),
                    temporal=h3_logger.get_temporal(),
                    res=h3_logger.res,
                    part=h3_logger.part,
                    soc_source=soc_source,
                    version=h3_logger.gedi_version,
                    version_kwargs=version_kwargs,
                    h3_dir=h3_logger._PARENT_DIR,
                    skip_granules=h3_logger.get_finished_granules(),
                    status_callback=h3_logger.save_log,
                    tmp_dir=args.tmpdir
                )

                h3_logger.set_post_build_info()
                h3_logger.save_log('COMPLETED')

                n_files = len(h3_files) if h3_files else 0
                print_success(f"{n_files} files exported to {args.output}", logger=logger)

            except Exception as e:
                h3_logger.save_log('FAILED')
                logger.error(f"Build failed: {e}")
                raise e

    except KeyboardInterrupt:
        logger.warning("\nBuild interrupted by user")
        h3_logger.set_post_build_info()
        h3_logger.save_log('INTERRUPTED')
        sys.exit(130)

    except Exception as e:
        from gedih3.exceptions import (
            H3ValidationError,
            GediFileError,
            GediDatabaseError,
            GediError
        )

        if isinstance(e, H3ValidationError):
            logger.error(f"H3 parameter error: {e}")
            sys.exit(2)
        elif isinstance(e, GediFileError):
            logger.error(f"File error: {e}")
            sys.exit(3)
        elif isinstance(e, GediDatabaseError):
            logger.error(f"Database error: {e}")
            sys.exit(4)
        elif isinstance(e, GediError):
            logger.error(f"GEDI error: {e}")
            sys.exit(1)
        else:
            logger.error(f"Unexpected error: {type(e).__name__}: {e}")
            if args.verbose >= 2:
                import traceback
                traceback.print_exc()
            sys.exit(1)

if __name__ == "__main__":
    main()