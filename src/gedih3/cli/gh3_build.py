#! python
DEBUG=False

import argparse
import logging

import dask

def get_cmd_args():
    p = argparse.ArgumentParser(description = "Build H3-indexed GEDI database from SOC files")

    p.add_argument("-r", "--region", dest="region", required=False, type=str, default=None,
                   help="path to vector (.shp, .gpkg, .kml, etc.) or raster (.tif, .vrt) file with ROI, or bounding box as 'W,S,E,N', or ISO3 country code")
    p.add_argument("-d0", "--date-start", dest="date_start", required=False, type=str, default=None, help="start search date in YYYY-MM-DD format")
    p.add_argument("-d1", "--date-end", dest="date_end", required=False, type=str, default=None, help="end search date in YYYY-MM-DD format")

    p.add_argument("-h3r", "--h3-resolution", dest="h3_resolution", required=False, type=int, default=12, help="H3 level for data indexing [0-15] (default=12)")
    p.add_argument("-h3p", "--h3-partition", dest="h3_partition", required=False, type=int, default=3, help="H3 level for file partitioning [0-15] (default=3)")

    p.add_argument("-l1b", "--l1b", dest="l1b", nargs='+', type=str, default=None, required=False, help="GEDI L1B variables to include")
    p.add_argument("-l2a", "--l2a", dest="l2a", nargs='+', type=str, default=None, required=False, help="GEDI L2A variables to include")
    p.add_argument("-l2b", "--l2b", dest="l2b", nargs='+', type=str, default=None, required=False, help="GEDI L2B variables to include")
    p.add_argument("-l4a", "--l4a", dest="l4a", nargs='+', type=str, default=None, required=False, help="GEDI L4A variables to include")
    p.add_argument("-l4c", "--l4c", dest="l4c", nargs='+', type=str, default=None, required=False, help="GEDI L4C variables to include")

    p.add_argument("-o", "--output", dest="output", required=False, type=str, default=None, help="output directory for H3 database (bypass GH3 default path)")
    p.add_argument("-i", '--indir', dest="indir", required=False, type=str, default=None, help="path to local GEDI SOC database to build H3 from")
    p.add_argument("-t", '--tmpdir', dest="tmpdir", required=False, type=str, default=None, help="path to temporary directory for intermediate files")
    p.add_argument("-s3", "--s3", dest="s3", action='store_true', help="build directly from NASA DAACs S3 storage")

    p.add_argument("--gedi-version", dest="version", required=False, type=int, default=2, help="GEDI data version [default = 2]")

    p.add_argument("-s", "--dask-scheduler", dest="dask_scheduler", type=str, default=None, required=False, help="existing dask scheduler address, e.g. tcp://localhost:8786")
    p.add_argument("--dask-config", dest="dask_config", type=str, default=None, required=False, help="path to Dask YAML config file")

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

    p.add_argument("-v", "--verbose", dest="verbose", action="count", default=0,
                   help="increase output verbosity (-v for INFO, -vv for DEBUG)")
    p.add_argument("-Q", "--quiet", dest="quiet", required=False, action='store_true',
                   help="suppress all output except errors")

    cmdargs = p.parse_args()
    return cmdargs

def main():
    args = get_cmd_args()

    if DEBUG:
        args.region = '-51,0,-50,1'
        args.l2a = ['default']
        args.l2b = ['default']
        args.l4a = ['default']
        args.l4c = ['default']
        args.n_cpus = 40
        args.port = 8887
        import sys
        sys.path.insert(0, os.path.abspath('./src/'))

    import os
    import warnings
    from gedih3 import __version__ as _gh3_version
    from gedih3.config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_TMP_DIR
    from gedih3.cliutils import parse_gedi_args, parse_dask_args, parse_region
    from gedih3.gh3builder import build_h3db
    from gedih3.logger import H3BuildLogger
    from gedih3.logging_config import configure_logging, get_logger
    from dask.distributed import Client

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

    logger.info("")
    logger.info("=" * 70)
    logger.info(" GEDI H3 Database Builder Tool".center(70))
    logger.info(f" gedih3 v{_gh3_version}".center(70))
    logger.info("=" * 70)
    logger.info("")

    if args.output is None:
        args.output = GH3_DEFAULT_H3_DIR
    os.makedirs(args.output, exist_ok=True)

    if args.indir is None:
        args.indir = GH3_DEFAULT_SOC_DIR

    if args.tmpdir is None:
        args.tmpdir = os.path.join(GH3_DEFAULT_TMP_DIR, 'gh3_build')
    os.makedirs(args.tmpdir, exist_ok=True)

    product_vars = parse_gedi_args(args)
    spatial = parse_region(args.region) if args.region is not None else None

    h3_logger = H3BuildLogger(
        product_vars=product_vars,
        spatial=spatial,
        res=args.h3_resolution,
        part=args.h3_partition,
        version=args.version,
        dir=args.output,
    )

    if not h3_logger.product_vars and not h3_logger.updating:
        raise ValueError("No GEDI product selected - please select at least one of --l1b, --l2a, --l2b, --l4a, --l4c")
    if h3_logger.get_spatial() is None:
        logger.warning("No spatial filter provided - processing global data")

    if h3_logger.updating:
        logger.info("Build log exists, checking for updates")
        if h3_logger.new_spatial is not None:
            logger.info("Spatial filter updated")
        if h3_logger.new_product_vars is not None:
            logger.info("Product variables updated")

    logger.info(f"Building GEDI H3 database at {args.output}")
    h3_logger.save_log('PARTITIONING')

    dask_kwargs = parse_dask_args(args)

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
                res=h3_logger.res,
                part=h3_logger.part,
                soc_source=args.indir,
                h3_dir=h3_logger._PARENT_DIR,
                skip_granules=h3_logger.get_finished_granules(),
                version_kwargs={'version': h3_logger.gedi_version},
                tmp_dir=args.tmpdir
            )

            h3_logger.set_post_build_info()
            h3_logger.save_log('COMPLETED')

            n_files = len(h3_files) if h3_files else 0
            logger.info("")
            logger.info("=" * 70)
            logger.info(f" SUCCESS: {n_files} files exported to {args.output}")
            logger.info("=" * 70)
            logger.info("")

        except Exception as e:
            h3_logger.save_log('FAILED')
            logger.error(f"Build failed: {e}")
            raise e

if __name__ == "__main__":
    main()