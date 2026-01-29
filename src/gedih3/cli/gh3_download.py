#! python
DEBUG=False

import argparse
import logging

def get_cmd_args():
    p = argparse.ArgumentParser(description = "Download GEDI data from NASA's DAAC")

    p.add_argument("-r", "--region", dest="region", required=False, type=str, default=None,
                   help="path to vector (.shp, .gpkg, .kml, etc.) or raster (.tif, .vrt) file with ROI, or bounding box as 'W,S,E,N', or ISO3 country code")
    p.add_argument("-d0", "--date-start", dest="date_start", required=False, type=str, default=None, help="start search date in YYYY-MM-DD format")
    p.add_argument("-d1", "--date-end", dest="date_end", required=False, type=str, default=None, help="end search date in YYYY-MM-DD format")

    p.add_argument("-l1b", "--l1b", dest="l1b", nargs='+', type=str, default=None, required=False, help="GEDI L1B variables to download")
    p.add_argument("-l2a", "--l2a", dest="l2a", nargs='+', type=str, default=None, required=False, help="GEDI L2A variables to download")
    p.add_argument("-l2b", "--l2b", dest="l2b", nargs='+', type=str, default=None, required=False, help="GEDI L2B variables to download")
    p.add_argument("-l4a", "--l4a", dest="l4a", nargs='+', type=str, default=None, required=False, help="GEDI L4A variables to download")
    p.add_argument("-l4c", "--l4c", dest="l4c", nargs='+', type=str, default=None, required=False, help="GEDI L4C variables to download")

    p.add_argument("-o", "--outdir", dest="outdir", required=False, type=str, default=None, help="output directory for downloaded files (bypass GH3 default path)")
    p.add_argument("--resume", dest="resume", action='store_true', help="validate downloaded files and redownload missing or corrupted files")

    p.add_argument("-s", "--dask-scheduler", dest="dask_scheduler", type=str, default=None, required=False, help="existing dask scheduler address, e.g. tcp://localhost:8786")

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
    import os
    args = get_cmd_args()

    if DEBUG:
        args.region = '-51,0,-50,1'
        args.l2a = ['default']
        args.l2b = ['default']
        args.l4a = ['default']
        args.l4c = ['default']
        args.n_cpus = 32
        args.threads = 1
        args.port = 9998
        import sys
        sys.path.insert(0, os.path.abspath('./src/'))

    from gedih3 import __version__ as _gh3_version
    from gedih3.config import GH3_DEFAULT_SOC_DIR
    from gedih3.cliutils import parse_gedi_args, parse_dask_args, parse_region
    from gedih3.gh3builder import download_soc
    from gedih3.logger import SOCDownloadLogger
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
    logger.info(" GEDI Data Download Tool".center(70))
    logger.info(f" gedih3 v{_gh3_version}".center(70))
    logger.info("=" * 70)
    logger.info("")

    if args.outdir is None:
        args.outdir = GH3_DEFAULT_SOC_DIR
    os.makedirs(args.outdir, exist_ok=True)

    product_vars = parse_gedi_args(args)
    spatial = parse_region(args.region) if args.region is not None else None
    temporal = None
    if args.date_start or args.date_end:
        temporal = (args.date_start, args.date_end)

    soc_logger = SOCDownloadLogger(
        product_vars=product_vars,
        spatial=spatial,
        temporal=temporal,
        dir=args.outdir
    )

    if not soc_logger.product_vars and not soc_logger.updating:
        raise ValueError("No GEDI product selected for download - please select at least one of --l1b, --l2a, --l2b, --l4a, --l4c")
    if soc_logger.get_spatial() is None:
        logger.warning("No spatial filter provided - downloading global data")
    if soc_logger.get_temporal() is None:
        logger.warning("No temporal filter provided - downloading data from all available dates")

    if soc_logger.updating:
        logger.info("Download log exists, resuming downloads")
        if soc_logger.new_spatial is not None:
            logger.info("Spatial filter updated")
        if soc_logger.new_temporal is not None:
            logger.info("Temporal filter updated")
        if soc_logger.new_product_vars is not None:
            logger.info("Product variables updated")

    logger.info(f"Downloading GEDI data to {args.outdir}")
    soc_logger.save_log('DOWNLOADING')

    dask_kwargs = parse_dask_args(args)

    try:
        with Client(**dask_kwargs) as client:
            logger.info(f"Dask dashboard available at: {client.dashboard_link}")
            try:
                soc_files = download_soc(
                    product_vars=soc_logger.get_product_vars(),
                    spatial=soc_logger.get_spatial(),
                    temporal=soc_logger.get_temporal(),
                    direct_access=False,
                    update=True,
                    odir=args.outdir
                )

                soc_logger.save_log('COMPLETED')

                n_files = len(soc_files) if soc_files else 0
                logger.info("")
                logger.info("=" * 70)
                logger.info(f" SUCCESS: {n_files} files downloaded to {args.outdir}")
                logger.info("=" * 70)
                logger.info("")

            except Exception as e:
                soc_logger.save_log('FAILED')
                logger.error(f"Download failed: {e}")
                raise e

    except KeyboardInterrupt:
        logger.warning("\nDownload interrupted by user")
        soc_logger.save_log('INTERRUPTED')
        import sys
        sys.exit(130)

    except Exception as e:
        from gedih3.exceptions import (
            GediDownloadError,
            GediAuthenticationError,
            GediNetworkError,
            GediError
        )

        if isinstance(e, GediAuthenticationError):
            logger.error(f"Authentication error: {e}")
            logger.info("Please check your NASA Earthdata credentials at ~/.netrc")
            import sys
            sys.exit(2)
        elif isinstance(e, GediDownloadError):
            logger.error(f"Download error: {e}")
            import sys
            sys.exit(3)
        elif isinstance(e, GediNetworkError):
            logger.error(f"Network error: {e}")
            logger.info("Check your internet connection and try again")
            import sys
            sys.exit(4)
        elif isinstance(e, GediError):
            logger.error(f"GEDI error: {e}")
            import sys
            sys.exit(1)
        else:
            logger.error(f"Unexpected error: {type(e).__name__}: {e}")
            if args.verbose >= 2:
                import traceback
                traceback.print_exc()
            import sys
            sys.exit(1)

if __name__ == "__main__":
    main()
