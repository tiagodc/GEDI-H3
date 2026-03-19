#! python

import argparse

def get_cmd_args():
    from gedih3.cliutils import add_dask_args, add_verbosity_args, add_product_args

    p = argparse.ArgumentParser(description="Download GEDI data from NASA's DAAC")

    # Spatial/temporal filtering
    p.add_argument("-r", "--region", dest="region", type=str, default=None,
                   help="vector file, bbox 'W,S,E,N', or ISO3 code")
    p.add_argument("-t0", "--time-start", dest="time_start", type=str, default=None,
                   help="start date [YYYY-MM-DD]")
    p.add_argument("-t1", "--time-end", dest="time_end", type=str, default=None,
                   help="end date [YYYY-MM-DD]")

    # GEDI product variables
    add_product_args(p)

    # Output options
    p.add_argument("-o", "--output", dest="output", type=str, default=None,
                   help="output directory for downloaded files")
    p.add_argument("-s3", "--s3", dest="s3", action='store_true',
                   help="use S3 ETL mode: stream and subset from NASA S3 (10-50x less data transfer)")
    p.add_argument("--gedi-version", dest="version", type=int, default=None,
                   help="GEDI data version [default=latest available]")

    # Dask and verbosity
    add_dask_args(p, profile='build')
    add_verbosity_args(p)

    return p.parse_args()

def main():
    import os
    import sys
    args = get_cmd_args()

    from gedih3.config import GH3_DEFAULT_SOC_DIR
    from gedih3.cliutils import parse_gedi_args, parse_dask_args, parse_region, setup_logging, print_banner, print_success
    from gedih3.gh3builder import download_soc, s3_etl_subset
    from gedih3.logger import SOCDownloadLogger
    from dask.distributed import Client

    # Setup logging and print banner
    logger = setup_logging(args, __name__)
    print_banner("GEDI Data Download Tool", logger=logger)

    if args.output is None:
        args.output = GH3_DEFAULT_SOC_DIR
    os.makedirs(args.output, exist_ok=True)

    product_vars = parse_gedi_args(args)
    spatial = parse_region(args.region) if args.region is not None else None
    temporal = None
    if args.time_start or args.time_end:
        temporal = (args.time_start, args.time_end)

    soc_logger = SOCDownloadLogger(
        product_vars=product_vars,
        spatial=spatial,
        temporal=temporal,
        version=args.version,
        dir=args.output
    )

    if args.s3:
        soc_logger.s3_access = True

    if not soc_logger.product_vars and not soc_logger.updating:
        raise ValueError(
            "No GEDI product selected for download - please select at least one of "
            "--l2a, --l2b, --l4a, --l4c (or use --detail-level for all four), "
            "and/or --l1b for waveform data"
        )
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

    source_label = "S3 ETL" if args.s3 else "DAAC download"
    logger.info(f"Downloading GEDI data to {args.output} ({source_label})")
    soc_logger.save_log('DOWNLOADING')

    dask_kwargs = parse_dask_args(args)

    try:
        with Client(**dask_kwargs) as client:
            logger.info(f"Dask dashboard available at: {client.dashboard_link}")
            try:
                if args.s3:
                    # S3 ETL mode: stream and subset from NASA S3 to permanent output dir
                    s3_etl_subset(
                        product_vars=soc_logger.product_vars,
                        spatial=soc_logger.get_spatial(),
                        temporal=soc_logger.get_temporal(),
                        version=args.version,
                        odir=args.output,
                        ensure_l2a=True,
                    )
                else:
                    # Standard DAAC download mode
                    download_soc(
                        product_vars=soc_logger.product_vars,
                        spatial=soc_logger.get_spatial(),
                        temporal=soc_logger.get_temporal(),
                        direct_access=False,
                        update=True,
                        version=args.version,
                        odir=args.output
                    )

                soc_logger.set_post_download_info()
                soc_logger.save_log('COMPLETED')

                import glob
                n_files = len(glob.glob(os.path.join(args.output, '**', 'GEDI*.h5'), recursive=True))
                print_success(f"{n_files} files downloaded to {args.output} ({source_label})", logger=logger)

            except Exception as e:
                soc_logger.save_log('FAILED')
                logger.error(f"Download failed: {e}")
                raise e

    except KeyboardInterrupt:
        logger.warning("\nDownload interrupted by user")
        soc_logger.save_log('INTERRUPTED')
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
            sys.exit(2)
        elif isinstance(e, GediDownloadError):
            logger.error(f"Download error: {e}")
            sys.exit(3)
        elif isinstance(e, GediNetworkError):
            logger.error(f"Network error: {e}")
            logger.info("Check your internet connection and try again")
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
