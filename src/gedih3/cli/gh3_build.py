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
                   help="directory with GEDI SOC files (builds from existing .h5 files; downloads only if directory is empty)")
    p.add_argument("-t", '--tmpdir', dest="tmpdir", type=str, default=None,
                   help="temporary directory for intermediate files")
    p.add_argument("-s3", "--s3", dest="s3", action='store_true',
                   help="download from NASA S3 to temp directory (no persistent local download)")
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
    import glob
    import warnings
    from gedih3.config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_TMP_DIR
    from gedih3.cliutils import parse_gedi_args, parse_dask_args, parse_region, setup_logging, print_banner, print_success
    from gedih3.utils import get_system_resources
    from gedih3.gh3builder import build_h3db, download_soc, soc_file_tree
    from gedih3.gedidriver import GEDIFile, validate_soc_files
    from gedih3.logger import H3BuildLogger, SOCDownloadLogger
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

    # Determine source mode: -i (download+build) or --s3 (temp download+build)
    if args.indir:
        soc_source = args.indir
        if args.s3:
            logger.warning("Both -i and --s3 specified. Using -i (local download mode).")
    elif args.s3:
        soc_source = None  # S3 download to temp dir
    else:
        logger.error("Either -i/--indir (download directory) or --s3 (S3 download) is required")
        sys.exit(2)

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
        source_mode='s3' if args.s3 else 'download',
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

    source_label = f"download+build: {soc_source}" if soc_source else "NASA S3 (temp download)"
    logger.info(f"Building GEDI H3 database at {args.output} (source: {source_label})")
    h3_logger.save_log('PARTITIONING')

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

            # Auto-download when using -i mode
            if soc_source is not None:
                os.makedirs(soc_source, exist_ok=True)
                soc_logger = SOCDownloadLogger(
                    product_vars=h3_logger.get_product_vars(),
                    spatial=h3_logger.get_spatial(),
                    temporal=h3_logger.get_temporal(),
                    version=h3_logger.gedi_version,
                    dir=soc_source,
                )
                existing_h5 = glob.glob(os.path.join(soc_source, '**', 'GEDI*.h5'), recursive=True)

                if soc_logger.updating and soc_logger.log_data.get('status') == 'COMPLETED':
                    # Completed download log exists — use existing files
                    needs_download = False
                    logger.info(f"Using existing downloads at {soc_source} ({len(soc_logger.granule_info)} granules)")
                elif existing_h5:
                    # No completed log, but .h5 files exist — skip download
                    needs_download = False
                    logger.warning(
                        f"Found {len(existing_h5)} existing HDF5 files in {soc_source}. "
                        f"Skipping download and building directly from existing files. "
                        f"Run gh3_download first if you need to update the source data."
                    )

                    # Validate that requested products/variables exist in the HDF5 files
                    try:
                        validation = validate_soc_files(h3_logger.get_product_vars(), soc_source)
                    except Exception as val_err:
                        logger.warning(f"Could not validate HDF5 files (corrupt file?): {val_err}")
                        validation = None

                    if validation is not None:
                        # Handle tuple return (no SOC files found) vs dict return
                        if isinstance(validation, tuple):
                            can_skip = False
                            validation = validation[1] if len(validation) > 1 else {}
                        else:
                            can_skip = validation.get("can_skip", True)
                    else:
                        can_skip = True  # Skip validation if it failed

                    if not can_skip:
                        msg_parts = ["Requested variables not found in existing HDF5 files:\n"]
                        if validation.get("missing_products"):
                            msg_parts.append(f"  Missing products: {', '.join(validation['missing_products'])}")
                        if validation.get("missing_variables"):
                            for prod, mvars in validation["missing_variables"].items():
                                msg_parts.append(f"  Missing variables in {prod}: {', '.join(mvars)}")
                        if validation.get("error"):
                            msg_parts.append(f"  {validation['error']}")
                        msg_parts.append("")
                        msg_parts.append("To fix:")
                        msg_parts.append("  1. Check available variables:  gh3_read_schema /path/to/file.h5")
                        msg_parts.append("  2. Adjust your -l2a/-l4a/... flags to match available data")
                        msg_parts.append("  3. Run gh3_download to fetch the required products into the SOC directory")
                        msg_parts.append("  4. Or use --s3 to build directly from NASA S3 (no persistent download)")
                        logger.error("\n".join(msg_parts))
                        sys.exit(2)
                else:
                    # No log AND no .h5 files — download
                    needs_download = True

                if needs_download:
                    logger.info(f"Downloading GEDI data to {soc_source}")
                    soc_logger.save_log('DOWNLOADING')

                    def _download_tracker(gran_info, status):
                        """Called from main thread (as_completed loop). Thread safe."""
                        if status == 'PENDING':
                            soc_logger.register_pending_granules([gran_info])
                        else:
                            soc_logger.update_granule_status(gran_info, status)
                        # Save after every granule — downloads are slow (seconds each),
                        # so the JSON write overhead is negligible relative to network I/O
                        soc_logger.save_log('DOWNLOADING')

                    download_soc(
                        product_vars=soc_logger.get_product_vars(),
                        spatial=soc_logger.get_spatial(),
                        temporal=soc_logger.get_temporal(),
                        direct_access=False,
                        update=True,
                        version=h3_logger.gedi_version,
                        odir=soc_source,
                        on_granule_complete=_download_tracker,
                    )
                    soc_logger.set_post_download_info()
                    soc_logger.save_log('COMPLETED')
                    logger.info("Download complete")

            try:
                # Register granules being submitted for build as PENDING
                # Only for local download mode (-i); S3 mode has no local SOC directory
                if soc_source is not None and isinstance(soc_source, str) and os.path.isdir(soc_source):
                    _soc_for_build = soc_file_tree(soc_source, to_list=True)
                    _build_granules = []
                    for _soc in _soc_for_build:
                        _first = list(_soc.values())[0]
                        _gf = GEDIFile(_first)
                        _build_granules.append({'orbit': _gf.orbit, 'granule': _gf.orbit_granule, 'track': _gf.track})
                    h3_logger.register_pending_granules(_build_granules)
                    h3_logger.save_log('PROCESSING')

                h3_files = build_h3db(
                    product_vars=h3_logger.get_product_vars(),
                    spatial=h3_logger.get_spatial(),
                    temporal=h3_logger.get_temporal(),
                    res=h3_logger.res,
                    part=h3_logger.part,
                    soc_source=soc_source,
                    version=h3_logger.gedi_version,
                    h3_dir=h3_logger._PARENT_DIR,
                    skip_granules=h3_logger.get_finished_granules(),
                    status_callback=h3_logger.save_log,
                    tmp_dir=args.tmpdir
                )

                h3_logger.set_post_build_info()
                h3_logger.save_log('COMPLETED')

                n_files = len(h3_files) if h3_files else 0
                print_success(f"{n_files} files exported to {args.output}", logger=logger)

                if soc_source is not None:
                    logger.info(f"Note: Downloaded HDF5 files in {soc_source} are no longer needed and can be deleted to free disk space")

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