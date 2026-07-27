#! python

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""
GEDI H3/EGI Rasterization Tool

Convert pre-aggregated GEDI datasets to GeoTIFF raster format.

This tool reads datasets produced by gh3_aggregate or gh3_extract and
converts them to GeoTIFF rasters. For EGI datasets, the output is natively
aligned to the EASE-Grid 2.0 projection. For H3 datasets, interpolation
is used to approximate hexagonal data on a regular grid.

IMPORTANT: This tool does NOT perform aggregation. To aggregate raw GEDI
shots to coarser resolutions before rasterization, use gh3_aggregate first
(optionally with the --rasterize flag to do both in one step).
"""

import os
import sys
import argparse


def get_cmd_args():
    """Parse command line arguments for GEDI rasterization"""
    from gedih3.cliutils import add_dask_args, add_verbosity_args, add_storage_args

    p = argparse.ArgumentParser(
        description="Convert aggregated GEDI datasets to GeoTIFF raster format",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Input/output configuration
    p.add_argument("-d", "--dataset", dest="dataset", required=True, type=str,
                   help="path to aggregated dataset (from gh3_aggregate or gh3_extract)")
    p.add_argument("-o", "--output", dest="output", required=True, type=str,
                   help="output directory or file path")
    p.add_argument("-m", "--merge", dest="merge", action='store_true',
                   help="merge all partitions into single file")
    p.add_argument("--compress", dest="compress", type=str, default='LZW',
                   choices=['LZW', 'ZSTD', 'DEFLATE', 'PACKBITS', 'NONE'],
                   help="GeoTIFF compression [default=LZW]")
    p.add_argument("--no-cog", dest="cog", action='store_false',
                   help="write plain GeoTIFFs instead of Cloud Optimized GeoTIFFs")

    # Variable selection
    p.add_argument("-l", "--list", dest="list", nargs='+', type=str, default=None,
                   help="variables to rasterize (space-separated or wildcards like 'agbd_*')")

    # Filtering
    p.add_argument("-q", "--query", dest="query", type=str, default=None,
                   help="pandas query string for filtering before rasterization")

    # Dask, storage, and verbosity
    add_dask_args(p)
    add_storage_args(p)
    add_verbosity_args(p)

    return p.parse_args()


def _rasterize_dataset(dataset_path, output_path, args, logger):
    """Rasterize a single dataset directory to raster output.

    Thin delegate to ``gh3.gh3_rasterize()`` — index-type dispatch, column
    resolution and partition-level derivation all live in the library so the
    CLI and the Python API cannot diverge.

    Must be called inside a Dask Client context.
    """
    import gedih3 as gh3
    from gedih3.cliutils import get_dataset_index_info

    # Informational only — gh3_rasterize() does its own resolution and
    # validation. json_read_cached makes this sidecar read free.
    info = get_dataset_index_info(dataset_path)
    index_type, index_level = info.get('index_type'), info.get('index_level')
    if index_type == 'egi' and index_level is not None:
        from gedih3 import egi
        logger.info(f"Dataset type: EGI level {index_level} "
                    f"(~{egi.get_resolution(index_level):.0f}m)")
    elif index_type == 'h3':
        logger.info(f"Dataset type: H3 level {index_level}")

    logger.info(f"Input: {dataset_path}")
    if args.list:
        logger.info(f"Variables to rasterize: {args.list}")
    if args.query:
        logger.info(f"Query filter: {args.query}")

    logger.info("Rasterizing...")
    result = gh3.gh3_rasterize(
        dataset_path, output_path,
        columns=args.list if args.list else None,
        merge=args.merge, query=args.query,
        compress=args.compress, cog=args.cog, show_progress=not args.quiet
    )

    if args.merge:
        logger.info(f"Merged raster exported to {result}")
    else:
        logger.info(f"Exported {len(result)} raster files to {output_path}")


def main():
    args = get_cmd_args()

    # Import cli_exception_handler early for wrapping the main logic
    from gedih3.cliutils import cli_exception_handler

    with cli_exception_handler(args):
        from dask.distributed import Client

        from gedih3.cliutils import (parse_dask_args, setup_logging,
                                     print_banner, print_success, setup_storage,
                                     resolve_path_args)
        from gedih3.config import DATASET_META_FILENAME

        # Setup logging and print banner
        logger = setup_logging(args, __name__)
        setup_storage(args, logger=logger)
        print_banner("GEDI Rasterization Tool", logger=logger)

        resolve_path_args(args, ['dataset', 'output'], logger=logger)

        # Validate input dataset exists
        from gedih3.utils import (smart_exists, smart_isdir, smart_glob,
                                  smart_database_exists)
        if not smart_database_exists(args.dataset):
            logger.error(f"Dataset not found: {args.dataset}")
            sys.exit(1)

        dask_kwargs = parse_dask_args(args)

        with Client(**dask_kwargs) as client:
            logger.info(f"Dask dashboard: {client.dashboard_link}")

            # Detect dataset type
            from gedih3.utils import smart_join
            dataset_meta_path = smart_join(args.dataset, DATASET_META_FILENAME)

            if smart_exists(dataset_meta_path):
                # Single dataset (existing behavior)
                _rasterize_dataset(args.dataset, args.output, args, logger)
                print_success("Rasterization complete", logger=logger)

            else:
                # Check for time-series (subdirectories with metadata)
                window_dirs = sorted([
                    d for d in smart_glob(smart_join(args.dataset, '*'))
                    if smart_isdir(d) and
                       smart_exists(smart_join(d, DATASET_META_FILENAME))
                ])

                if not window_dirs:
                    logger.error(f"Dataset metadata not found: {dataset_meta_path}")
                    logger.error("This tool requires a dataset produced by "
                                 "gh3_aggregate or gh3_extract.")
                    logger.error("For raw GEDI data, use gh3_aggregate with "
                                 "--rasterize flag instead.")
                    sys.exit(1)

                # Time-series mode
                logger.info(f"Time-series dataset: {len(window_dirs)} windows")
                for window_dir in window_dirs:
                    window_name = os.path.basename(window_dir)
                    if args.merge:
                        window_output = os.path.join(args.output, f"{window_name}.tif")
                    else:
                        window_output = os.path.join(args.output, window_name)
                    logger.info(f"── Window: {window_name} ──")
                    _rasterize_dataset(window_dir, window_output, args, logger)

                print_success(f"Time-series rasterization complete: "
                              f"{len(window_dirs)} windows", logger=logger)


if __name__ == '__main__':
    main()
